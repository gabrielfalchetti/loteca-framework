# scripts/plan_bet_portfolio_adv.py
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd
from risk_utils import load_prob_matrix, simulate_outcomes, ticket_hits, portfolio_payouts, var_es, kelly_fraction

RNG = np.random.default_rng(7)

def _entropy_row(p):
    p = np.clip(np.array(p, dtype=float), 1e-12, 1.0)
    p = p / p.sum()
    return -np.sum(p * np.log(p))

def _greedy_ticket(P: np.ndarray, max_duplos=4, max_triplos=2) -> list[set[int]]:
    """
    Cria um ticket com base em entropia: jogos mais incertos recebem duplos/triplos.
    """
    n = P.shape[0]
    ents = np.array([_entropy_row(P[i]) for i in range(n)])
    order = np.argsort(ents)[::-1]  # decrescente incerteza
    ticket = []
    used_d, used_t = 0, 0
    for i in range(n):
        # default: seco = argmax
        choice = np.argmax(P[i])
        ticket.append(set([int(choice)]))
    for idx in order:
        if used_t < max_triplos:
            ticket[idx] = set([0,1,2])
            used_t += 1
        elif used_d < max_duplos:
            # pegue as duas maiores probabilidades
            top2 = np.argsort(P[idx])[::-1][:2]
            ticket[idx] = set([int(top2[0]), int(top2[1])])
            used_d += 1
    return ticket

def _candidate_pool(P: np.ndarray, n_cand=20, max_duplos=4, max_triplos=2):
    """
    Gera um pool de candidatos variando o sorteio de jogos 'limítrofes' para diversificar.
    """
    n = P.shape[0]
    pool = []
    base = _greedy_ticket(P, max_duplos=max_duplos, max_triplos=max_triplos)
    pool.append(base)
    # perturbações leves: troca um duplo por seco e adiciona duplo em outro
    for _ in range(n_cand - 1):
        t = [set(s) for s in base]
        # mexe em 2 jogos aleatórios
        js = RNG.choice(n, size=2, replace=False)
        for j in js:
            if len(t[j]) == 1:
                # promove a duplo (duas maiores)
                top2 = np.argsort(P[j])[::-1][:2]
                t[j] = set([int(top2[0]), int(top2[1])])
            elif len(t[j]) == 2:
                # às vezes volta a seco (argmax), para liberar orçamento
                if RNG.random() < 0.5:
                    t[j] = set([int(np.argmax(P[j]))])
        pool.append(t)
    return pool

def _p14_ticket(P: np.ndarray, ticket: list[set[int]]) -> float:
    """Probabilidade de 14 acertos exata sob independência."""
    p = 1.0
    for i in range(P.shape[0]):
        p_game = sum(P[i, s] for s in ticket[i])
        p *= p_game
    return float(p)

def main():
    ap = argparse.ArgumentParser(description="Planejador de portfólio com gestão de risco (Kelly fracionário, VaR/ES).")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--n-tickets", type=int, default=5)
    ap.add_argument("--max-duplos", type=int, default=4)
    ap.add_argument("--max-triplos", type=int, default=2)
    ap.add_argument("--sims", type=int, default=50000)
    ap.add_argument("--kelly-frac", type=float, default=0.25, help="fração do Kelly (0 a 1)")
    ap.add_argument("--min-divers", type=float, default=0.20, help="mínimo de peso por 2º melhor ticket (diversificação)")
    ap.add_argument("--paytable-json", default="", help="JSON opcional: {'14': x, '13': y, ...}")
    args = ap.parse_args()

    df, P = load_prob_matrix(args.rodada)  # (14x3)
    # pool de candidatos
    pool = _candidate_pool(P, n_cand=max(20, args.n_tickets*4), max_duplos=args.max_duplos, max_triplos=args.max_triplos)

    # calcula p14 exato aproximado por independência para ranking inicial
    scores = np.array([_p14_ticket(P, t) for t in pool])
    idxs = np.argsort(scores)[::-1]
    chosen = [pool[i] for i in idxs[:args.n_tickets]]

    # simula outcomes
    sim = simulate_outcomes(P, n_sims=args.sims)

    # define paytable (opcional)
    pay_table = None
    if args.paytable_json.strip():
        try:
            raw = json.loads(args.paytable_json)
            pay_table = {int(k): float(v) for k, v in raw.items()}
        except Exception:
            pay_table = None

    # calcula base 'edge' por ticket: p14 (ou utilidade média simulada se paytable)
    bases = []
    for t in chosen:
        if pay_table is None:
            # proxy de edge: prob. de 14 acertos
            bases.append(_p14_ticket(P, t))
        else:
            # utilidade esperada sob paytable (simulada)
            ret = portfolio_payouts(sim, [t], np.array([1.0]), pay_table=pay_table)
            bases.append(float(np.mean(ret)))
    bases = np.array(bases, dtype=float)

    # Kelly fracionário sobre odds implícitas do próprio ranking (b simples): b = (1/p) - 1
    # Evita infinito quando p~0
    stakes = np.zeros(len(chosen), dtype=float)
    for i, pwin in enumerate(bases):
        pwin = max(1e-9, min(1.0-1e-9, pwin))
        b = (1.0 / pwin) - 1.0
        f = kelly_fraction(pwin, b) * max(0.0, min(1.0, args.kelly_frac))
        stakes[i] = f

    # se tudo zero (muito conservador), coloca pesos proporcionais a bases
    if stakes.sum() <= 1e-12:
        stakes = bases.copy()

    # normaliza e garante diversificação mínima no top-2 quando possível
    stakes = stakes / stakes.sum() if stakes.sum() > 0 else np.ones_like(stakes)/len(stakes)
    if len(stakes) >= 2 and args.min_divers > 0:
        # top-1 não pode ultrapassar 1 - min_divers
        top = np.argmax(stakes)
        if stakes[top] > 1.0 - args.min_divers:
            spill = stakes[top] - (1.0 - args.min_divers)
            stakes[top] -= spill
            # distribui 'spill' para os demais proporcionalmente
            rest_idx = [i for i in range(len(stakes)) if i != top]
            if rest_idx:
                stakes[rest_idx] += spill * (stakes[rest_idx] / stakes[rest_idx].sum())

    # risco do portfólio
    ret = portfolio_payouts(sim, chosen, stakes, pay_table=pay_table)
    var95, es95 = var_es(ret, alpha=0.95)

    # salva plano
    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)
    # tickets
    rows=[]
    for k, t in enumerate(chosen, 1):
        row = {"ticket_id": k, "stake_weight": float(stakes[k-1])}
        for j, s in enumerate(t, 1):
            # s é set de {0,1,2} -> converte para "1", "1X", "12", "X2", "123"
            mapping = {0:"1", 1:"X", 2:"2"}
            cell = "".join(sorted(mapping[x] for x in sorted(list(s))))
            row[f"J{j}"] = cell
        rows.append(row)
    df_tk = pd.DataFrame(rows)
    df_tk.to_csv(base/"portfolio_plan.csv", index=False)

    # métricas
    pd.DataFrame({
        "metric":["p14_top","p14_mean","var95","es95"],
        "value":[float(bases.max()), float(bases.mean()), var95, es95]
    }).to_csv(base/"portfolio_metrics.csv", index=False)

    # distribuição de acertos (utilidade) — se pay_table for None, é fracionária; senão, payout
    out_kind = "utility" if pay_table is None else "payout"
    pd.DataFrame({out_kind: ret}).to_csv(base/"portfolio_returns.csv", index=False)

    print(f"[portfolio] OK -> {base/'portfolio_plan.csv'}")
    print(f"[portfolio] Metrics -> {base/'portfolio_metrics.csv'} | VaR95={var95:.4f} ES95={es95:.4f}")
    print(f"[portfolio] Returns -> {base/'portfolio_returns.csv'}")

if __name__ == "__main__":
    main()
