# scripts/plan_bet_portfolio.py
from __future__ import annotations
import argparse, os
from pathlib import Path
import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)

def _pick_joined(base: Path) -> Path:
    for name in ["joined_referee.csv","joined_weather.csv","joined_enriched.csv","joined.csv"]:
        p = base/name
        if p.exists() and p.stat().st_size>0:
            return p
    raise RuntimeError("[portfolio] nenhum joined* encontrado.")

def _simulate_hits(P, picks, n_sims=20000):
    """
    P: (n_matches, 3) probabilidades [1,X,2]
    picks: lista de sets com outcomes permitidos por jogo. Ex.: {"1"}, {"1","X"}, {"1","X","2"}
    Retorna prob de 14/14 (todos acertam) sob simulação.
    """
    n = len(picks)
    # sample outcomes para cada jogo
    cat = RNG.multinomial(1, [1/3,1/3,1/3], size=1)  # dummy para criar shape
    draws = []
    for i in range(n):
        probs = P[i]
        # amostra outcomes: 0->"1",1->"X",2->"2"
        s = RNG.choice(3, size=n_sims, p=probs)
        draws.append(s)
    draws = np.stack(draws, axis=1)  # (n_sims, n_matches)
    # mapeia picks
    ok = np.ones(n_sims, dtype=bool)
    for j in range(n):
        allow = picks[j]
        allow_idx = set([ {"1":0,"X":1,"2":2}[a] for a in allow ])
        ok &= np.isin(draws[:,j], list(allow_idx))
        if not ok.any():  # curto-circuito
            return 0.0
    return float(ok.mean())

def _entropy_row(probs):
    p = probs.clip(1e-9, 1.0)
    p = p / p.sum()
    return float(-(p*np.log(p)).sum())

def _baseline_ticket(P):
    # "seco" com maior prob em cada jogo
    labels = np.argmax(P, axis=1)
    map_out = {0:"1",1:"X",2:"2"}
    return [ {map_out[i]} for i in labels ]

def _expand_ticket(P, ticket, max_duplos, max_triplos):
    """
    Greedy: adiciona duplos/triplos nos jogos de maior entropia.
    """
    n = len(ticket)
    ent = np.array([_entropy_row(P[i]) for i in range(n)])
    order = np.argsort(-ent)  # decrescente
    d_used = 0; t_used = 0
    for idx in order:
        if d_used < max_duplos:
            # transforma o seco em duplo: adiciona a 2ª melhor opção
            cur = ticket[idx]
            if len(cur)==1:
                sorted_idx = np.argsort(-P[idx])  # 0,1,2
                sec = sorted_idx[0]; second = sorted_idx[1]
                map_out = {0:"1",1:"X",2:"2"}
                ticket[idx] = {map_out[sec], map_out[second]}
                d_used += 1
                continue
        if t_used < max_triplos:
            # transforma o atual (seco ou duplo) em triplo
            if len(ticket[idx])<3:
                ticket[idx] = {"1","X","2"}
                t_used += 1
    return ticket

def _marginal_gain(P, portfolio, candidate, sims):
    # ganho marginal em Prob(≥1 acerta 14) adicionando "candidate"
    # aproximação: P(∪A_i) ≈ 1 - ∏(1 - P(A_i)) assumindo independência
    def p14(ticket):
        return _simulate_hits(P, ticket, n_sims=sims)
    existing = [p14(t) for t in portfolio]
    base = 1.0 - np.prod([1.0 - p for p in existing]) if existing else 0.0
    cand = p14(candidate)
    new = 1.0 - np.prod([1.0 - p for p in existing + [cand]])
    return new - base, cand

def main():
    ap = argparse.ArgumentParser(description="Gerador de portfólio de cartões")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--n-tickets", type=int, default=5)
    ap.add_argument("--max-duplos", type=int, default=4)
    ap.add_argument("--max-triplos", type=int, default=2)
    ap.add_argument("--sims-eval", type=int, default=25000)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    df = pd.read_csv(_pick_joined(base))
    need = {"match_id","home","away","p_home","p_draw","p_away"}
    if not need.issubset(df.columns):
        need2 = {"odd_home","odd_draw","odd_away"}
        if need2.issubset(df.columns):
            arr = df[["odd_home","odd_draw","odd_away"]].values.astype(float)
            with np.errstate(divide="ignore", invalid="ignore"):
                inv=1.0/arr
            inv[~np.isfinite(inv)] = 0.0
            s=inv.sum(axis=1, keepdims=True)
            P = inv/np.where(s>0,s,1.0)
        else:
            raise RuntimeError("[portfolio] joined* precisa ter p_* ou odds_*.") 
    else:
        P = df[["p_home","p_draw","p_away"]].values.astype(float)

    # ticket baseline + expansão
    n = len(df)
    portfolio=[]
    summary=[]
    for k in range(args.n_tickets):
        t0 = _baseline_ticket(P)
        tk = _expand_ticket(P, t0, args.max_duplos, args.max_triplos)
        gain, p14 = _marginal_gain(P, portfolio, tk, sims=args.sims_eval)
        portfolio.append(tk)
        summary.append({"ticket": k+1, "p14": round(p14,6), "marginal_gain": round(gain,6)})

    # salvar
    outdir = base/"portfolio"; outdir.mkdir(parents=True, exist_ok=True)
    for i, t in enumerate(portfolio, start=1):
        rows=[]
        for j, allow in enumerate(t, start=1):
            rows.append({
                "match_id": int(df.loc[j-1,"match_id"]),
                "home": df.loc[j-1,"home"],
                "away": df.loc[j-1,"away"],
                "pick": "".join(sorted(list(allow), key=lambda x: {"1":0,"X":1,"2":2}[x])),
                "tipo": {1:"SECO",2:"DUPLO",3:"TRIPLO"}[len(allow)],
                "probs_h|x|a": f"{P[j-1,0]:.3f}|{P[j-1,1]:.3f}|{P[j-1,2]:.3f}"
            })
        pd.DataFrame(rows).to_csv(outdir/f"cartao_{i:03d}.csv", index=False)

    pd.DataFrame(summary).to_csv(outdir/"portfolio_summary.csv", index=False)
    print(f"[portfolio] {len(portfolio)} cartões salvos em {outdir}")
    for s in summary:
        print(f"  - ticket {s['ticket']:03d}: p14={s['p14']} (marginal +{s['marginal_gain']})")

if __name__ == "__main__":
    main()
