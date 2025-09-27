# scripts/evaluate_ticket_ev.py
from __future__ import annotations
import argparse, yaml, os
from pathlib import Path
import numpy as np
import pandas as pd

RNG = np.random.default_rng(123)

DEFAULT_PRIZE = {
    "cost_per_ticket": 1.5,     # custo por cartão (ajuste para o valor real)
    "payout_14": 500000.0,      # prêmio esperado para 14 acertos
    "payout_13": 1200.0,        # prêmio médio para 13 acertos
    "kelly_fraction": 0.25      # fração de Kelly (25%)
}

def load_prize_model(path="config/prize_model.yml"):
    p = Path(path)
    if p.exists() and p.stat().st_size>0:
        with open(p, "r") as f:
            return yaml.safe_load(f)
    return DEFAULT_PRIZE.copy()

def load_joined(base: Path):
    for name in ["joined_referee.csv","joined_weather.csv","joined_enriched.csv","joined.csv"]:
        p = base/name
        if p.exists() and p.stat().st_size>0:
            df = pd.read_csv(p)
            if {"p_home","p_draw","p_away"}.issubset(df.columns):
                P = df[["p_home","p_draw","p_away"]].values.astype(float)
            else:
                arr = df[["odd_home","odd_draw","odd_away"]].values.astype(float)
                with np.errstate(divide="ignore", invalid="ignore"):
                    inv=1.0/arr
                inv[~np.isfinite(inv)] = 0.0
                s=inv.sum(axis=1, keepdims=True)
                P=inv/np.where(s>0,s,1.0)
            return df, P
    raise RuntimeError("[ev] nenhum joined* encontrado.")

def simulate_outcomes(P, n_sims=50000):
    # retorna matrix (n_sims, n_matches) de outcomes idx 0/1/2
    n = P.shape[0]
    out = np.zeros((n_sims, n), dtype=np.int8)
    for i in range(n):
        out[:,i] = RNG.choice(3, size=n_sims, p=P[i])
    return out

def eval_ticket(outcomes, ticket_picks):
    # outcomes: (n_sims, n)
    # ticket_picks: list[set], com "1","X","2"
    idx_map = {"1":0,"X":1,"2":2}
    allow = [ set(idx_map[a] for a in s) for s in ticket_picks ]
    ok = np.ones(outcomes.shape[0], dtype=bool)
    for j, al in enumerate(allow):
        ok &= np.isin(outcomes[:,j], list(al))
        if not ok.any(): return 0.0, 0.0
    # 14 acertos
    p14 = float(ok.mean())
    # 13 acertos: erra exatamente 1 jogo
    n = outcomes.shape[1]
    p13 = 0.0
    for miss in range(n):
        ok13 = np.ones(outcomes.shape[0], dtype=bool)
        for j, al in enumerate(allow):
            if j == miss:
                ok13 &= ~np.isin(outcomes[:,j], list(al))
            else:
                ok13 &= np.isin(outcomes[:,j], list(al))
            if not ok13.any(): break
        p13 += ok13.mean()
    return p14, p13

def parse_ticket_csv(path: Path):
    df = pd.read_csv(path)
    picks=[]
    for _, r in df.iterrows():
        s = str(r["pick"]).upper().strip()
        picks.append(set(list(s)))
    return picks

def main():
    ap = argparse.ArgumentParser(description="Avaliação de EV e Kelly para cartões")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--portfolio-dir", default=None, help="diretório com cartao_*.csv; se vazio, usa data/out/<rodada>/portfolio")
    ap.add_argument("--n-sims", type=int, default=60000)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    joined_df, P = load_joined(base)
    outcomes = simulate_outcomes(P, n_sims=args.n_sims)

    prize = load_prize_model()
    cpt = float(prize.get("cost_per_ticket", DEFAULT_PRIZE["cost_per_ticket"]))
    pay14 = float(prize.get("payout_14", DEFAULT_PRIZE["payout_14"]))
    pay13 = float(prize.get("payout_13", DEFAULT_PRIZE["payout_13"]))
    kfrac = float(prize.get("kelly_fraction", DEFAULT_PRIZE["kelly_fraction"]))

    port_dir = Path(args.portfolio_dir) if args.portfolio_dir else (base/"portfolio")
    if not port_dir.exists():
        raise RuntimeError(f"[ev] diretório de portfólio não existe: {port_dir}")

    rows=[]
    for f in sorted(port_dir.glob("cartao_*.csv")):
        picks = parse_ticket_csv(f)
        p14, p13 = eval_ticket(outcomes, picks)
        ev = p14*pay14 + p13*pay13 - cpt
        # Kelly fracionado: proxy simples — stake* = k * EV / cost (cap 0..1)
        edge = ev / cpt
        stake = kfrac * max(0.0, edge)
        stake = float(min(1.0, stake))
        rows.append({
            "file": f.name,
            "p14": round(p14,8),
            "p13": round(p13,8),
            "EV": round(ev,2),
            "kelly_frac": kfrac,
            "suggested_stake_per_ticket": round(stake,4)
        })

    out = pd.DataFrame(rows)
    out_path = port_dir/"portfolio_ev.csv"
    out.to_csv(out_path, index=False)
    print(f"[ev] EV e Kelly calculados -> {out_path}")
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
