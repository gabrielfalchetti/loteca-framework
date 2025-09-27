# scripts/evaluate_portfolio_risk.py
from __future__ import annotations
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from risk_utils import load_prob_matrix, simulate_outcomes, portfolio_payouts, var_es

def parse_ticket_row(row: pd.Series) -> list[set[int]]:
    mapping = {"1":0, "X":1, "2":2}
    picks = []
    for j in range(1, 15):
        col = f"J{j}"
        if col not in row or pd.isna(row[col]):
            picks.append(set([0,1,2]))  # fallback triplo
            continue
        cell = str(row[col]).strip().upper()
        s=set()
        for ch in cell:
            if ch in mapping: s.add(mapping[ch])
        if not s: s = set([0,1,2])
        picks.append(s)
    return picks

def main():
    ap = argparse.ArgumentParser(description="Avalia risco do portfólio a partir de portfolio_plan.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--sims", type=int, default=100000)
    ap.add_argument("--paytable-json", default="")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    plan_path = base/"portfolio_plan.csv"
    if not plan_path.exists() or plan_path.stat().st_size==0:
        raise RuntimeError(f"[eval] portfolio_plan.csv ausente: {plan_path}")

    df_plan = pd.read_csv(plan_path)
    df, P = load_prob_matrix(args.rodada)
    sim = simulate_outcomes(P, n_sims=args.sims)

    # reconstruir tickets e pesos
    tickets=[]; weights=[]
    for _, r in df_plan.iterrows():
        tickets.append(parse_ticket_row(r))
        weights.append(float(r.get("stake_weight", 1.0)))
    weights = np.array(weights, dtype=float)
    if weights.sum() <= 0: weights = np.ones_like(weights)/len(weights)
    else: weights = weights / weights.sum()

    # paytable (opcional)
    import json
    pay_table = None
    if args.paytable_json.strip():
        try:
            raw = json.loads(args.paytable_json)
            pay_table = {int(k): float(v) for k, v in raw.items()}
        except Exception:
            pay_table = None

    returns = portfolio_payouts(sim, tickets, weights, pay_table=pay_table)
    var95, es95 = var_es(returns, alpha=0.95)

    # salva
    pd.DataFrame({"return": returns}).to_csv(base/"portfolio_returns_eval.csv", index=False)
    pd.DataFrame({"metric":["VaR95","ES95"], "value":[var95, es95]}).to_csv(base/"portfolio_risk_eval.csv", index=False)

    print(f"[eval] OK -> {base/'portfolio_risk_eval.csv'} | VaR95={var95:.4f} ES95={es95:.4f}")

if __name__ == "__main__":
    main()
