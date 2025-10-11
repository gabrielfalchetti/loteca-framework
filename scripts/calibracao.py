# scripts/calibracao.py
import argparse, os, sys
import pandas as pd
import numpy as np

REQ = ["team_home","team_away","odds_home","odds_draw","odds_away"]

def implied(row):
    inv = np.array([1/row["odds_home"], 1/row["odds_draw"], 1/row["odds_away"]], dtype=float)
    s = inv.sum()
    if s <= 0:
        return pd.Series([np.nan, np.nan, np.nan], index=["p_home","p_draw","p_away"])
    return pd.Series(inv/s, index=["p_home","p_draw","p_away"])

def debias_platt(p, k=0.02):
    # Correção conservadora de overround: suaviza levemente em direção à distribuição uniforme (sem inventar dados externos).
    # p' = (1-k)*p + k/3
    return (1-k)*p + (k/3.0)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", dest="debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    cons = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.exists(cons):
        sys.exit("odds_consensus.csv not found")

    df = pd.read_csv(cons)
    for c in REQ:
        if c not in df.columns:
            sys.exit(f"missing column '{c}' in odds_consensus.csv")

    probs = df.apply(implied, axis=1)
    dfp = pd.concat([df[["team_home","team_away"]], probs], axis=1)
    # Calibração simples (conservadora)
    for col in ["p_home","p_draw","p_away"]:
        dfp[col] = debias_platt(dfp[col].clip(0,1))

    # renormaliza pra somar 1
    s = (dfp["p_home"] + dfp["p_draw"] + dfp["p_away"]).replace(0, np.nan)
    for col in ["p_home","p_draw","p_away"]:
        dfp[col] = dfp[col] / s

    out = os.path.join(out_dir, "probs_calibrated.csv")
    dfp.to_csv(out, index=False)
    if args.debug:
        print(dfp.head())

if __name__ == "__main__":
    main()