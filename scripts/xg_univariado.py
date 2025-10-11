# scripts/xg_univariado.py
import argparse, sys, os
import pandas as pd
import numpy as np

REQ_COLS = ["team_home","team_away","odds_home","odds_draw","odds_away"]

def implied_probs(row):
    # Probabilidades implícitas com ajuste de overround
    inv = np.array([1/row["odds_home"], 1/row["odds_draw"], 1/row["odds_away"]], dtype=float)
    s = inv.sum()
    if s <= 0:
        return pd.Series([np.nan, np.nan, np.nan], index=["p_home","p_draw","p_away"])
    p = inv / s
    return pd.Series({"p_home":p[0], "p_draw":p[1], "p_away":p[2]})

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
    for c in REQ_COLS:
        if c not in df.columns:
            sys.exit(f"missing column '{c}' in odds_consensus.csv")

    probs = df.apply(implied_probs, axis=1)
    dfu = pd.concat([df[["team_home","team_away"]], probs], axis=1)

    # Heurística neutra p/ xG derivada das probabilidades (sem “inventar” dados externos):
    # média de gols do jogo ~ 2.4; divide por “força” de cada lado ~ prob de não-perder
    mean_goals = 2.4
    strength_home = dfu["p_home"] + 0.5*dfu["p_draw"]
    strength_away = dfu["p_away"] + 0.5*dfu["p_draw"]
    total = strength_home + strength_away
    # Evita divisão por zero
    total = total.replace(0, np.nan)

    dfu["xg_home_uni"] = mean_goals * (strength_home / total)
    dfu["xg_away_uni"] = mean_goals * (strength_away / total)

    out = os.path.join(out_dir, "xg_univariate.csv")
    dfu.to_csv(out, index=False)
    if args.debug:
        print(dfu.head())

if __name__ == "__main__":
    main()