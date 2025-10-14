# -*- coding: utf-8 -*-
import argparse, json, os, sys, pandas as pd, numpy as np

# Modelo dinâmico mínimo: estados por time (ataque/defesa) com atualização exponencial simples.

def fit_states(df: pd.DataFrame, span: int = 10):
    # df esperado do feature_engineer long-form: date, team, gf, ga, pts, etc.
    df = df.copy()
    if "team" not in df.columns or "gf" not in df.columns or "ga" not in df.columns:
        raise ValueError("features sem colunas esperadas (team,gf,ga)")
    df = df.sort_values("date")
    alpha = 2.0/(span+1.0)

    states = {}
    for team, g in df.groupby("team"):
        atk, dfn = 0.0, 0.0
        seen = 0
        for _, r in g.iterrows():
            seen += 1
            atk = (1-alpha)*atk + alpha*float(r["gf"])
            dfn = (1-alpha)*dfn + alpha*float(r["ga"])
        if seen == 0: continue
        states[team] = {"attack": max(atk, 0.1), "defense": max(dfn, 0.1), "home_adv": 0.15}
    return states

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--out_state", required=True)
    ap.add_argument("--out_model", required=True)
    args = ap.parse_args()

    feats = pd.read_parquet(args.features)
    states = fit_states(feats, span=12)

    os.makedirs(os.path.dirname(args.out_state), exist_ok=True)
    with open(args.out_state, "w", encoding="utf-8") as f:
        json.dump(states, f, ensure_ascii=False, indent=2)
    # modelo picklável (placeholder)
    with open(args.out_model, "wb") as f:
        f.write(b"DYNMODEL_PLACEHOLDER")
    print(f"[train_dynamic] estados: {len(states)} times -> {args.out_state}")

if __name__ == "__main__":
    main()