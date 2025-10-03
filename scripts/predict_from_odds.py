#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, math
import pandas as pd
import numpy as np

REQ_COLS = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]

def implied_probs_no_overround(row):
    def inv(o):
        return (1.0/o) if (isinstance(o,(int,float,np.floating)) and o>1.0 and np.isfinite(o)) else 0.0
    invs = [inv(row.get("odds_home")), inv(row.get("odds_draw")), inv(row.get("odds_away"))]
    s = sum(invs)
    if s <= 0:
        return 0.0, 0.0, 0.0
    return invs[0]/s, invs[1]/s, invs[2]/s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data","out",args.rodada)
    in_path  = os.path.join(out_dir,"odds_consensus.csv")
    out_path = os.path.join(out_dir,"predictions_market.csv")

    if not os.path.exists(in_path):
        raise SystemExit(f"[predict] ERRO: arquivo não encontrado: {in_path}")

    df = pd.read_csv(in_path)
    miss = [c for c in REQ_COLS if c not in df.columns]
    if miss:
        raise SystemExit(f"[predict] ERRO: colunas ausentes em odds_consensus.csv: {miss}")

    rows = []
    for _, r in df.iterrows():
        ph,pd,pa = implied_probs_no_overround(r)
        triple = [("HOME",ph),("DRAW",pd),("AWAY",pa)]
        pick, conf = max(triple, key=lambda x: x[1])
        rows.append({
            "match_key": r["match_key"],
            "team_home": r["team_home"],
            "team_away": r["team_away"],
            "odds_home": r["odds_home"],
            "odds_draw": r["odds_draw"],
            "odds_away": r["odds_away"],
            "prob_home": ph,
            "prob_draw": pd,
            "prob_away": pa,
            "pick": pick,
            "confidence": conf,
        })
    out = pd.DataFrame(rows).sort_values("confidence", ascending=False)
    os.makedirs(out_dir, exist_ok=True)
    out.to_csv(out_path, index=False, float_format="%.6f")
    if args.debug:
        print(f"[predict] AMOSTRA (top 5): {out.head(5).to_dict(orient='records')}")
    print(f"[predict] OK -> {out_path} ({len(out)} linhas)")

if __name__ == "__main__":
    main()
