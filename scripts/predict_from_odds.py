#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Predição baseline a partir de odds de mercado (consenso).

Entrada:
  - {OUT_DIR}/odds_consensus.csv
Saída:
  - {OUT_DIR}/predictions_market.csv
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np

REQ_COLS = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]

def die(msg: str):
    print(f"[predict] ERRO: {msg}", file=sys.stderr)
    sys.exit(2)

def implied_probs(oh, od, oa):
    # prob ~ 1/odd; normaliza
    vals = np.array([1.0/oh, 1.0/od, 1.0/oa], dtype=float)
    s = vals.sum()
    if s <= 0: return (np.nan, np.nan, np.nan)
    vals = vals / s
    return tuple(vals.tolist())

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="OUT_DIR")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    out_dir = args.rodada
    in_path = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(in_path):
        die(f"{in_path} não encontrado")

    df = pd.read_csv(in_path)
    for c in REQ_COLS:
        if c not in df.columns:
            die(f"coluna ausente em odds_consensus.csv: {c}")

    rows = []
    for _, r in df.iterrows():
        try:
            oh = float(r["odds_home"])
            od = float(r["odds_draw"])
            oa = float(r["odds_away"])
            ph, pd, pa = implied_probs(oh, od, oa)
            probs = {"HOME": ph, "DRAW": pd, "AWAY": pa}
            pred = max(probs, key=probs.get)
            rows.append({
                "match_key": r["match_key"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "odds_home": oh, "odds_draw": od, "odds_away": oa,
                "prob_home": ph, "prob_draw": pd, "prob_away": pa,
                "pred": pred, "pred_conf": probs[pred]
            })
        except Exception as e:
            if args.debug:
                print(f"[predict][DEBUG] linha ignorada: {e}")

    if not rows:
        die("nenhuma linha válida para predição")

    outp = os.path.join(out_dir, "predictions_market.csv")
    pd.DataFrame(rows).to_csv(outp, index=False)

    if args.debug:
        import json
        print(f"[predict] AMOSTRA (top 5): {json.dumps(rows[:5], ensure_ascii=False)}")
    print(f"[predict] OK -> {outp} ({len(rows)} linhas; válidas p/ predição: {len(rows)})")

if __name__ == "__main__":
    main()