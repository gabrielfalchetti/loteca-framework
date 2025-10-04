# -*- coding: utf-8 -*-
"""
Gera predições a partir de odds_consensus.csv (ou, fallback, odds_theoddsapi.csv)
- Converte odds -> probabilidades implícitas
- Remove overround via normalização proporcional
- Escolhe o maior (HOME/DRAW/AWAY) como predição
Saída: data/out/<rodada>/predictions_market.csv
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import json

def implied_probs(row):
    oh, od, oa = row["odds_home"], row["odds_draw"], row["odds_away"]
    vals = []
    for x in (oh, od, oa):
        vals.append(0.0 if (pd.isna(x) or float(x)<=1.0) else 1.0/float(x))
    s = sum(vals)
    if s == 0:
        return 0,0,0
    # normaliza (remove overround)
    return vals[0]/s, vals[1]/s, vals[2]/s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # preferir consenso; fallback theoddsapi
    in_path = out_dir/"odds_consensus.csv"
    if not in_path.exists() or in_path.stat().st_size == 0:
        in_path = out_dir/"odds_theoddsapi.csv"
    df = pd.read_csv(in_path)

    # probabilidades
    ps = df.apply(implied_probs, axis=1, result_type="expand")
    ps.columns = ["prob_home","prob_draw","prob_away"]
    df = pd.concat([df, ps], axis=1)

    # predição = argmax
    def _pred(row):
        arr = [row["prob_home"], row["prob_draw"], row["prob_away"]]
        lab = ["HOME","DRAW","AWAY"]
        m = max(arr)
        return lab[arr.index(m)], m
    preds = df.apply(lambda r: _pred(r), axis=1, result_type="expand")
    preds.columns = ["pred","pred_conf"]
    df = pd.concat([df, preds], axis=1)

    out = df[["match_key","team_home","team_away","odds_home","odds_draw","odds_away","prob_home","prob_draw","prob_away","pred","pred_conf"]]
    out.to_csv(out_dir/"predictions_market.csv", index=False)

    if args.debug:
        sample = out.head(5).to_dict(orient="records")
        print("[predict] AMOSTRA (top 5): " + json.dumps(sample, ensure_ascii=False))

    print(f"[predict] OK -> {out_dir/'predictions_market.csv'} ({len(out)} linhas; válidas p/ predição: {len(out)})")

if __name__ == "__main__":
    main()