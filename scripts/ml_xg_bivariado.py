#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
from typing import Tuple
import pandas as pd
import numpy as np

def log(msg: str, debug: bool = False):
    if debug:
        print(f"[ml_xg_bi][DEBUG] {msg}")

def implied_probs(row: pd.Series) -> Tuple[float, float, float]:
    o_home, o_draw, o_away = row["odds_home"], row["odds_draw"], row["odds_away"]
    if any(pd.isna([o_home, o_draw, o_away])) or (o_home <= 1) or (o_draw <= 1) or (o_away <= 1):
        return (np.nan, np.nan, np.nan)
    inv = np.array([1.0/o_home, 1.0/o_draw, 1.0/o_away], dtype=float)
    s = inv.sum()
    return tuple((inv / s).tolist())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", default="")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    in_dir = os.path.join("data", "in", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    odds_path = os.path.join(out_dir, "odds_consensus.csv")
    matches_path = os.path.join(in_dir, "matches_source.csv")
    out_path = os.path.join(out_dir, "predictions_xg_bi.csv")

    if not os.path.exists(odds_path):
        print(f"[ml] AVISO: {odds_path} não encontrado — pulando XG bivariado.")
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","prob_home","prob_draw","prob_away","pred","pred_conf"
        ]).to_csv(out_path, index=False)
        return 0

    df_odds = pd.read_csv(odds_path)
    req_cols = {"match_key","team_home","team_away","odds_home","odds_draw","odds_away"}
    missing = req_cols - set(df_odds.columns)
    if missing:
        print(f"[ml] ERRO: colunas ausentes em odds_consensus.csv: {sorted(missing)}")
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","prob_home","prob_draw","prob_away","pred","pred_conf"
        ]).to_csv(out_path, index=False)
        return 0

    # baseline bivariado: “puxa” um pouco a prob do mandante usando a razão de odds
    probs = df_odds.apply(implied_probs, axis=1, result_type="expand")
    probs.columns = ["prob_home","prob_draw","prob_away"]

    # pequeno ajuste bivariado usando log-odds home vs away
    def adjust(row):
        ph, pd_, pa = row["prob_home"], row["prob_draw"], row["prob_away"]
        if any(pd.isna([ph, pd_, pa])): 
            return row
        # ajuste sutil: se odd_home << odd_away, aumenta levemente prob_home
        oh, oa = row["odds_home"], row["odds_away"]
        if oh>1 and oa>1:
            bias = np.clip(np.log(oa/oh)/10.0, -0.08, 0.08)  # +-8pp
            ph2 = np.clip(ph + bias, 0, 1)
            rem = 1 - ph2
            # re-normaliza draw/away mantendo proporção original
            k = pd_ + pa
            if k>0:
                pd2 = rem * (pd_/k)
                pa2 = rem * (pa/k)
            else:
                pd2 = rem*0.5
                pa2 = rem*0.5
            return pd.Series({"prob_home":ph2, "prob_draw":pd2, "prob_away":pa2})
        return row[["prob_home","prob_draw","prob_away"]]

    adj = pd.concat([df_odds[["odds_home","odds_away"]], probs], axis=1).apply(adjust, axis=1, result_type="expand")
    adj.columns = ["prob_home","prob_draw","prob_away"]
    df_pred = pd.concat([df_odds[["match_key","team_home","team_away"]], adj], axis=1)

    def pick_pred(r):
        arr = np.array([r["prob_home"], r["prob_draw"], r["prob_away"]], dtype=float)
        if np.any(np.isnan(arr)):
            return pd.Series({"pred": np.nan, "pred_conf": np.nan})
        idx = int(np.argmax(arr))
        label = ["HOME","DRAW","AWAY"][idx]
        return pd.Series({"pred": label, "pred_conf": float(arr[idx])})

    picks = df_pred.apply(pick_pred, axis=1)
    df_pred = pd.concat([df_pred, picks], axis=1)

    df_pred.to_csv(out_path, index=False)
    sample = df_pred.head(5).to_dict(orient="records")
    print(f"[ml] OK -> {out_path} ({len(df_pred)} linhas) | AMOSTRA: {json.dumps(sample, ensure_ascii=False)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())