#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
import pandas as pd
import numpy as np

def saferead(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def renorm(row):
    arr = np.array([row["prob_home"], row["prob_draw"], row["prob_away"]], dtype=float)
    if np.any(np.isnan(arr)):
        return row
    s = arr.sum()
    if s <= 0:
        return row
    arr /= s
    row["prob_home"], row["prob_draw"], row["prob_away"] = arr.tolist()
    return row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", default="")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    uni_path = os.path.join(out_dir, "predictions_xg_uni.csv")
    bi_path  = os.path.join(out_dir, "predictions_xg_bi.csv")
    out_path = os.path.join(out_dir, "predictions_calibrated.csv")

    df_uni = saferead(uni_path)
    df_bi  = saferead(bi_path)

    # fallback: se não houver ambos, usa o que existir; se nada existir, escreve vazio
    df = None
    if not df_uni.empty and not df_bi.empty:
        # média simples das probabilidades
        keycols = ["match_key", "team_home", "team_away"]
        cols = keycols + ["prob_home","prob_draw","prob_away"]
        df = df_uni[cols].merge(df_bi[cols], on=keycols, suffixes=("_uni","_bi"))
        for c in ["home","draw","away"]:
            df[f"prob_{c}"] = (df[f"prob_{c}_uni"] + df[f"prob_{c}_bi"]) / 2.0
        df = df[keycols + ["prob_home","prob_draw","prob_away"]]
    elif not df_uni.empty:
        df = df_uni[["match_key","team_home","team_away","prob_home","prob_draw","prob_away"]].copy()
    elif not df_bi.empty:
        df = df_bi[["match_key","team_home","team_away","prob_home","prob_draw","prob_away"]].copy()
    else:
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","prob_home","prob_draw","prob_away","pred","pred_conf"
        ]).to_csv(out_path, index=False)
        print(f"[ml] AVISO: sem entradas para calibrar — gerado vazio em {out_path}")
        return 0

    # “calibração isotônica” placeholder: renormalização suave (sem dados rotulados)
    df = df.apply(renorm, axis=1)

    def pick_pred(r):
        arr = np.array([r["prob_home"], r["prob_draw"], r["prob_away"]], dtype=float)
        if np.any(np.isnan(arr)):
            return pd.Series({"pred": np.nan, "pred_conf": np.nan})
        idx = int(np.argmax(arr))
        label = ["HOME","DRAW","AWAY"][idx]
        return pd.Series({"pred": label, "pred_conf": float(arr[idx])})

    picks = df.apply(pick_pred, axis=1)
    df = pd.concat([df, picks], axis=1)

    df.to_csv(out_path, index=False)
    sample = df.head(5).to_dict(orient="records")
    print(f"[ml] OK -> {out_path} ({len(df)} linhas) | AMOSTRA: {json.dumps(sample, ensure_ascii=False)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())