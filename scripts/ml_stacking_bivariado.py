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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", default="")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    base_paths = [
        os.path.join(out_dir, "predictions_xg_uni.csv"),
        os.path.join(out_dir, "predictions_xg_bi.csv"),
        os.path.join(out_dir, "predictions_calibrated.csv"),
        os.path.join(out_dir, "predictions_market.csv"),  # do predict_from_odds.py
    ]

    frames = [saferead(p) for p in base_paths]
    frames = [f for f in frames if not f.empty]
    out_path = os.path.join(out_dir, "predictions_stacked.csv")

    if not frames:
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","prob_home","prob_draw","prob_away","pred","pred_conf"
        ]).to_csv(out_path, index=False)
        print(f"[ml] AVISO: sem entradas para stacking — gerado vazio em {out_path}")
        return 0

    # stacking simples: média ponderada — dá mais peso ao “calibrated” e ao “market”
    key = ["match_key","team_home","team_away"]
    merged = None
    weights = []

    for f in frames:
        cols_needed = key + ["prob_home","prob_draw","prob_away"]
        if not set(cols_needed).issubset(set(f.columns)):
            continue
        f = f[cols_needed].copy()
        if merged is None:
            merged = f
            merged.columns = key + ["prob_home_0","prob_draw_0","prob_away_0"]
        else:
            idx = sum(c.startswith("prob_home_") for c in merged.columns)
            merged = merged.merge(f, on=key, how="outer", suffixes=("",""))
            merged.rename(columns={
                "prob_home":f"prob_home_{idx}",
                "prob_draw":f"prob_draw_{idx}",
                "prob_away":f"prob_away_{idx}",
            }, inplace=True)

    if merged is None:
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","prob_home","prob_draw","prob_away","pred","pred_conf"
        ]).to_csv(out_path, index=False)
        print(f"[ml] AVISO: nada mesclado — gerado vazio em {out_path}")
        return 0

    # decide pesos: calibrated (se existir) = 0.4, market = 0.3, demais dividem 0.3
    cols = [c for c in merged.columns if c.startswith("prob_home_")]
    n = len(cols)
    w = np.full(n, 0.0)

    def col_index_by_suffix(sfx: str):
        for i in range(n):
            if f"prob_home_{i}" in merged.columns and f"prob_draw_{i}" in merged.columns and f"prob_away_{i}" in merged.columns:
                # não temos metadados; tentamos identificar pelas chaves disponíveis
                pass
        return None

    # heurística: se há arquivo "predictions_calibrated.csv", assume que ele entrou por último ou penúltimo
    # Como não temos rastreamento, usamos regra simples: último = market, penúltimo = calibrated (se existir ambos)
    # Ponderação: market 0.3, calibrated 0.4, restantes dividem 0.3
    if n >= 2:
        w[-1] = 0.30   # market (suposição)
        w[-2] = 0.40   # calibrated (suposição)
        if n > 2:
            rest = n - 2
            w[:rest] = 0.30 / rest
    else:
        # só 1 fonte
        w[:] = 1.0

    # aplica média ponderada
    ph = np.zeros(len(merged))
    pdw = np.zeros(len(merged))
    pa = np.zeros(len(merged))

    for i in range(n):
        ph_i = merged[f"prob_home_{i}"].fillna(0).to_numpy()
        pd_i = merged[f"prob_draw_{i}"].fillna(0).to_numpy()
        pa_i = merged[f"prob_away_{i}"].fillna(0).to_numpy()
        ph += w[i] * ph_i
        pdw += w[i] * pd_i
        pa  += w[i] * pa_i

    # renormaliza
    s = ph + pdw + pa
    s[s==0] = 1.0
    ph /= s; pdw /= s; pa /= s

    df = merged[["match_key","team_home","team_away"]].copy()
    df["prob_home"] = ph
    df["prob_draw"] = pdw
    df["prob_away"] = pa

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