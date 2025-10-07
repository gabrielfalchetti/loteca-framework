#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Calibração simples das probabilidades implícitas (valida e, se houver labels no OUT_DIR/labels.csv, plota tabela de bins).

Entrada:
  <OUT_DIR>/features_univariado.csv
  (opcional) <OUT_DIR>/labels.csv com colunas: match_key, result (valores em {1,'X',2})

Saída:
  <OUT_DIR>/calibration_report.json
  (se labels) <OUT_DIR>/calibration_bins.csv
"""

import argparse, os, sys, json, pandas as pd, numpy as np

def log(m): print(f"[calib] {m}")
def die(c,m): log(m); sys.exit(c)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true", default=False)
    args = ap.parse_args()

    od = args.rodada
    uni_p = os.path.join(od, "features_univariado.csv")
    if not os.path.exists(uni_p): die(24,"features_univariado.csv não encontrado")
    df = pd.read_csv(uni_p)

    # sanity das probs
    probs = df[["imp_home","imp_draw","imp_away"]].clip(0,1)
    sums = probs.sum(axis=1)
    report = {
        "n_matches": int(df.shape[0]),
        "probs_in_[0,1]_rows": int((probs.ge(0)&probs.le(1)).all(axis=None)),
        "mean_sum_probs": float(sums.mean()),
        "std_sum_probs": float(sums.std(ddof=0)),
        "min_sum_probs": float(sums.min()),
        "max_sum_probs": float(sums.max())
    }

    # labels opcionais
    labels_p = os.path.join(od, "labels.csv")
    if os.path.exists(labels_p):
        lab = pd.read_csv(labels_p)
        lab = lab[["match_key","result"]].dropna()
        m = df.merge(lab, on="match_key", how="inner")
        if not m.empty:
            # binning simples por prob do pick favorito
            fav_p = m[["imp_home","imp_draw","imp_away"]].max(axis=1)
            bins = pd.cut(fav_p, bins=[0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0], right=True, include_lowest=True)
            # acerto real: 1 se favorito venceu
            idx = m[["imp_home","imp_draw","imp_away"]].idxmax(axis=1)
            pick = idx.map({"imp_home":"1","imp_draw":"X","imp_away":"2"})
            y = (pick == m["result"].astype(str)).astype(int)
            tab = m.assign(fav_bin=bins, correct=y).groupby("fav_bin")["correct"].agg(["mean","count"]).reset_index()
            tab.rename(columns={"mean":"empirical_accuracy","count":"n"}, inplace=True)
            bins_p = os.path.join(od,"calibration_bins.csv")
            tab.to_csv(bins_p, index=False)
            report["bins_file"] = os.path.relpath(bins_p)
            report["has_labels"] = True
            report["overall_accuracy"] = float(y.mean())
        else:
            report["has_labels"] = False
    else:
        report["has_labels"] = False

    out_j = os.path.join(od, "calibration_report.json")
    with open(out_j,"w",encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    log(f"OK -> {out_j}")
    if args.debug:
        print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()