#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cria features bivariadas (interações simples) a partir do univariado.

Entrada: <OUT_DIR>/features_univariado.csv
Saída:   <OUT_DIR>/features_bivariado.csv
"""

import argparse, os, sys, pandas as pd, numpy as np, json

def log(m): print(f"[bivariado] {m}")
def die(c,m): log(m); sys.exit(c)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (data/out/<RID>)")
    ap.add_argument("--debug", action="store_true", default=False)
    args = ap.parse_args()

    od = args.rodada
    uni_p = os.path.join(od, "features_univariado.csv")
    if not os.path.exists(uni_p): die(22,f"features_univariado.csv não encontrado em {uni_p}")
    df = pd.read_csv(uni_p)
    if df.empty: die(22,"features_univariado.csv vazio")

    # Interações e diffs
    df["diff_ph_pa"] = df["imp_home"] - df["imp_away"]
    df["ratio_ph_pa"] = (df["imp_home"] / df["imp_away"]).replace([np.inf,-np.inf], np.nan)
    df["diff_fair_home_away"] = df["fair_p_home"] - df["fair_p_away"]
    df["gap_value_home_away"] = df["value_home"] - df["value_away"]
    df["entropy_x_gap"] = df["entropy_bits"] * df["gap_top_second"]
    df["overround_x_entropy"] = df["overround"] * df["entropy_bits"]

    out_cols = [
        "match_key","home","away",
        "diff_ph_pa","ratio_ph_pa",
        "diff_fair_home_away","gap_value_home_away",
        "entropy_x_gap","overround_x_entropy"
    ]
    out_p = os.path.join(od, "features_bivariado.csv")
    df[out_cols].to_csv(out_p, index=False)

    if not os.path.exists(out_p) or os.path.getsize(out_p)==0:
        die(22,"features_bivariado.csv não gerado")

    meta = {"rows": int(df.shape[0]), "source": os.path.relpath(uni_p)}
    with open(os.path.join(od,"features_bivariado_meta.json"),"w",encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    log(f"OK -> {out_p} ({df.shape[0]} jogos)")
    if args.debug:
        print(df[out_cols].head(10).to_string(index=False))

if __name__ == "__main__":
    main()