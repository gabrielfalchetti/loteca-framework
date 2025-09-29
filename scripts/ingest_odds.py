#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, argparse, pandas as pd

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--out", default=None)
    return ap.parse_args()

def main():
    args = parse_args()
    outdir = f"data/out/{args.rodada}"
    os.makedirs(outdir, exist_ok=True)

    f_od1 = f"{outdir}/odds_theoddsapi.csv"
    f_od2 = f"{outdir}/odds_apifootball.csv"

    dfs = []
    used = {"theodds":0, "rapidapi":0}

    if os.path.exists(f_od1):
        d1 = pd.read_csv(f_od1)
        if len(d1) > 0:
            used["theodds"] = len(d1)
            d1 = d1.groupby("match_id", as_index=False).agg({
                "home_price":"mean","draw_price":"mean","away_price":"mean"
            }).assign(source="theodds")
            dfs.append(d1)

    if os.path.exists(f_od2):
        d2 = pd.read_csv(f_od2)
        if len(d2) > 0:
            used["rapidapi"] = len(d2)
            d2 = d2.groupby("match_id", as_index=False).agg({
                "home_price":"mean","draw_price":"mean","away_price":"mean"
            }).assign(source="rapidapi")
            dfs.append(d2)

    if not dfs:
        print("[consensus] ERRO: nenhum provedor retornou odds. Aborte.", file=sys.stderr)
        sys.exit(1)

    # prioridade: TheOddsAPI > RapidAPI
    base = dfs[0]
    for df in dfs[1:]:
        base = base.combine_first(df)

    out = args.out or f"{outdir}/odds.csv"
    base.to_csv(out, index=False)
    print(f"[consensus] odds de consenso -> {out} (n={base['match_id'].nunique()})")
    print(f"[audit] Odds usadas: TheOddsAPI={used['theodds']} RapidAPI={used['rapidapi']}")

if __name__ == "__main__":
    main()
