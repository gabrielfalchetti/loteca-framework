#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
import pandas as pd
import argparse

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--out", default=None)
    return ap.parse_args()

def main():
    args = parse_args()
    rodada = args.rodada
    outdir = f"data/out/{rodada}"
    os.makedirs(outdir, exist_ok=True)

    f1 = f"{outdir}/odds_theoddsapi.csv"
    f2 = f"{outdir}/odds_apifootball.csv"
    dfs = []
    used = {"theodds":0, "rapidapi":0}

    if os.path.exists(f1):
        df1 = pd.read_csv(f1)
        if len(df1)>0:
            used["theodds"] = len(df1)
            dfs.append(df1[["match_id","home_price","draw_price","away_price"]].assign(source="theodds"))
    if os.path.exists(f2):
        df2 = pd.read_csv(f2)
        if len(df2)>0:
            used["rapidapi"] = len(df2)
            # se vierem várias casas, faz média harmônica simples por resultado
            grp = df2.groupby("match_id", as_index=False).agg({
                "home_price":"mean","draw_price":"mean","away_price":"mean"
            })
            grp["source"] = "rapidapi"
            dfs.append(grp)

    if not dfs:
        print("[consensus] ERRO: nenhum provedor retornou odds. Aborte.", file=sys.stderr)
        sys.exit(1)

    # junta por prioridade (TheOddsAPI > RapidAPI para preencher faltantes)
    base = dfs[0]
    for df in dfs[1:]:
        base = base.combine_first(df)

    out_path = args.out or f"{outdir}/odds.csv"
    base.to_csv(out_path, index=False)
    print(f"[consensus] odds de consenso -> {out_path} (n={base['match_id'].nunique()})")
    print(f"[audit] Odds usadas: TheOddsAPI={used['theodds']} RapidAPI={used['rapidapi']}")

if __name__ == "__main__":
    main()
