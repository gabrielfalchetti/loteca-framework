#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball_rapidapi.py — placeholder resiliente
Este script apenas garante a existência de `data/out/<rodada>/odds_apifootball.csv`
com colunas padronizadas. Integre sua coleta real aqui quando desejar.
"""

import argparse
import os
import pandas as pd
import numpy as np

COLS = ["home","away","book","k1","kx","k2","total_line","over","under","ts"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    path = os.path.join(out_dir,"odds_apifootball.csv")
    if not os.path.isfile(path):
        pd.DataFrame(columns=COLS).to_csv(path, index=False)

    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.DataFrame(columns=COLS)

    # Apenas garante colunas
    for c in COLS:
        if c not in df.columns:
            df[c] = np.nan

    df.to_csv(path, index=False)
    print(f"[apifootball] OK -> {path} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
