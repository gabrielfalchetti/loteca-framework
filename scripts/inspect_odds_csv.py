#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Uso:
  python scripts/inspect_odds_csv.py --path data/out/<RODADA>/odds_theoddsapi.csv
Mostra cabeçalhos, 5 primeiras linhas e contagens de valores > 1.0 por coluna.
"""

import argparse
import pandas as pd

def cleanse(x):
    return pd.to_numeric(
        x.astype(str).str.replace(",", ".", regex=False).str.replace("%","",regex=False),
        errors="coerce"
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.path)
    print("[inspect] colunas:", list(df.columns))
    print("[inspect] head(5):")
    print(df.head(5).to_string(index=False))

    # tenta achar colunas com nomes comuns
    for col in ["odds_home","home_odds","h2h_home","home","moneyline_home",
                "odds_draw","draw_odds","h2h_draw","x","draw",
                "odds_away","away_odds","h2h_away","away","moneyline_away"]:
        if col in df.columns:
            s = cleanse(df[col])
            c_gt1 = (s > 1.0).sum()
            c_notna = s.notna().sum()
            print(f"[inspect] {col}: notna={c_notna}  >1.0={c_gt1}")

if __name__ == "__main__":
    main()