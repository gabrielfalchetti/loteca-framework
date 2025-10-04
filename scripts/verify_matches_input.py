#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verifica o arquivo data/in/<RODADA>/matches_source.csv:
 - existência
 - colunas obrigatórias (team_home, team_away, match_key)
 - duplicatas em match_key
 - valores vazios
Saída clara para logs do Actions.
"""

import argparse
import os
import sys
import pandas as pd


REQ_COLS = ["team_home", "team_away", "match_key"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    path = os.path.join("data", "in", args.rodada, "matches_source.csv")
    if not os.path.exists(path):
        print(f"[verify] ERRO: arquivo não encontrado: {path}")
        sys.exit(2)

    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[verify] ERRO ao ler CSV: {e}")
        sys.exit(3)

    cols = list(df.columns)
    missing = [c for c in REQ_COLS if c not in cols]
    if missing:
        print(f"[verify] ERRO: colunas ausentes: {missing}. Encontradas: {cols}")
        sys.exit(4)

    # Checa vazios nas obrigatórias
    null_counts = df[REQ_COLS].isna().sum().to_dict()
    if any(null_counts.values()):
        print(f"[verify] ERRO: valores vazios nas colunas obrigatórias: {null_counts}")
        sys.exit(5)

    # Checa duplicatas de chave
    dups = df["match_key"].duplicated(keep=False)
    if dups.any():
        print("[verify] ERRO: match_key duplicado nas linhas:")
        print(df.loc[dups, REQ_COLS].to_string(index=False))
        sys.exit(6)

    print(f"[verify] OK: {len(df)} linhas | colunas={cols}")
    print("[verify] AMOSTRA (até 5):")
    print(df.head(5).to_string(index=False))
    sys.exit(0)


if __name__ == "__main__":
    main()