#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório de risco para a rodada:
- Checa odds presentes
- Checa probabilidade bem-formada (p1+px+p2 ~ 1)
- Marca flags úteis p/ o picker

Entrada esperada:
  data/out/{rodada}/matches.csv           (match_id, home_team, away_team, ...)
  data/out/{rodada}/probabilities_calibrated.csv  (ou probabilities.csv) (match_id, p1, px, p2)
  data/out/{rodada}/odds.csv              (match_id, ...)

Saída:
  data/out/{rodada}/risk_report.csv
"""

import argparse
import os
import sys
import pandas as pd
import numpy as np

def read_csv_force_str(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)  # força tudo como string
    # converte probs se existirem
    for col in ["p1","px","p2"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    base = f"data/out/{args.rodada}"
    p_matches = os.path.join(base, "matches.csv")
    p_probs_cal = os.path.join(base, "probabilities_calibrated.csv")
    p_probs = os.path.join(base, "probabilities.csv")
    p_odds = os.path.join(base, "odds.csv")
    out_path = os.path.join(base, "risk_report.csv")

    df_matches = read_csv_force_str(p_matches)
    df_odds = read_csv_force_str(p_odds)

    # probabilities_calibrated preferência; se não existir, usa probabilities
    df_probs = read_csv_force_str(p_probs_cal) if os.path.exists(p_probs_cal) else read_csv_force_str(p_probs)

    if df_matches.empty:
        print("[risk] ERRO: matches.csv não encontrado/sem linhas.", flush=True)
        sys.exit(1)
    if df_probs.empty:
        print("[risk] ERRO: probabilities(.csv|_calibrated.csv) não encontrado/sem linhas.", flush=True)
        sys.exit(1)
    if df_odds.empty:
        print("[risk] ERRO: odds.csv não encontrado/sem linhas.", flush=True)
        sys.exit(1)

    # Normaliza chave
    for df in (df_matches, df_probs, df_odds):
        if "match_id" not in df.columns:
            print("[risk] ERRO: falta coluna 'match_id' em um dos arquivos.", flush=True)
            sys.exit(1)
        df["match_id"] = df["match_id"].astype(str)

    # Merge
    df = df_matches.merge(df_probs, on="match_id", how="left", suffixes=("",""))
    df = df.merge(df_odds, on="match_id", how="left", suffixes=("","_odds"))

    # Regras de risco
    df["risk_missing_odds"] = df.filter(like="_odds").isna().all(axis=1).astype(int)
    df["risk_missing_probs"] = df[["p1","px","p2"]].isna().any(axis=1).astype(int)
    df["sum_probs"] = df[["p1","px","p2"]].sum(axis=1)
    df["risk_bad_calibration"] = ((df["sum_probs"] < 0.98) | (df["sum_probs"] > 1.02)).astype(int)

    # Score simples
    df["risk_score"] = df[["risk_missing_odds","risk_missing_probs","risk_bad_calibration"]].sum(axis=1)

    keep_cols = [c for c in ["match_id","home_team","away_team","p1","px","p2","sum_probs",
                             "risk_missing_odds","risk_missing_probs","risk_bad_calibration","risk_score"]
                 if c in df.columns]
    df_out = df[keep_cols].copy()
    df_out.to_csv(out_path, index=False)
    print(f"[risk] OK -> {out_path} ({len(df_out)} linhas)", flush=True)

if __name__ == "__main__":
    main()
