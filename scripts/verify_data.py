#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Verifica a integridade do arquivo histórico de resultados (results.csv).
Checa duplicatas, valores inválidos (gols negativos, NaN) e consistência de dados.

Uso:
  python -m scripts.verify_data --history data/history/results.csv
"""

from __future__ import annotations

import os
import argparse
import pandas as pd
from typing import Optional

def _log(msg: str) -> None:
    print(f"[verify_data] {msg}", flush=True)

def verify_data(history: str) -> bool:
    """Verifica integridade do CSV histórico."""
    if not os.path.isfile(history):
        _log(f"{history} não encontrado")
        return False

    try:
        df = pd.read_csv(history)
        required_cols = ["date", "home", "away", "home_goals", "away_goals", "xG_home", "xG_away"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas ausentes: {missing}")

        # Checar duplicatas
        if df.duplicated(subset=["date", "home", "away"]).any():
            _log("Duplicatas detectadas, removendo...")
            df = df.drop_duplicates(subset=["date", "home", "away"])
            df.to_csv(history, index=False)

        # Checar valores inválidos
        for col in ["home_goals", "away_goals", "xG_home", "xG_away"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            if (df[col] < 0).any():
                _log(f"Valores negativos em {col}, ajustando para 0...")
                df[col] = df[col].clip(lower=0)
            if df[col].isna().any():
                _log(f"NaN em {col}, preenchendo com 0...")
                df[col] = df[col].fillna(0)

        # Checar consistência de datas
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if df["date"].isna().any():
            raise ValueError("Datas inválidas detectadas")

        df.to_csv(history, index=False)
        _log(f"OK — {len(df)} linhas validadas em {history}")
        return True

    except Exception as e:
        _log(f"[CRITICAL] Erro: {e}")
        return False

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=str, required=True, help="Caminho do CSV de histórico")
    args = parser.parse_args()
    if not verify_data(args.history):
        sys.exit(1)

if __name__ == "__main__":
    main()