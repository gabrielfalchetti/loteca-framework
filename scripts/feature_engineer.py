#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera features a partir de um CSV de resultados no formato:

date,home,away,home_goals,away_goals

Saída: Parquet (requer pyarrow ou fastparquet), com features EWMA por time.
Mantém comportamento "fail-safe": lança erro claro se o CSV não tem conteúdo válido.

Uso:
  python -m scripts.feature_engineer \
      --history data/history/results.csv \
      --out data/history/features.parquet \
      --ewma 0.20
"""

from __future__ import annotations

import argparse
import sys
import os
import pandas as pd
from pandas import DataFrame


def _log(msg: str) -> None:
    print(f"[features] {msg}", flush=True)


def _validate_history(df: DataFrame) -> None:
    required = ["date", "home", "away", "home_goals", "away_goals"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"history sem colunas requeridas: {missing}")

    if len(df) == 0:
        raise ValueError("history vazio")

    # Tipos mínimos
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isna().all():
        raise ValueError("coluna 'date' inválida (nenhum parse possível)")

    for c in ["home_goals", "away_goals"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if df[["home_goals", "away_goals"]].isna().any().any():
        # substitui NaN por 0 para robustez — mas poderia também abortar
        df["home_goals"] = df["home_goals"].fillna(0)
        df["away_goals"] = df["away_goals"].fillna(0)


def _long_format(df: DataFrame) -> DataFrame:
    """
    Duplica as linhas, uma por time (home/away), para calcular EWM por time.
    Campos: date, team, opponent, gf, ga, is_home
    """
    home = df[["date", "home", "away", "home_goals", "away_goals"]].copy()
    home.columns = ["date", "team", "opponent", "gf", "ga"]
    home["is_home"] = 1

    away = df[["date", "home", "away", "home_goals", "away_goals"]].copy()
    away.columns = ["date", "opponent", "team", "ga", "gf"]  # note a troca gf/ga
    away["is_home"] = 0

    long_df = pd.concat([home, away], ignore_index=True)
    long_df.sort_values(["team", "date"], inplace=True)
    return long_df


def _ewm_features(long_df: DataFrame, alpha: float) -> DataFrame:
    """
    Calcula EWM por time para métricas básicas.
    """
    def _grp(g: DataFrame) -> DataFrame:
        g = g.sort_values("date").copy()
        g["ewm_gf"] = g["gf"].ewm(alpha=alpha, adjust=False).mean()
        g["ewm_ga"] = g["ga"].ewm(alpha=alpha, adjust=False).mean()
        g["ewm_gd"] = (g["gf"] - g["ga"]).ewm(alpha=alpha, adjust=False).mean()
        g["ewm_home_rate"] = g["is_home"].ewm(alpha=alpha, adjust=False).mean()
        return g

    out = long_df.groupby("team", group_keys=False).apply(_grp)
    # Assegura ordenação/colunas
    cols = ["date", "team", "opponent", "gf", "ga", "is_home", "ewm_gf", "ewm_ga", "ewm_gd", "ewm_home_rate"]
    return out[cols].reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=str, required=True, help="CSV de histórico (results.csv)")
    parser.add_argument("--out", type=str, required=True, help="arquivo Parquet de saída")
    parser.add_argument("--ewma", type=float, default=0.20, help="alpha do EWMA (0<alpha<=1)")
    args = parser.parse_args()

    hist_csv = args.history
    out_parquet = args.out
    alpha = float(args.ewma)
    assert 0 < alpha <= 1.0, "--ewma deve estar em (0,1]"

    if not os.path.isfile(hist_csv):
        _log(f"{hist_csv} não encontrado")
        sys.exit(2)

    df = pd.read_csv(hist_csv)
    try:
        _validate_history(df)
    except Exception as e:
        _log(f"[CRITICAL] {e}")
        sys.exit(2)

    long_df = _long_format(df)
    feats = _ewm_features(long_df, alpha)

    # Garante diretório
    os.makedirs(os.path.dirname(out_parquet), exist_ok=True)

    # Tenta salvar via pyarrow / fastparquet (o pandas escolhe o engine)
    try:
        feats.to_parquet(out_parquet, index=False)
    except Exception as e:
        _log(
            "Falha ao salvar Parquet. Instale pyarrow ou fastparquet em requirements.txt "
            "(ex.: 'pyarrow>=15.0.0'). Erro: %s" % e
        )
        sys.exit(2)

    _log(f"OK — gerado {out_parquet} com {len(feats)} linhas")


if __name__ == "__main__":
    main()