#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera features a partir de um CSV de resultados no formato:

date,home,away,home_goals,away_goals,xG_home,xG_away,formation_home,formation_away

Saída: Parquet (requer pyarrow ou fastparquet), com features EWMA por time, VAEP, impacto de lesões e táticas.
Mantém comportamento "fail-safe": lança erro claro se o CSV não tem conteúdo válido.

Uso:
  python -m scripts.feature_engineer \
      --history data/history/results.csv \
      --tactics data/history/tactics.json \
      --out data/history/features.parquet \
      --ewma 0.20
"""

from __future__ import annotations

import argparse
import sys
import os
import json
import pandas as pd
import numpy as np
from pandas import DataFrame
from typing import Optional

def _log(msg: str) -> None:
    print(f"[features] {msg}", flush=True)

def _validate_history(df: DataFrame) -> None:
    required = ["date", "home", "away", "home_goals", "away_goals", "xG_home", "xG_away", "formation_home", "formation_away"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"history sem colunas requeridas: {missing}")

    if len(df) == 0:
        raise ValueError("history vazio")

    # Tipos mínimos
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isna().all():
        raise ValueError("coluna 'date' inválida (nenhum parse possível)")

    for c in ["home_goals", "away_goals", "xG_home", "xG_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if df[["home_goals", "away_goals"]].isna().any().any():
        df["home_goals"] = df["home_goals"].fillna(0)
        df["away_goals"] = df["away_goals"].fillna(0)
    if df[["xG_home", "xG_away"]].isna().any().any():
        df["xG_home"] = df["xG_home"].fillna(0)
        df["xG_away"] = df["xG_away"].fillna(0)

    # Checar duplicatas/outliers
    if df.duplicated(subset=["date", "home", "away"]).any():
        _log("Duplicatas detectadas, removendo...")
        df = df.drop_duplicates(subset=["date", "home", "away"])
    if (df[["home_goals", "away_goals"]] > 10).any().any():
        _log("Outliers (gols > 10) detectados, ajustando para 10...")
        df[["home_goals", "away_goals"]] = df[["home_goals", "away_goals"]].clip(upper=10)

def _long_format(df: DataFrame) -> DataFrame:
    """Duplica linhas, uma por time (home/away), para calcular EWM por time."""
    home = df[["date", "home", "away", "home_goals", "away_goals", "xG_home", "xG_away", "formation_home", "formation_away"]].copy()
    home.columns = ["date", "team", "opponent", "gf", "ga", "xG", "xG_opponent", "formation", "formation_opponent"]
    home["is_home"] = 1

    away = df[["date", "home", "away", "home_goals", "away_goals", "xG_home", "xG_away", "formation_home", "formation_away"]].copy()
    away.columns = ["date", "opponent", "team", "ga", "gf", "xG_opponent", "xG", "formation_opponent", "formation"]
    away["is_home"] = 0

    long_df = pd.concat([home, away], ignore_index=True)
    long_df.sort_values(["team", "date"], inplace=True)
    return long_df

def _calculate_vaep(df: DataFrame) -> DataFrame:
    """Calcula VAEP básico (placeholder para modelo completo)."""
    df['vaep'] = df['xG'] - df['xG_opponent'].shift(1).fillna(0)  # Diferença esperada simplificada
    return df

def _ewm_features(long_df: DataFrame, alpha: float, tactics: Optional[Dict] = None) -> DataFrame:
    """Calcula EWM por time para métricas básicas e integra táticas."""
    def _grp(g: DataFrame) -> DataFrame:
        g = g.sort_values("date").copy()
        g["ewm_gf"] = g["gf"].ewm(alpha=alpha, adjust=False).mean()
        g["ewm_ga"] = g["ga"].ewm(alpha=alpha, adjust=False).mean()
        g["ewm_gd"] = (g["gf"] - g["ga"]).ewm(alpha=alpha, adjust=False).mean()
        g["ewm_xG"] = g["xG"].ewm(alpha=alpha, adjust=False).mean()
        g["ewm_xGA"] = g["xG_opponent"].ewm(alpha=alpha, adjust=False).mean()
        g["ewm_home_rate"] = g["is_home"].ewm(alpha=alpha, adjust=False).mean()
        # Impacto de lesões (placeholder: simula redução de forma)
        g["injury_impact"] = np.where(g["formation"] == "", 0, 0.1)  # Ajustar com dados reais
        # Táticas (se disponível)
        if tactics:
            for date, team_data in tactics.items():
                mask = (g["date"] == date) & (g["team"] == team_data["team"])
                if mask.any():
                    g.loc[mask, "tactic_score"] = team_data.get("tactic_score", 0)
        return g

    out = long_df.groupby("team", group_keys=False).apply(_grp)
    cols = ["date", "team", "opponent", "gf", "ga", "is_home", "ewm_gf", "ewm_ga", "ewm_gd", 
            "ewm_xG", "ewm_xGA", "ewm_home_rate", "injury_impact", "tactic_score", "vaep", "formation", "formation_opponent"]
    return out[cols].reset_index(drop=True)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=str, required=True, help="CSV de histórico (results.csv)")
    parser.add_argument("--tactics", type=str, default=None, help="JSON com táticas (tactics.json)")
    parser.add_argument("--out", type=str, required=True, help="arquivo Parquet de saída")
    parser.add_argument("--ewma", type=float, default=0.20, help="alpha do EWMA (0<alpha<=1)")
    args = parser.parse_args()

    hist_csv = args.history
    tactics_json = args.tactics
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

    tactics = None
    if tactics_json and os.path.isfile(tactics_json):
        with open(tactics_json, "r", encoding="utf-8") as f:
            tactics = json.load(f)

    long_df = _long_format(df)
    long_df = _calculate_vaep(long_df)  # Adiciona VAEP básico
    feats = _ewm_features(long_df, alpha, tactics)

    # Garante diretório
    os.makedirs(os.path.dirname(out_parquet), exist_ok=True)

    # Tenta salvar via pyarrow / fastparquet
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