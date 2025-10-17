#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Estima parâmetros bivariados (ex.: correlação de gols) a partir de dados históricos e partidas futuras,
usando um modelo Poisson bivariado simplificado.

Saída: CSV com cabeçalho: match_id, team_home, team_away, lambda_home, lambda_away, correlation

Uso:
  python -m scripts.bivariate_estimator --history data/history/features.parquet --matches data/out/matches_norm.csv --out data/out/bivariate.csv
"""

from __future__ import annotations

import argparse
import os
import pandas as pd
import numpy as np
from typing import Optional

def _log(msg: str) -> None:
    print(f"[bivariate_estimator] {msg}", flush=True)

def estimate_bivariate(history: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    """
    Estima lambdas (taxas de gols) e correlação bivariada para cada partida.
    Usa histórico para calcular médias ponderadas e matches para contextos futuros.
    """
    # Verificar colunas mínimas
    required_cols = ["team", "gf", "ga"]
    if not all(col in history.columns for col in required_cols):
        raise ValueError(f"history sem colunas obrigatórias: {required_cols}")

    # Agregar histórico por time
    hist_agg = history.groupby("team").agg({"gf": "mean", "ga": "mean"}).rename(columns={"gf": "avg_gf", "ga": "avg_ga"})

    # Preparar saída
    results = []
    for _, match in matches.iterrows():
        home_team = match["team_home"]
        away_team = match["team_away"]
        match_id = match.get("match_id", f"{home_team}_vs_{away_team}")

        # Buscar médias do histórico
        home_stats = hist_agg.loc[home_team] if home_team in hist_agg.index else {"avg_gf": 1.0, "avg_ga": 1.0}
        away_stats = hist_agg.loc[away_team] if away_team in hist_agg.index else {"avg_gf": 1.0, "avg_ga": 1.0}

        # Estimar lambdas (ajustado por home advantage básico)
        lambda_home = home_stats["avg_gf"] * away_stats["avg_ga"] * 1.15  # home_adv simplificado
        lambda_away = away_stats["avg_gf"] * home_stats["avg_ga"]

        # Correlação bivariada simplificada (placeholder; estimar via Dixon-Coles seria ideal)
        correlation = -0.1  # Valor típico para dependência negativa

        results.append([match_id, home_team, away_team, lambda_home, lambda_away, correlation])

    return pd.DataFrame(results, columns=["match_id", "team_home", "team_away", "lambda_home", "lambda_away", "correlation"])

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=str, required=True, help="Parquet de histórico de features")
    parser.add_argument("--matches", type=str, required=True, help="CSV de partidas normais")
    parser.add_argument("--out", type=str, required=True, help="CSV de saída com estimativas bivariadas")
    args = parser.parse_args()

    if not os.path.isfile(args.history):
        _log(f"{args.history} não encontrado")
        sys.exit(7)
    if not os.path.isfile(args.matches):
        _log(f"{args.matches} não encontrado")
        sys.exit(7)

    try:
        history = pd.read_parquet(args.history)
        matches = pd.read_csv(args.matches)
        result_df = estimate_bivariate(history, matches)

        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        result_df.to_csv(args.out, index=False)
        _log(f"OK — gerado {args.out} com {len(result_df)} estimativas")
    except Exception as e:
        _log(f"[CRITICAL] Erro: {e}")
        sys.exit(7)

if __name__ == "__main__":
    main()