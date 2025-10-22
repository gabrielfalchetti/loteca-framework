# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import json
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[features] {msg}", flush=True)

def feature_engineer(history_csv, tactics_json, out_parquet, ewma):
    if not os.path.isfile(history_csv):
        _log(f"Arquivo {history_csv} não encontrado")
        sys.exit(12)

    try:
        history = pd.read_csv(history_csv)
    except Exception as e:
        _log(f"Erro ao ler {history_csv}: {e}")
        sys.exit(12)

    if history.empty:
        _log("Arquivo de histórico vazio")
        sys.exit(12)

    if not os.path.isfile(tactics_json):
        _log(f"Arquivo {tactics_json} não encontrado, usando táticas padrão")
        tactics = {}
    else:
        try:
            with open(tactics_json, 'r') as f:
                tactics = json.load(f)
        except Exception as e:
            _log(f"Erro ao ler {tactics_json}: {e}, usando táticas padrão")
            tactics = {}

    # Verificar colunas necessárias no history
    required_cols = ['team_home', 'team_away', 'score_home', 'score_away']
    missing_cols = [col for col in required_cols if col not in history.columns]
    if missing_cols:
        _log(f"Colunas ausentes no history.csv: {missing_cols}")
        sys.exit(12)

    # Inicializar DataFrame de features
    teams = pd.concat([history['team_home'], history['team_away']]).unique()
    features = pd.DataFrame({'team': teams})
    features['avg_goals_scored'] = history.groupby('team_home')['score_home'].mean().reindex(teams).fillna(0.0)
    features['avg_goals_conceded'] = history.groupby('team_home')['score_away'].mean().reindex(teams).fillna(0.0)
    features['formation'] = [tactics.get(team, "4-3-3") for team in teams]
    features['sentiment'] = 0.0  # Default para enriquecimento posterior
    features['injuries'] = 0  # Default para enriquecimento posterior
    features['rain_prob'] = 0.0  # Default para enriquecimento posterior
    features['temperature'] = 0.0  # Default para enriquecimento posterior

    # Aplicar EWMA (média móvel exponencial) se necessário
    if ewma > 0:
        features['avg_goals_scored'] = features['avg_goals_scored'].ewm(alpha=ewma).mean()
        features['avg_goals_conceded'] = features['avg_goals_conceded'].ewm(alpha=ewma).mean()

    os.makedirs(os.path.dirname(out_parquet), exist_ok=True)
    features.to_parquet(out_parquet, index=False)
    _log(f"OK — gerado {out_parquet} com {len(features)} linhas")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True)
    ap.add_argument("--tactics", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ewma", type=float, default=0.20)
    args = ap.parse_args()

    feature_engineer(args.history, args.tactics, args.out, args.ewma)

if __name__ == "__main__":
    main()