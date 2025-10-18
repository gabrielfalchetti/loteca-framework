# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import json
import os
import numpy as np

def _log(msg: str) -> None:
    print(f"[features] {msg}", flush=True)

def calculate_features(history_df: pd.DataFrame, tactics_file: str, ewma_alpha: float) -> pd.DataFrame:
    """Gera features a partir de dados históricos e táticas."""
    required_cols = ['match_id', 'team_home', 'team_away', 'score_home', 'score_away']
    missing = [col for col in required_cols if col not in history_df.columns]
    if missing:
        _log(f"Colunas obrigatórias ausentes em history: {missing}")
        sys.exit(2)

    if history_df.empty:
        _log("Arquivo de histórico vazio — falhando.")
        sys.exit(2)

    # Carregar táticas
    if not os.path.isfile(tactics_file):
        _log(f"{tactics_file} não encontrado")
        sys.exit(2)
    
    with open(tactics_file, 'r') as f:
        tactics = json.load(f)
    if not tactics:
        _log("Arquivo de táticas vazio — falhando.")
        sys.exit(2)

    # Renomear colunas para consistência
    df = history_df.rename(columns={
        'team_home': 'home',
        'team_away': 'away',
        'score_home': 'home_goals',
        'score_away': 'away_goals'
    })

    # Adicionar colunas opcionais se ausentes
    for col in ['xG_home', 'xG_away', 'formation_home', 'formation_away']:
        if col not in df.columns:
            df[col] = np.nan

    # Calcular features simples (exemplo: média de gols, forma recente)
    teams = set(df['home']).union(set(df['away']))
    features = []
    for team in teams:
        team_matches = df[(df['home'] == team) | (df['away'] == team)]
        if team_matches.empty:
            continue

        # Forma recente (EWMA de gols)
        home_matches = team_matches[team_matches['home'] == team]
        away_matches = team_matches[team_matches['away'] == team]
        goals_scored = pd.concat([
            home_matches[['home_goals']].rename(columns={'home_goals': 'goals'}),
            away_matches[['away_goals']].rename(columns={'away_goals': 'goals'})
        ]).sort_index()
        goals_conceded = pd.concat([
            home_matches[['away_goals']].rename(columns={'away_goals': 'goals'}),
            away_matches[['home_goals']].rename(columns={'home_goals': 'goals'})
        ]).sort_index()

        avg_goals_scored = goals_scored['goals'].ewm(alpha=ewma_alpha).mean().iloc[-1] if not goals_scored.empty else 0
        avg_goals_conceded = goals_conceded['goals'].ewm(alpha=ewma_alpha).mean().iloc[-1] if not goals_conceded.empty else 0

        # Táticas (exemplo: formação mais comum)
        formation = tactics.get(team, {}).get('formation', 'unknown')

        features.append({
            'team': team,
            'avg_goals_scored': avg_goals_scored,
            'avg_goals_conceded': avg_goals_conceded,
            'formation': formation
        })

    features_df = pd.DataFrame(features)
    if features_df.empty:
        _log("Nenhuma feature gerada — falhando.")
        sys.exit(2)

    return features_df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="Arquivo CSV de histórico")
    ap.add_argument("--tactics", required=True, help="Arquivo JSON de táticas")
    ap.add_argument("--out", required=True, help="Arquivo Parquet de saída")
    ap.add_argument("--ewma", type=float, default=0.2, help="Alpha para EWMA")
    args = ap.parse_args()

    if not os.path.isfile(args.history):
        _log(f"{args.history} não encontrado")
        sys.exit(2)

    history_df = pd.read_csv(args.history)
    features_df = calculate_features(history_df, args.tactics, args.ewma)
    
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    features_df.to_parquet(args.out, index=False)
    _log(f"OK — gerado {args.out} com {len(features_df)} linhas")

if __name__ == "__main__":
    main()