# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import numpy as np
import os
from datetime import datetime
from scipy.stats import poisson

def _log(msg: str) -> None:
    print(f"[bivariate_estimator] {msg}", flush=True)

def estimate_bivariate(history_df: pd.DataFrame, matches_df: pd.DataFrame) -> pd.DataFrame:
    # Verificar colunas no history_df
    required_cols = ['team', 'avg_goals_scored', 'avg_goals_conceded']
    missing_cols = [col for col in required_cols if col not in history_df.columns]
    if missing_cols:
        _log(f"Aviso: Colunas ausentes no history: {missing_cols}. Usando valores padrão.")
        history_df = pd.DataFrame(columns=required_cols)

    # Verificar colunas no matches_df
    home_col = next((col for col in ['team_home', 'home'] if col in matches_df.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in matches_df.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas em matches_norm.csv")
        sys.exit(7)

    _log(f"Processando {len(matches_df)} jogos do matches_norm.csv")
    _log(f"Colunas no history: {list(history_df.columns)}")
    _log(f"Linhas no history: {len(history_df)}")

    results = []
    for _, row in matches_df.iterrows():
        home_team = row[home_col]
        away_team = row[away_col]

        # Inicializar valores padrão
        home_goals_lambda = 1.0
        away_goals_lambda = 1.0
        prob_home = 0.33
        prob_draw = 0.33
        prob_away = 0.34

        if not history_df.empty and 'team' in history_df.columns and 'avg_goals_scored' in history_df.columns and 'avg_goals_conceded' in history_df.columns:
            home_history = history_df[history_df['team'] == home_team]
            if not home_history.empty:
                home_goals_lambda = home_history['avg_goals_scored'].mean() if home_history['avg_goals_scored'].notnull().any() else 1.0
                away_goals_lambda = home_history['avg_goals_conceded'].mean() if home_history['avg_goals_conceded'].notnull().any() else 1.0
            else:
                _log(f"[WARNING] Time não encontrado no history: {home_team}")

            away_history = history_df[history_df['team'] == away_team]
            if not away_history.empty:
                away_goals_lambda = away_history['avg_goals_scored'].mean() if away_history['avg_goals_scored'].notnull().any() else 1.0
                home_goals_lambda = away_history['avg_goals_conceded'].mean() if away_history['avg_goals_conceded'].notnull().any() else 1.0
            else:
                _log(f"[WARNING] Time não encontrado no history: {away_team}")

            # Simples modelo Poisson para probabilidades
            max_goals = 5
            prob_matrix = np.zeros((max_goals + 1, max_goals + 1))
            for i in range(max_goals + 1):
                for j in range(max_goals + 1):
                    prob_matrix[i, j] = poisson.pmf(i, home_goals_lambda) * poisson.pmf(j, away_goals_lambda)
            
            prob_home = sum(prob_matrix[i, j] for i in range(max_goals + 1) for j in range(max_goals + 1) if i > j)
            prob_draw = sum(prob_matrix[i, i] for i in range(max_goals + 1))
            prob_away = sum(prob_matrix[i, j] for i in range(max_goals + 1) for j in range(max_goals + 1) if i < j)

        results.append({
            'team_home': home_team,
            'team_away': away_team,
            'prob_home': prob_home,
            'prob_draw': prob_draw,
            'prob_away': prob_away
        })

    df = pd.DataFrame(results)
    _log(f"Gerado DataFrame com {len(df)} jogos estimados")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True)
    ap.add_argument("--matches", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if not os.path.isfile(args.history):
        _log(f"Arquivo de histórico {args.history} não encontrado, inicializando DataFrame vazio")
        history_df = pd.DataFrame(columns=['team', 'avg_goals_scored', 'avg_goals_conceded'])
    else:
        try:
            history_df = pd.read_parquet(args.history)
        except Exception as e:
            _log(f"Erro ao ler {args.history}: {e}, inicializando DataFrame vazio")
            history_df = pd.DataFrame(columns=['team', 'avg_goals_scored', 'avg_goals_conceded'])

    if not os.path.isfile(args.matches):
        _log(f"Arquivo {args.matches} não encontrado")
        sys.exit(7)

    try:
        matches_df = pd.read_csv(args.matches)
    except Exception as e:
        _log(f"Erro ao ler {args.matches}: {e}")
        sys.exit(7)

    if matches_df.empty:
        _log("Arquivo de jogos está vazio")
        sys.exit(7)

    df = estimate_bivariate(history_df, matches_df)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    _log(f"Arquivo {args.out} gerado com {len(df)} jogos")

if __name__ == "__main__":
    main()