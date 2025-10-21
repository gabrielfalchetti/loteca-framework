# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import numpy as np
from scipy.stats import poisson
import os

def _log(msg: str, level: str = "INFO") -> None:
    print(f"[bivariate_estimator] [{level.upper()}] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="Path to history features file (features.parquet)")
    ap.add_argument("--matches", required=True, help="Path to matches_norm.csv")
    ap.add_argument("--out", required=True, help="Path to output bivariate.csv")
    args = ap.parse_args()

    # Carregar history (features.parquet)
    try:
        history_df = pd.read_parquet(args.history)
    except Exception as e:
        _log(f"Erro ao ler {args.history}: {e}", "CRITICAL")
        sys.exit(7)

    _log(f"Colunas disponíveis no history: {list(history_df.columns)}")

    # Colunas obrigatórias ajustadas com base nos dados reais
    required_cols = ['team', 'avg_goals_scored', 'avg_goals_conceded']
    missing_cols = [col for col in required_cols if col not in history_df.columns]
    if missing_cols:
        _log(f"Erro: history sem colunas obrigatórias: {missing_cols}", "CRITICAL")
        sys.exit(7)

    # Carregar matches (matches_norm.csv)
    try:
        matches_df = pd.read_csv(args.matches)
    except Exception as e:
        _log(f"Erro ao ler {args.matches}: {e}", "CRITICAL")
        sys.exit(7)

    home_col = 'team_home' if 'team_home' in matches_df.columns else 'home'
    away_col = 'team_away' if 'team_away' in matches_df.columns else 'away'
    if home_col not in matches_df.columns or away_col not in matches_df.columns:
        _log("Colunas team_home/team_away ou home/away não encontradas em matches_norm.csv", "CRITICAL")
        sys.exit(7)

    _log(f"Processando {len(matches_df)} jogos do matches_norm.csv")

    bivariate = []
    for _, row in matches_df.iterrows():
        home_team = row[home_col]
        away_team = row[away_col]

        # Obter lambdas para home (ataque home vs defesa away)
        home_stats = history_df[history_df['team'] == home_team]
        away_stats = history_df[history_df['team'] == away_team]

        if home_stats.empty or away_stats.empty:
            _log(f"Time não encontrado no history: {home_team} ou {away_team}", "WARNING")
            continue

        lambda_home = home_stats['avg_goals_scored'].values[0] * away_stats['avg_goals_conceded'].values[0]
        lambda_away = away_stats['avg_goals_scored'].values[0] * home_stats['avg_goals_conceded'].values[0]

        # Estimar probabilidades bivariadas (Poisson para gols home vs away)
        max_goals = 5  # Limitar para eficiência
        probs = np.zeros((max_goals+1, max_goals+1))
        for h in range(max_goals+1):
            for a in range(max_goals+1):
                probs[h, a] = poisson.pmf(h, lambda_home) * poisson.pmf(a, lambda_away)

        # Calcular probabilidades agregadas: P(home win), P(draw), P(away win)
        p_home_win = np.sum(np.tril(probs, -1))
        p_draw = np.sum(np.diag(probs))
        p_away_win = np.sum(np.triu(probs, 1))

        bivariate.append({
            'team_home': home_team,
            'team_away': away_team,
            'p_home_win': p_home_win,
            'p_draw': p_draw,
            'p_away_win': p_away_win
        })

    df = pd.DataFrame(bivariate)
    if df.empty:
        _log("Nenhum jogo estimado, criando CSV vazio", "WARNING")
    else:
        _log(f"Gerado bivariate.csv com {len(df)} estimativas")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)

if __name__ == "__main__":
    main()