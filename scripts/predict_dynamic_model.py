# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import pickle
import os
import json

def _log(msg: str) -> None:
    print(f"[predict_dynamic_model] {msg}", flush=True)

def predict_dynamic_model(model_file, state_file, matches_file, out_csv):
    if not os.path.isfile(model_file):
        _log(f"Arquivo {model_file} não encontrado")
        sys.exit(8)

    if not os.path.isfile(state_file):
        _log(f"Arquivo {state_file} não encontrado")
        sys.exit(8)

    if not os.path.isfile(matches_file):
        _log(f"Arquivo {matches_file} não encontrado")
        sys.exit(8)

    try:
        with open(model_file, 'rb') as f:
            model = pickle.load(f)
    except Exception as e:
        _log(f"Erro ao ler {model_file}: {e}")
        sys.exit(8)

    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
    except Exception as e:
        _log(f"Erro ao ler {state_file}: {e}")
        sys.exit(8)

    try:
        matches = pd.read_csv(matches_file)
    except Exception as e:
        _log(f"Erro ao ler {matches_file}: {e}")
        sys.exit(8)

    home_col = 'team_home' if 'team_home' in matches.columns else 'home'
    away_col = 'team_away' if 'team_away' in matches.columns else 'away'

    # Carregar features para predição
    features = pd.read_parquet('data/history/features.parquet')
    predictions = []
    for _, match in matches.iterrows():
        match_id = match.get('match_id', 0)  # Preservar match_id
        home_team = match[home_col]
        away_team = match[away_col]
        home_stats = features[features['team'] == home_team]
        away_stats = features[features['team'] == away_team]
        X = pd.DataFrame({
            'avg_goals_scored_home': [home_stats['avg_goals_scored'].iloc[0] if not home_stats.empty else 1.0],
            'avg_goals_conceded_home': [home_stats['avg_goals_conceded'].iloc[0] if not home_stats.empty else 1.0],
            'avg_goals_scored_away': [away_stats['avg_goals_scored'].iloc[0] if not away_stats.empty else 1.0],
            'avg_goals_conceded_away': [away_stats['avg_goals_conceded'].iloc[0] if not away_stats.empty else 1.0],
            'sentiment_home': [home_stats['sentiment'].iloc[0] if not home_stats.empty else 0.0],
            'injuries_home': [home_stats['injuries'].iloc[0] if not home_stats.empty else 0],
            'rain_prob_home': [home_stats['rain_prob'].iloc[0] if not home_stats.empty else 0.0],
            'temperature_home': [home_stats['temperature'].iloc[0] if not home_stats.empty else 0.0],
            'sentiment_away': [away_stats['sentiment'].iloc[0] if not away_stats.empty else 0.0],
            'injuries_away': [away_stats['injuries'].iloc[0] if not away_stats.empty else 0],
            'rain_prob_away': [away_stats['rain_prob'].iloc[0] if not away_stats.empty else 0.0],
            'temperature_away': [away_stats['temperature'].iloc[0] if not away_stats.empty else 0.0]
        })
        try:
            probs = model.predict_proba(X)
            predictions.append({
                'match_id': match_id,  # Incluir match_id
                'home_team': home_team,
                'away_team': away_team,
                'home_prob': probs[0][2] if len(probs[0]) > 2 else 0.33,
                'draw_prob': probs[0][1] if len(probs[0]) > 1 else 0.33,
                'away_prob': probs[0][0] if len(probs[0]) > 0 else 0.34
            })
        except Exception as e:
            _log(f"Erro ao prever para {home_team} x {away_team}: {e}")
            predictions.append({
                'match_id': match_id,  # Incluir match_id
                'home_team': home_team,
                'away_team': away_team,
                'home_prob': 0.33,
                'draw_prob': 0.33,
                'away_prob': 0.34
            })

    df_predictions = pd.DataFrame(predictions)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df_predictions.to_csv(out_csv, index=False)
    _log(f"Predições salvas em {out_csv}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--matches", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    predict_dynamic_model(args.model, args.state, args.matches, args.out)

if __name__ == "__main__":
    main()