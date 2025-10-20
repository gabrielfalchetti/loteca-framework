# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import wandb
import pickle
import json
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[train_dynamic_model] {msg}", flush=True)

def calculate_result(row):
    """Calcula result: 1=home win, 0=draw, -1=away win"""
    score_home = row['score_home']
    score_away = row['score_away']
    if score_home > score_away:
        return 1
    elif score_home < score_away:
        return -1
    else:
        return 0

def train_model(features_path: str, out_state: str, out_model: str):
    # Inicializar W&B
    wandb_api_key = os.getenv("WANDB_API_KEY")
    if wandb_api_key:
        wandb.login(key=wandb_api_key)
        wandb.init(project="loteca-model", config={"season": 2025, "ewma": 0.20, "model_type": "LogisticRegression"})
        _log("W&B inicializado com sucesso!")
    else:
        _log("WANDB_API_KEY não definida, pulando logging no W&B")

    # Carregar features (team-level)
    df_features = pd.read_parquet(features_path)
    _log(f"Features carregadas: {len(df_features)} times, colunas: {list(df_features.columns)}")

    # Carregar historical results (match-level)
    results_path = "data/history/results.csv"
    if not os.path.exists(results_path):
        _log(f"Arquivo {results_path} não encontrado")
        sys.exit(2)
    df_results = pd.read_csv(results_path)
    _log(f"Results carregados: {len(df_results)} partidas, colunas: {list(df_results.columns)}")

    # Calcular coluna 'result' se não existir
    if 'result' not in df_results.columns:
        df_results['result'] = df_results.apply(calculate_result, axis=1)
        _log("Coluna 'result' calculada com sucesso!")

    # Preparar features para treinamento (match-level)
    # Para cada partida, combinar features de home e away teams
    train_data = []
    for _, match in df_results.iterrows():
        home_team = match['team_home']
        away_team = match['team_away']
        result = match['result']
        
        # Buscar features do home team
        home_features = df_features[df_features['team'] == home_team]
        away_features = df_features[df_features['team'] == away_team]
        
        if not home_features.empty and not away_features.empty:
            # Usar a primeira linha de cada (assumindo uma por time)
            home_row = home_features.iloc[0]
            away_row = away_features.iloc[0]
            
            # Combinar features
            combined = {
                'home_avg_goals_scored': home_row['avg_goals_scored'],
                'home_avg_goals_conceded': home_row['avg_goals_conceded'],
                'away_avg_goals_scored': away_row['avg_goals_scored'],
                'away_avg_goals_conceded': away_row['avg_goals_conceded'],
                'home_sentiment': home_row.get('sentiment', 0),
                'away_sentiment': away_row.get('sentiment', 0),
                'home_injuries': home_row.get('injuries', 0),
                'away_injuries': away_row.get('injuries', 0),
                'rain_prob': home_row.get('rain_prob', 0),  # Assumir home stadium
                'temperature': home_row.get('temperature', 0),
                'result': result
            }
            train_data.append(combined)

    if not train_data:
        _log("Nenhum dado de treinamento válido encontrado")
        sys.exit(2)

    df_train = pd.DataFrame(train_data)
    _log(f"Dados de treinamento preparados: {len(df_train)} partidas")

    # Features disponíveis
    feature_cols = ['home_avg_goals_scored', 'home_avg_goals_conceded', 
                    'away_avg_goals_scored', 'away_avg_goals_conceded',
                    'home_sentiment', 'away_sentiment', 
                    'home_injuries', 'away_injuries', 
                    'rain_prob', 'temperature']
    available_features = [f for f in feature_cols if f in df_train.columns]
    
    if not available_features:
        _log("Nenhuma feature válida encontrada")
        sys.exit(2)

    X = df_train[available_features].fillna(0)
    y = df_train['result']

    # Treinar modelo
    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X, y)

    # Log métricas no W&B
    y_pred_proba = model.predict_proba(X)
    loss = log_loss(y, y_pred_proba)
    accuracy = (model.predict(X) == y).mean()
    _log(f"Log loss: {loss:.4f}, Accuracy: {accuracy:.4f}")
    
    if wandb_api_key:
        wandb.log({
            "log_loss": loss,
            "accuracy": accuracy,
            "n_samples": len(X),
            "n_features": len(available_features),
            "timestamp": datetime.now().isoformat()
        })

    # Salvar modelo
    os.makedirs(os.path.dirname(out_model), exist_ok=True)
    with open(out_model, 'wb') as f:
        pickle.dump(model, f)
    _log(f"Modelo salvo em {out_model}")

    # Salvar estado (hiperparâmetros)
    state = {
        "model_type": "LogisticRegression", 
        "max_iter": 1000, 
        "features_used": available_features,
        "n_samples": len(X),
        "log_loss": float(loss),
        "accuracy": float(accuracy)
    }
    os.makedirs(os.path.dirname(out_state), exist_ok=True)
    with open(out_state, 'w') as f:
        json.dump(state, f, indent=4)
    _log(f"Estado salvo em {out_state}")

    if wandb_api_key:
        wandb.finish()
    _log("Treinamento concluído com sucesso!")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--out_state", required=True)
    ap.add_argument("--out_model", required=True)
    args = ap.parse_args()

    train_model(args.features, args.out_state, args.out_model)

if __name__ == "__main__":
    main()