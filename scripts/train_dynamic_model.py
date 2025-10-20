# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import wandb
import pickle
import json
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, accuracy_score
from sklearn.model_selection import train_test_split
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[train_dynamic_model] {msg}", flush=True)

def train_model(features_path: str, results_path: str, out_state: str, out_model: str):
    # Inicializar W&B
    wandb_api_key = os.getenv("WANDB_API_KEY")
    if wandb_api_key:
        wandb.login(key=wandb_api_key)
        wandb.init(project="loteca-model", config={"season": 2025, "ewma": 0.20, "model_type": "LogisticRegression"})
        _log("W&B inicializado com sucesso!")

    # 1. Carregar features team-level
    df_features = pd.read_parquet(features_path)
    _log(f"Features carregadas: {len(df_features)} times, colunas: {list(df_features.columns)}")

    # 2. Carregar results match-level
    df_results = pd.read_csv(results_path)
    _log(f"Results carregados: {len(df_results)} partidas, colunas: {list(df_results.columns)}")

    # 3. Preparar dados para merge (assumindo colunas home_team, away_team no results.csv)
    home_col = 'team_home' if 'team_home' in df_results.columns else 'home'
    away_col = 'team_away' if 'team_away' in df_results.columns else 'away'
    result_col = 'result' if 'result' in df_results.columns else None

    if result_col is None:
        _log(f"Coluna 'result' não encontrada. Colunas disponíveis: {list(df_results.columns)}")
        sys.exit(2)

    # 4. Merge: para cada partida, pegar features dos dois times
    training_data = []
    for _, match in df_results.iterrows():
        home_team = match[home_col]
        away_team = match[away_col]
        result = match[result_col]

        # Features home team
        home_features = df_features[df_features['team'] == home_team]
        if not home_features.empty:
            home_row = home_features.iloc[0]
            # Prefix home_
            home_features_dict = {f"home_{k}": v for k, v in home_row.items() if k != 'team'}
            home_features_dict['result'] = result
            training_data.append(home_features_dict)

    if not training_data:
        _log("Nenhum dado de treinamento gerado após merge")
        sys.exit(2)

    df_training = pd.DataFrame(training_data)
    _log(f"Dados de treinamento: {len(df_training)} partidas")

    # 5. Features para o modelo
    feature_cols = [col for col in df_training.columns if col.startswith('home_') and col != 'home_result']
    X = df_training[feature_cols].fillna(0)
    y = df_training['result']

    # 6. Treinar modelo
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)

    # 7. Métricas
    y_train_pred = model.predict_proba(X_train)
    y_test_pred = model.predict_proba(X_test)
    train_loss = log_loss(y_train, y_train_pred)
    test_loss = log_loss(y_test, y_test_pred)
    train_acc = accuracy_score(y_train, model.predict(X_train))
    test_acc = accuracy_score(y_test, model.predict(X_test))

    _log(f"Train Loss: {train_loss:.4f}, Test Loss: {test_loss:.4f}")
    _log(f"Train Accuracy: {train_acc:.4f}, Test Accuracy: {test_acc:.4f}")

    # 8. Log no W&B
    if wandb_api_key:
        wandb.log({
            "train_log_loss": train_loss,
            "test_log_loss": test_loss,
            "train_accuracy": train_acc,
            "test_accuracy": test_acc,
            "n_samples": len(df_training),
            "n_features": len(feature_cols),
            "timestamp": datetime.now().isoformat()
        })

    # 9. Salvar modelo
    os.makedirs(os.path.dirname(out_model), exist_ok=True)
    with open(out_model, 'wb') as f:
        pickle.dump(model, f)
    _log(f"Modelo salvo em {out_model}")

    # 10. Salvar estado
    state = {
        "model_type": "LogisticRegression",
        "max_iter": 1000,
        "features_used": feature_cols,
        "n_samples": len(df_training),
        "train_loss": train_loss,
        "test_loss": test_loss,
        "train_acc": train_acc,
        "test_acc": test_acc
    }
    os.makedirs(os.path.dirname(out_state), exist_ok=True)
    with open(out_state, 'w') as f:
        json.dump(state, f, indent=4)
    _log(f"Estado salvo em {out_state}")

    if wandb_api_key:
        wandb.finish()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--results", default="data/history/results.csv")
    ap.add_argument("--out_state", required=True)
    ap.add_argument("--out_model", required=True)
    args = ap.parse_args()

    train_model(args.features, args.results, args.out_state, args.out_model)

if __name__ == "__main__":
    main()