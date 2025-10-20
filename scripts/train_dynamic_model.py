# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import wandb
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[train_dynamic_model] {msg}", flush=True)

def train_model(features_path: str, out_state: str, out_model: str):
    # Inicializar W&B
    wandb_api_key = os.getenv("WANDB_API_KEY")
    if not wandb_api_key:
        _log("WANDB_API_KEY não definida, pulando logging no W&B")
    else:
        wandb.login(key=wandb_api_key)
        wandb.init(project="loteca-model", config={"season": 2025, "ewma": 0.20, "model_type": "LogisticRegression"})

    # Carregar features
    df = pd.read_parquet(features_path)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    # Features e target (ajuste conforme suas colunas)
    features = ['xG_home', 'xG_away', 'lesions_home', 'lesions_away', 'home_sentiment', 'away_sentiment', 'home_injuries', 'away_injuries', 'rain_prob', 'temperature']
    available_features = [f for f in features if f in df.columns]
    if not available_features:
        _log("Nenhuma feature válida encontrada")
        sys.exit(2)

    X = df[available_features].fillna(0)
    y = df['result'] if 'result' in df.columns else None  # 1 (home win), 0 (draw), -1 (away win)

    if y is None:
        _log("Coluna 'result' não encontrada nos dados")
        sys.exit(2)

    # Treinar modelo
    model = LogisticRegression(max_iter=1000)
    model.fit(X, y)

    # Log métricas no W&B
    y_pred_proba = model.predict_proba(X)
    loss = log_loss(y, y_pred_proba)
    _log(f"Log loss: {loss}")
    if wandb_api_key:
        wandb.log({"log_loss": loss, "timestamp": datetime.now().isoformat()})

    # Salvar modelo
    os.makedirs(os.path.dirname(out_model), exist_ok=True)
    with open(out_model, 'wb') as f:
        pickle.dump(model, f)
    _log(f"Modelo salvo em {out_model}")

    # Salvar estado (hiperparâmetros)
    state = {"model_type": "LogisticRegression", "max_iter": 1000, "features_used": available_features}
    os.makedirs(os.path.dirname(out_state), exist_ok=True)
    with open(out_state, 'w') as f:
        json.dump(state, f, indent=4)
    _log(f"Estado salvo em {out_state}")

    if wandb_api_key:
        wandb.finish()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--out_state", required=True)
    ap.add_argument("--out_model", required=True)
    args = ap.parse_args()

    train_model(args.features, args.out_state, args.out_model)

if __name__ == "__main__":
    main()