# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import wandb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss
import pickle

def _log(msg: str) -> None:
    print(f"[train_dynamic_model] {msg}", flush=True)

def train_model(features_path: str, out_state: str, out_model: str):
    # Initialize W&B
    wandb.login(key=os.getenv("WANDB_API_KEY"))
    wandb.init(project="loteca-model", config={"season": 2025, "ewma": 0.20})

    # Load features
    df = pd.read_parquet(features_path)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    # Example features and target (adjust as per your data)
    X = df[['xG_home', 'xG_away', 'lesions_home', 'lesions_away']].fillna(0)
    y = df['result'] if 'result' in df.columns else None  # Assume 'result' is 1 (home win), 0 (draw), -1 (away win)

    if y is None:
        _log("Coluna 'result' não encontrada nos dados")
        sys.exit(2)

    # Train model
    model = LogisticRegression(max_iter=1000)
    model.fit(X, y)

    # Log metrics to W&B
    y_pred_proba = model.predict_proba(X)
    loss = log_loss(y, y_pred_proba)
    wandb.log({"log_loss": loss})

    # Save model
    with open(out_model, 'wb') as f:
        pickle.dump(model, f)
    _log(f"Modelo salvo em {out_model}")

    # Save state (example: hyperparameters)
    state = {"model_type": "LogisticRegression", "max_iter": 1000}
    with open(out_state, 'w') as f:
        json.dump(state, f)
    _log(f"Estado salvo em {out_state}")

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