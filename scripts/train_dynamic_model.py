# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import pickle
import os
import json
import wandb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, accuracy_score

def _log(msg: str) -> None:
    print(f"[train_dynamic_model] {msg}", flush=True)

def train_dynamic_model(features_parquet, out_state, out_model):
    features = pd.read_parquet(features_parquet)
    history = pd.read_csv('data/history/results.csv')  # Assumindo results.csv
    history['result'] = history.apply(lambda x: 0 if x['score_home'] > x['score_away'] else 1 if x['score_home'] == x['score_away'] else 2, axis=1)
    merged = pd.merge(history, features, left_on='team_home', right_on='team', suffixes=('_home', ''))
    merged = pd.merge(merged, features, left_on='team_away', right_on='team', suffixes=('', '_away'))
    X = merged[['avg_goals_scored', 'avg_goals_conceded', 'formation', 'sentiment', 'injuries', 'rain_prob', 'temperature', 'avg_goals_scored_away', 'avg_goals_conceded_away', 'formation_away']]
    y = merged['result']

    wandb.init(project="loteca-model")
    model = LogisticRegression(multi_class='multinomial', solver='lbfgs')
    model.fit(X, y)
    y_pred = model.predict_proba(X)
    logloss = log_loss(y, y_pred)
    acc = accuracy_score(y, model.predict(X))
    wandb.log({"log_loss": logloss, "accuracy": acc, "n_samples": len(X), "n_features": X.shape[1]})
    _log(f"Log loss: {logloss:.4f}, Accuracy: {acc:.4f}")
    with open(out_model, 'wb') as f:
        pickle.dump(model, f)
    with open(out_state, 'w') as f:
        json.dump({"features": list(X.columns), "timestamp": datetime.now().isoformat()}, f)
    wandb.finish()
    _log("Treinamento concluído com sucesso!")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--out_state", required=True)
    ap.add_argument("--out_model", required=True)
    args = ap.parse_args()

    train_dynamic_model(args.features, args.out_state, args.out_model)

if __name__ == "__main__":
    main()