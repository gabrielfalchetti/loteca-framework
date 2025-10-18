# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import pickle
import os
import json
import numpy as np
from pykalman import KalmanFilter

def _log(msg: str) -> None:
    print(f"[train_dynamic] {msg}", flush=True)

def fit_states(feats: pd.DataFrame, model_type: str = 'kalman') -> dict:
    """Ajusta estados para o modelo dinâmico."""
    required_cols = ['team', 'avg_goals_scored', 'avg_goals_conceded']
    missing = [col for col in required_cols if col not in feats.columns]
    if missing:
        _log(f"features sem colunas obrigatórias: {missing}")
        sys.exit(2)

    if feats.empty:
        _log("Arquivo de features vazio — falhando.")
        sys.exit(2)

    states = {}
    for _, row in feats.iterrows():
        team = row['team']
        # Estado inicial: ataque (avg_goals_scored), defesa (avg_goals_conceded)
        states[team] = {
            'attack': row['avg_goals_scored'],
            'defense': row['avg_goals_conceded'],
            'formation': row.get('formation', 'unknown')
        }

    return states

def train_model(feats: pd.DataFrame, states: dict, model_type: str = 'kalman'):
    """Treina o modelo dinâmico."""
    if model_type == 'kalman':
        # Placeholder para modelo Kalman
        kf = KalmanFilter(
            initial_state_mean=np.array([1.0, 1.0]),  # Ataque e defesa iniciais
            initial_state_covariance=np.eye(2),
            observation_matrices=np.eye(2),
            transition_matrices=np.eye(2),
            observation_covariance=np.eye(2),
            transition_covariance=np.eye(2) * 0.1
        )
        return kf
    else:
        _log(f"Tipo de modelo {model_type} não suportado")
        sys.exit(2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True, help="Arquivo Parquet de features")
    ap.add_argument("--out_state", required=True, help="Arquivo JSON de estados")
    ap.add_argument("--out_model", required=True, help="Arquivo PKL do modelo")
    ap.add_argument("--model_type", default="kalman", choices=["kalman"], help="Tipo de modelo")
    args = ap.parse_args()

    if not os.path.isfile(args.features):
        _log(f"{args.features} não encontrado")
        sys.exit(2)

    feats = pd.read_parquet(args.features)
    states = fit_states(feats, model_type=args.model_type)
    model = train_model(feats, states, model_type=args.model_type)

    # Salvar estados
    os.makedirs(os.path.dirname(args.out_state), exist_ok=True)
    with open(args.out_state, 'w') as f:
        json.dump(states, f, ensure_ascii=False, indent=2)
    _log(f"OK — estados salvos em {args.out_state}")

    # Salvar modelo
    os.makedirs(os.path.dirname(args.out_model), exist_ok=True)
    with open(args.out_model, 'wb') as f:
        pickle.dump(model, f)
    _log(f"OK — modelo salvo em {args.out_model}")

if __name__ == "__main__":
    main()