# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import pickle
import os
import json
import numpy as np
from sklearn.ensemble import RandomForestClassifier

def _log(msg: str) -> None:
    print(f"[predict_dynamic_model] {msg}", flush=True)

def predict_matches(model, state: dict, matches_df: pd.DataFrame) -> pd.DataFrame:
    # Verificar colunas no matches_df
    home_col = next((col for col in ['team_home', 'home'] if col in matches_df.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in matches_df.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas em matches_norm.csv")
        sys.exit(8)

    _log(f"Processando {len(matches_df)} jogos do matches_norm.csv")

    # Inicializar DataFrame de predições
    results = []
    for _, row in matches_df.iterrows():
        home_team = row[home_col]
        away_team = row[away_col]

        # Inicializar valores padrão
        prob_home = 0.33
        prob_draw = 0.33
        prob_away = 0.34

        # Se o modelo estiver disponível, usar para predição
        if model is not None:
            try:
                # Criar features dummy para predição (ajustar conforme necessário)
                features = np.array([[state.get('home_strength', 1.0), state.get('away_strength', 1.0)]])
                probs = model.predict_proba(features)[0]
                if len(probs) == 3:
                    prob_home, prob_draw, prob_away = probs
            except Exception as e:
                _log(f"Erro ao prever para {home_team} x {away_team}: {e}, usando valores padrão")

        results.append({
            'team_home': home_team,
            'team_away': away_team,
            'prob_home': prob_home,
            'prob_draw': prob_draw,
            'prob_away': prob_away
        })

    df = pd.DataFrame(results)
    _log(f"Gerado DataFrame com {len(df)} predições")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--matches", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # Carregar modelo
    model = None
    if os.path.isfile(args.model):
        try:
            with open(args.model, 'rb') as f:
                model = pickle.load(f)
            _log(f"Modelo carregado de {args.model}")
        except Exception as e:
            _log(f"Erro ao carregar modelo {args.model}: {e}, prosseguindo com valores padrão")
    else:
        _log(f"Modelo {args.model} não encontrado, prosseguindo com valores padrão")

    # Carregar estado
    state = {}
    if os.path.isfile(args.state):
        try:
            with open(args.state, 'r') as f:
                state = json.load(f)
            _log(f"Estado carregado de {args.state}")
        except Exception as e:
            _log(f"Erro ao carregar estado {args.state}: {e}, prosseguindo com estado vazio")
    else:
        _log(f"Estado {args.state} não encontrado, prosseguindo com estado vazio")

    # Carregar jogos
    if not os.path.isfile(args.matches):
        _log(f"Arquivo {args.matches} não encontrado")
        sys.exit(8)

    try:
        matches_df = pd.read_csv(args.matches)
    except Exception as e:
        _log(f"Erro ao ler {args.matches}: {e}")
        sys.exit(8)

    if matches_df.empty:
        _log("Arquivo de jogos está vazio")
        sys.exit(8)

    df = predict_matches(model, state, matches_df)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    _log(f"Arquivo {args.out} gerado com {len(df)} predições")

if __name__ == "__main__":
    main()