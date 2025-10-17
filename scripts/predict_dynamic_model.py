#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Aplica um modelo dinâmico treinado (Poisson Bivariado com Kalman) para prever probabilidades
de resultados de futebol a partir de estados salvos.

Saída: CSV com cabeçalho: match_id, team_home, team_away, p_home, p_draw, p_away

Uso:
  python -m scripts.predict_dynamic_model --model data/out/dynamic_model.pkl --state data/out/state_params.json --matches data/out/matches_norm.csv --out data/out/predictions.csv
"""

from __future__ import annotations

import argparse
import os
import json
import pandas as pd
import numpy as np
from typing import Dict, List

def _log(msg: str) -> None:
    print(f"[predict_dynamic] {msg}", flush=True)

def predict_dynamic_model(model_path: str, state_path: str, matches_path: str, out_path: str) -> None:
    """Preve probabilidades usando Poisson Bivariado com estados dinâmicos."""
    if not all(os.path.isfile(p) for p in [model_path, state_path, matches_path]):
        _log(f"Arquivos ausentes: {', '.join(p for p in [model_path, state_path, matches_path] if not os.path.isfile(p))}")
        return

    try:
        # Carregar estados
        with open(state_path, "r", encoding="utf-8") as f:
            states = json.load(f)
        model_data = joblib.load(model_path)
        model_type = model_data.get("model_type", "poisson")

        # Carregar partidas
        df_matches = pd.read_csv(matches_path)
        if not all(col in df_matches.columns for col in ["match_id", "team_home", "team_away"]):
            raise ValueError("matches sem colunas esperadas")

        # Calcular probabilidades
        def poisson_prob(lam: float, k: int) -> float:
            return (np.exp(-lam) * lam ** k) / np.math.factorial(k)

        predictions = []
        for _, match in df_matches.iterrows():
            home_team = match["team_home"]
            away_team = match["team_away"]
            match_id = match["match_id"]

            if home_team not in states or away_team not in states:
                _log(f"Times não encontrados: {home_team}, {away_team}")
                continue

            home_state = states[home_team]
            away_state = states[away_team]
            lambda_home = (home_state["attack"] * away_state["defense"] * (1 + home_state["home_adv"])) * home_state["xG_weight"]
            lambda_away = (away_state["attack"] * home_state["defense"]) * away_state["xG_weight"]

            # Dixon-Coles (simplificado)
            gamma = home_state["gamma"]
            max_goals = 10
            p_home = 0.0
            p_draw = 0.0
            p_away = 0.0
            for gh in range(max_goals + 1):
                for ga in range(max_goals + 1):
                    p = poisson_prob(lambda_home, gh) * poisson_prob(lambda_away, ga)
                    if gh > ga:
                        p_home += p * (1 + gamma if gh == ga + 1 else 1)
                    elif gh < ga:
                        p_away += p * (1 + gamma if gh + 1 == ga else 1)
                    else:
                        p_draw += p * (1 + gamma if gh == ga else 1)

            total = p_home + p_draw + p_away
            if total > 0:
                p_home, p_draw, p_away = p_home / total, p_draw / total, p_away / total

            predictions.append([match_id, home_team, away_team, p_home, p_draw, p_away])

        # Salvar resultados
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["match_id", "team_home", "team_away", "p_home", "p_draw", "p_away"])
            w.writerows(predictions)
        _log(f"OK — geradas {len(predictions)} previsões em {out_path}")

    except Exception as e:
        _log(f"[CRITICAL] Erro: {e}")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["match_id", "team_home", "team_away", "p_home", "p_draw", "p_away"])

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, required=True, help="Caminho do modelo pickle")
    parser.add_argument("--state", type=str, required=True, help="Caminho do JSON de estados")
    parser.add_argument("--matches", type=str, required=True, help="Caminho do CSV de partidas")
    parser.add_argument("--out", type=str, required=True, help="Caminho do CSV de previsões")
    args = parser.parse_args()
    predict_dynamic_model(args.model, args.state, args.matches, args.out)

if __name__ == "__main__":
    import joblib
    main()