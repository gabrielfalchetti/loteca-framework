# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import os
import json
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[update_history] {msg}", flush=True)

def update_history(source_csv, results_csv, tactics_json):
    # Ler matches_source.csv
    try:
        matches = pd.read_csv(source_csv)
    except Exception as e:
        _log(f"Erro ao ler {source_csv}: {e}")
        return

    # Definir colunas esperadas
    home_col = 'team_home' if 'team_home' in matches.columns else 'home'
    away_col = 'team_away' if 'team_away' in matches.columns else 'away'
    score_home_col = 'score_home' if 'score_home' in matches.columns else None
    score_away_col = 'score_away' if 'score_away' in matches.columns else None

    # Selecionar colunas relevantes
    results_data = matches[[home_col, away_col]]
    if score_home_col and score_away_col:
        results_data = matches[[home_col, away_col, score_home_col, score_away_col]]
        results_data = results_data.rename(columns={home_col: 'team_home', away_col: 'team_away', score_home_col: 'score_home', score_away_col: 'score_away'})
    else:
        # Se não houver resultados, preencher com 0
        results_data = results_data.rename(columns={home_col: 'team_home', away_col: 'team_away'})
        results_data['score_home'] = 0
        results_data['score_away'] = 0

    # Carregar results.csv existente, se houver
    if os.path.isfile(results_csv):
        try:
            existing_results = pd.read_csv(results_csv)
            # Concatenar e remover duplicatas
            results_data = pd.concat([existing_results, results_data]).drop_duplicates(subset=['team_home', 'team_away', 'score_home', 'score_away'], keep='last')
        except Exception as e:
            _log(f"Erro ao ler {results_csv}: {e}")
    else:
        os.makedirs(os.path.dirname(results_csv), exist_ok=True)

    # Salvar results.csv
    results_data.to_csv(results_csv, index=False)
    _log(f"Resultados salvos em {results_csv} com {len(results_data)} jogos")

    # Atualizar tactics.json
    teams = set(results_data['team_home']).union(set(results_data['team_away']))
    default_formations = ['4-2-3-1', '4-3-3', '4-4-2', '3-5-2', '4-1-4-1']
    tactics = {}
    
    # Carregar tactics.json existente, se houver
    if os.path.isfile(tactics_json):
        try:
            with open(tactics_json, 'r') as f:
                tactics = json.load(f)
        except Exception as e:
            _log(f"Erro ao ler {tactics_json}: {e}")

    # Atribuir formações padrão para times novos
    for i, team in enumerate(teams):
        norm_team = unidecode(team).lower().strip()
        if norm_team not in tactics:
            tactics[norm_team] = default_formations[i % len(default_formations)]

    # Salvar tactics.json
    os.makedirs(os.path.dirname(tactics_json), exist_ok=True)
    with open(tactics_json, 'w') as f:
        json.dump(tactics, f, indent=2)
    _log(f"Táticas salvas em {tactics_json} para {len(tactics)} times")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--results_csv", required=True)
    ap.add_argument("--tactics_json", required=True)
    args = ap.parse_args()

    update_history(args.source_csv, args.results_csv, args.tactics_json)

if __name__ == "__main__":
    main()