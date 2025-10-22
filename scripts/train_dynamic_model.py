# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import os
from sklearn.ensemble import RandomForestClassifier
import pickle

def _log(msg: str) -> None:
    print(f"[train_dynamic_model] {msg}", flush=True)

def train_dynamic_model(features, out_state, out_model):
    try:
        features_df = pd.read_parquet(features)
    except Exception as e:
        _log(f"Erro ao ler {features}: {e}")
        return

    try:
        history = pd.read_csv('data/history/results.csv')
    except Exception as e:
        _log(f"Erro ao ler data/history/results.csv: {e}, usando dados padrão")
        history = pd.DataFrame()

    _log(f"Colunas disponíveis no DataFrame: {list(features_df.columns)}")
    if history.empty:
        _log("Histórico vazio, usando labels padrão")
        model = RandomForestClassifier()
    else:
        # Ajustar para usar 'team' em vez de 'team_home'/'team_away'
        features_df['team_home'] = features_df['team']
        features_df['team_away'] = features_df['team']
        merged_df = features_df.merge(history, on=['team_home', 'team_away'], how='left', suffixes=('', '_hist'))
        model = RandomForestClassifier()
        try:
            model.fit(merged_df[['avg_goals_scored', 'avg_goals_conceded']], merged_df['score_home'].fillna(0))
        except Exception as e:
            _log(f"Erro ao treinar modelo: {e}, usando modelo padrão")
            model = RandomForestClassifier()

    os.makedirs(os.path.dirname(out_model), exist_ok=True)
    with open(out_model, 'wb') as f:
        pickle.dump(model, f)
    with open(out_state, 'w') as f:
        f.write('{}')
    _log(f"Estado salvo em {out_state}")
    _log(f"Modelo salvo em {out_model}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--out_state", required=True)
    ap.add_argument("--out_model", required=True)
    args = ap.parse_args()

    train_dynamic_model(args.features, args.out_state, args.out_model)

if __name__ == "__main__":
    main()