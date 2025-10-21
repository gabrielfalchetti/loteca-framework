# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import json
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[features] {msg}", flush=True)

def feature_engineer(history_csv, tactics_json, out_parquet, ewma):
    history = pd.read_csv(history_csv)
    with open(tactics_json, 'r') as f:
        tactics = json.load(f)

    # Inicializar DataFrame de features
    teams = history['team_home'].unique()
    features = pd.DataFrame({'team': teams})
    features['avg_goals_scored'] = history.groupby('team_home')['score_home'].mean().reindex(teams).fillna(0.0)
    features['avg_goals_conceded'] = history.groupby('team_home')['score_away'].mean().reindex(teams).fillna(0.0)
    features['formation'] = [tactics.get(team, "4-3-3") for team in teams]
    # Adicionar mais features aqui (ex.: sentiment, injuries, rain_prob, temperature de enrichment)
    features.to_parquet(out_parquet, index=False)
    _log(f"OK — gerado {out_parquet} com {len(features)} linhas")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True)
    ap.add_argument("--tactics", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ewma", type=float, default=0.20)
    args = ap.parse_args()

    feature_engineer(args.history, args.tactics, args.out, args.ewma)

if __name__ == "__main__":
    main()