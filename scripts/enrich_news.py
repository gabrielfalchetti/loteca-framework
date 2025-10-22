# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import requests
import time
import os

def _log(msg: str) -> None:
    print(f"[enrich_news] {msg}", flush=True)

def enrich_news(features_in, features_out, newsapi_key, matches_csv):
    try:
        matches = pd.read_csv(matches_csv)
    except Exception as e:
        _log(f"Erro ao ler {matches_csv}: {e}")
        return

    home_col = 'team_home' if 'team_home' in matches.columns else 'home'
    away_col = 'team_away' if 'team_away' in matches.columns else 'away'
    teams = set(matches[home_col]).union(set(matches[away_col]))
    _log(f"Filtrando para {len(teams)} times dos jogos atuais: {teams}")

    try:
        features = pd.read_parquet(features_in)
    except Exception as e:
        _log(f"Erro ao ler {features_in}: {e}")
        return

    _log(f"Colunas disponíveis no DataFrame: {list(features.columns)}")
    for team in teams:
        url = f"https://newsapi.org/v2/everything?q={team}&apiKey={newsapi_key}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('status') == 'error':
                _log(f"Erro ao buscar notícias para {team}: {data}")
                continue
            features.loc[features['team'] == team, 'sentiment'] = 0.0
        except Exception as e:
            _log(f"Erro ao buscar notícias para {team}: {e}")
            if 'rateLimited' in str(e):
                _log("Rate limit atingido. Considere plano pago da NewsAPI ou esperar 12 horas.")
        time.sleep(1)

    features.to_parquet(features_out)
    _log(f"Features enriquecidas com notícias salvas em {features_out}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--newsapi_key", required=True)
    ap.add_argument("--matches_csv", required=True)
    args = ap.parse_args()

    enrich_news(args.features_in, args.features_out, args.newsapi_key, args.matches_csv)

if __name__ == "__main__":
    main()