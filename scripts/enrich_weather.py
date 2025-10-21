# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import requests
from datetime import datetime
import time

def _log(msg: str) -> None:
    print(f"[enrich_weather] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--weatherapi_key", default=os.getenv("WEATHERAPI_KEY"))
    ap.add_argument("--matches_csv", default="data/out/18698638611/matches_norm.csv")  # Ajuste para OUT_DIR dinâmico
    args = ap.parse_args()

    if not args.weatherapi_key:
        _log("WEATHERAPI_KEY não definida, pulando enriquecimento de clima")
        return

    df = pd.read_parquet(args.features_in)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    _log(f"Colunas disponíveis no DataFrame: {list(df.columns)}")

    # Verificar coluna 'team'
    if 'team' not in df.columns:
        _log(f"Coluna 'team' não encontrada no DataFrame. Colunas disponíveis: {list(df.columns)}")
        sys.exit(2)

    # Carregar matches_norm.csv para filtrar times relevantes
    if not os.path.isfile(args.matches_csv):
        _log(f"Arquivo {args.matches_csv} não encontrado, usando todos os times")
        teams = df['team'].unique()
    else:
        matches = pd.read_csv(args.matches_csv)
        home_col = next((col for col in ['team_home', 'home'] if col in matches.columns), None)
        away_col = next((col for col in ['team_away', 'away'] if col in matches.columns), None)
        if not (home_col and away_col):
            _log("Colunas team_home/team_away ou home/away não encontradas em matches_norm.csv")
            sys.exit(2)
        teams = set(matches[home_col]).union(set(matches[away_col]))
        _log(f"Filtrando para {len(teams)} times dos jogos atuais: {teams}")

    # Adicionar colunas para clima
    df['rain_prob'] = 0.0
    df['temperature'] = 0.0

    # Mapeamento de times para cidades
    TEAM_CITIES = {
        'Fluminense': 'Rio de Janeiro',
        'Internacional': 'Porto Alegre',
        'Bahia': 'Salvador',
        'Atlético mineiro': 'Belo Horizonte',
        'Ceara': 'Fortaleza',
        'Sport': 'Recife',
        'Mirassol': 'Mirassol',
        'Ferroviaria': 'Araraquara',
        'Paysandu': 'Belém',
        'Novorizontino': 'Novo Horizonte',
        'Botafogo sp': 'Ribeirão Preto'
    }

    for team in teams:
        city = TEAM_CITIES.get(team, 'São Paulo')  # Default São Paulo
        try:
            url = f"http://api.weatherapi.com/v1/forecast.json?key={args.weatherapi_key}&q={city}&days=3"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            forecast = response.json()
            if 'forecast' not in forecast or 'forecastday' not in forecast['forecast']:
                _log(f"Resposta inválida para {team} em {city}: {forecast.get('error', 'No forecast data')}")
                continue
            day_forecast = forecast['forecast']['forecastday'][0]['day']
            rain_prob = day_forecast.get('daily_chance_of_rain', 0) / 100.0  # Converte % para prob [0,1]
            temperature = day_forecast.get('avgtemp_c', 0.0)
            df.loc[df['team'] == team, 'rain_prob'] = rain_prob
            df.loc[df['team'] == team, 'temperature'] = temperature
            _log(f"Sucesso para {team} em {city}: rain_prob={rain_prob}, temperature={temperature}°C")
            time.sleep(0.5)  # Delay para evitar excesso de requisições
        except Exception as e:
            _log(f"Erro ao buscar clima para {team} em {city}: {str(e)}")
            if 'rate limit' in str(e).lower():
                _log("Rate limit atingido. Verifique o plano da WeatherAPI.")
            time.sleep(1)  # Delay maior após erro

    df.to_parquet(args.features_out, index=False)
    _log(f"Features enriquecidas com clima salvas em {args.features_out}")

if __name__ == "__main__":
    main()