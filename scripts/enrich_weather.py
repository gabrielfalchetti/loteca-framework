# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import requests
import os
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[enrich_weather] {msg}", flush=True)

# Mapeamento de times para cidades (estádios)
TEAM_CITY_MAP = {
    "Flamengo": "Rio de Janeiro",
    "Palmeiras": "São Paulo",
    "Internacional": "Porto Alegre",
    "Sport": "Recife",
    "Corinthians": "São Paulo",
    "Atlético": "Belo Horizonte",
    "Roma": "Rome",
    "Inter": "Milan",
    "Atlético madrid": "Madrid",
    "Osasuna": "Pamplona",
    "Cruzeiro": "Belo Horizonte",
    "Fortaleza": "Fortaleza",
    "Tottenham": "London",
    "Aston villa": "Birmingham",
    "Mirassol": "Mirassol",
    "São paulo": "São Paulo",
    "Ceara": "Fortaleza",
    "Botafogo": "Rio de Janeiro",
    "Liverpool": "Liverpool",
    "Manchester united": "Manchester",
    "Atalanta": "Bergamo",
    "Lazio": "Rome",
    "Bahia": "Salvador",
    "Grêmio": "Porto Alegre",
    "Milan": "Milan",
    "Fiorentina": "Florence",
    "Getafe": "Getafe",
    "Real madrid": "Madrid"
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--weatherapi_key", default=os.getenv("WEATHERAPI_KEY"))
    args = ap.parse_args()

    if not args.weatherapi_key:
        _log("WEATHERAPI_KEY não definida, pulando enriquecimento de clima")
        return

    df = pd.read_parquet(args.features_in)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    # Adicionar colunas para clima
    df['rain_prob'] = 0.0
    df['temperature'] = 0.0

    for idx, row in df.iterrows():
        home_team = row['team_home'] if 'team_home' in df.columns else row['home'] if 'home' in df.columns else None
        if home_team is None:
            _log(f"Coluna 'team_home' ou 'home' não encontrada no DataFrame. Colunas disponíveis: {list(df.columns)}")
            continue
        city = TEAM_CITY_MAP.get(home_team, "Unknown")
        if city == "Unknown":
            _log(f"Cidade não mapeada para {home_team}")
            continue

        url = f"http://api.weatherapi.com/v1/forecast.json?key={args.weatherapi_key}&q={city}&days=1"
        try:
            response = requests.get(url, timeout=25)
            response.raise_for_status()
            weather_data = response.json()
            forecast = weather_data['forecast']['forecastday'][0]['day']
            df.at[idx, 'rain_prob'] = forecast['daily_chance_of_rain']
            df.at[idx, 'temperature'] = forecast['avgtemp_c']
            _log(f"Clima para {home_team} ({city}): chuva={forecast['daily_chance_of_rain']}%, temp={forecast['avgtemp_c']}°C")
        except Exception as e:
            _log(f"Erro ao buscar clima para {city}: {e}")

    df.to_parquet(args.features_out, index=False)
    _log(f"Features enriquecidas com clima salvas em {args.features_out}")

if __name__ == "__main__":
    main()