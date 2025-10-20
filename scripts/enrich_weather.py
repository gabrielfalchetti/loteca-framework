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
    "Brentford": "London",
    "West Ham United": "London",
    "Valencia": "Valencia",
    "Alaves": "Vitoria-Gasteiz",
    "Udinese": "Udine",
    "Cremonese": "Cremona",
    "Al Duhail": "Doha",
    "Al Wahda": "Abu Dhabi",
    "Al Ittihad": "Jeddah",
    "Al Shorta": "Baghdad",
    "Traktor Sazi FC": "Tabriz",
    "Sharjah": "Sharjah",
    "Al Gharafa": "Doha",
    "Al Ahli": "Doha",
    "Kasimpasa": "Istanbul",
    "Eyupspor": "Istanbul",
    "Antwerp": "Antwerp",
    "Standard Liege": "Liege",
    "Burgos": "Burgos",
    "Cadiz": "Cadiz",
    "SC Amiens": "Amiens",
    "AS Nancy Lorraine": "Nancy",
    "RKC Waalwijk": "Waalwijk",
    "FC Dordrecht": "Dordrecht",
    "Willem II": "Tilburg",
    "Jong AZ": "Alkmaar",
    "ADO Den Haag": "Den Haag",
    "Jong PSV": "Eindhoven"
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--weatherapi_key", default=os.getenv("WEATHERAPI_KEY"))
    args = ap.parse_args()

    if not args.weatherapi_key:
        _log("WEATHERAPI_KEY não definida")
        sys.exit(2)

    df = pd.read_parquet(args.features_in)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    # Adicionar columns para clima
    df['rain_prob'] = 0.0
    df['temperature'] = 0.0

    for idx, row in df.iterrows():
        home_team = row['home']
        city = TEAM_CITY_MAP.get(home_team, "Unknown")
        if city == "Unknown":
            continue

        url = f"http://api.weatherapi.com/v1/forecast.json?key={args.weatherapi_key}&q={city}&days=1"
        try:
            response = requests.get(url, timeout=25)
            response.raise_for_status()
            weather_data = response.json()
            forecast = weather_data['forecast']['forecastday'][0]['day']
            df.at[idx, 'rain_prob'] = forecast['daily_chance_of_rain']
            df.at[idx, 'temperature'] = forecast['avgtemp_c']
        except Exception as e:
            _log(f"Erro ao buscar clima para {city}: {e}")

    df.to_parquet(args.features_out, index=False)
    _log(f"Features enriquecidas com clima salvas em {args.features_out}")

if __name__ == "__main__":
    main()