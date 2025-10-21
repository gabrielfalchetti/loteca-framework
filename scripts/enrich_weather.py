# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
from weather_api import WeatherAPI
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[enrich_weather] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--weatherapi_key", default=os.getenv("WEATHERAPI_KEY"))
    args = ap.parse_args()

    if not args.weatherapi_key:
        _log("WEATHERAPI_KEY não definida, pulando enriquecimento de clima")
        return

    weather = WeatherAPI(args.weatherapi_key)
    df = pd.read_parquet(args.features_in)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    _log(f"Colunas disponíveis no DataFrame: {list(df.columns)}")

    # Verificar coluna 'team'
    if 'team' not in df.columns:
        _log(f"Coluna 'team' não encontrada no DataFrame. Colunas disponíveis: {list(df.columns)}")
        sys.exit(2)

    # Adicionar colunas para clima
    df['rain_prob'] = 0.0
    df['temperature'] = 0.0

    for idx, row in df.iterrows():
        team = row['team']
        match_date = datetime.strptime(row['date'], '%Y-%m-%d') if 'date' in df.columns else datetime.now()

        # Buscar clima para o time
        try:
            forecast = weather.get_forecast(team, days=3)  # Para LOOKAHEAD_DAYS=3
            df.at[idx, 'rain_prob'] = forecast['rain_probability']
            df.at[idx, 'temperature'] = forecast['temperature']
            _log(f"Sucesso para {team}")
        except Exception as e:
            _log(f"Erro ao buscar clima para {team}: {e}")

    df.to_parquet(args.features_out, index=False)
    _log(f"Features enriquecidas com clima salvas em {args.features_out}")

if __name__ == "__main__":
    main()