# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import requests
from datetime import datetime
import time
import urllib.parse

def _log(msg: str) -> None:
    print(f"[enrich_weather] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--weatherapi_key", default=os.getenv("WEATHERAPI_KEY"))
    ap.add_argument("--matches_csv", required=True)  # Tornar obrigatório para evitar fallback
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
        _log(f"Arquivo {args.matches_csv} não encontrado, abortando")
        sys.exit(2)

    try:
        matches = pd.read_csv(args.matches_csv)
    except Exception as e:
        _log(f"Erro ao ler {args.matches_csv}: {str(e)}")
        sys.exit(2)

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
        'Fluminense': 'Rio de Janeiro,BR',
        'Internacional': 'Porto Alegre,BR',
        'Bahia': 'Salvador,BR',
        'Atlético mineiro': 'Belo Horizonte,BR',
        'Ceara': 'Fortaleza,BR',
        'Sport': 'Recife,BR',
        'Mirassol': 'Mirassol,BR',
        'Ferroviaria': 'Araraquara,BR',
        'Paysandu': 'Belém,BR',
        'Novorizontino': 'Novo Horizonte,BR',
        'Botafogo sp': 'Ribeirão Preto,BR',
        'Palmeiras': 'São Paulo,BR',
        'Flamengo': 'Rio de Janeiro,BR',
        'Santos': 'Santos,BR',
        'Cruzeiro': 'Belo Horizonte,BR',
        'Gremio': 'Porto Alegre,BR',
        'Vasco DA Gama': 'Rio de Janeiro,BR',
        'Fortaleza EC': 'Fortaleza,BR',
        'Juventude': 'Caxias do Sul,BR',
        'RB Bragantino': 'Bragança Paulista,BR',
        'Corinthians': 'São Paulo,BR',
        'Sao Paulo': 'São Paulo,BR',
        'Botafogo': 'Rio de Janeiro,BR',
        'Vitoria': 'Salvador,BR',
        'America Mineiro': 'Belo Horizonte,BR',
        'Criciuma': 'Criciúma,BR',
        'Atletico Paranaense': 'Curitiba,BR',
        'Cuiaba': 'Cuiabá,BR',
        'Vila Nova': 'Goiânia,BR',
        'CRB': 'Maceió,BR',
        'Athletic Club': 'São João del Rei,BR',
        'Operario-PR': 'Ponta Grossa,BR',
        'Volta Redonda': 'Volta Redonda,BR',
        'Coritiba': 'Curitiba,BR',
        'Atletico Goianiense': 'Goiânia,BR',
        'Chapecoense-sc': 'Chapecó,BR',
        'remo': 'Belém,BR',
        'Amazonas': 'Manaus,BR',
        'Avai': 'Florianópolis,BR',
        'Goias': 'Goiânia,BR'
    }

    for team in teams:
        city = TEAM_CITIES.get(team, 'São Paulo,BR')  # Default São Paulo com código do país
        city_encoded = urllib.parse.quote(city)  # Codificar cidade para URL
        try:
            url = f"http://api.weatherapi.com/v1/forecast.json?key={args.weatherapi_key}&q={city_encoded}&days=3"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            forecast = response.json()
            if 'forecast' not in forecast or 'forecastday' not in forecast['forecast']:
                _log(f"Resposta inválida para {team} em {city}: {forecast.get('error', 'No forecast data')}")
                continue
            day_forecast = forecast['forecast']['forecastday'][0]['day']  # Primeiro dia de previsão
            rain_prob = day_forecast.get('daily_chance_of_rain', 0) / 100.0  # Converte % para prob [0,1]
            temperature = day_forecast.get('avgtemp_c', 0.0)
            df.loc[df['team'] == team, 'rain_prob'] = rain_prob
            df.loc[df['team'] == team, 'temperature'] = temperature
            _log(f"Sucesso para {team} em {city}: rain_prob={rain_prob}, temperature={temperature}°C")
            time.sleep(0.5)  # Delay para evitar excesso de requisições
        except requests.exceptions.HTTPError as e:
            _log(f"Erro HTTP ao buscar clima para {team} em {city}: {str(e)}")
            if '401' in str(e) or '403' in str(e):
                _log("Erro de autorização (401/403). Verifique a WEATHERAPI_KEY no GitHub Secrets.")
            time.sleep(1)  # Delay maior após erro
        except Exception as e:
            _log(f"Erro ao buscar clima para {team} em {city}: {str(e)}")
            time.sleep(1)

    df.to_parquet(args.features_out, index=False)
    _log(f"Features enriquecidas com clima salvas em {args.features_out}")

if __name__ == "__main__":
    main()