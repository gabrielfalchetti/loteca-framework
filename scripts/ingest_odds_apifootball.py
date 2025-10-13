#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball: Busca jogos e odds da API-Football.

CORREÇÃO FINAL: Corrige o SyntaxError na linha 76, completando a
comparação de timestamp para filtrar corretamente os jogos que já começaram.
"""

import os
import sys
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

def log(level, msg):
    tag = "" if level == "INFO" else f"[{level}] "
    print(f"[apifootball]{tag}{msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--aliases", default="")
    args = ap.parse_args()

    API_KEY = os.environ.get("API_FOOTBALL_KEY")
    lookahead_days = int(os.environ.get("LOOKAHEAD_DAYS", 3))

    if not API_KEY:
        log("CRITICAL", "Variável de ambiente API_FOOTBALL_KEY não definida.")
        sys.exit(5)

    headers = {'x-apisports-key': API_KEY}
    url = "https://v3.football.api-sports.io/fixtures"
    
    # Começa a busca do dia anterior para evitar problemas de fuso horário.
    date_from = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_to = (datetime.utcnow() + timedelta(days=lookahead_days)).strftime('%Y-%m-%d')
    
    params = {
        "from": date_from,
        "to": date_to,
        "status": "NS" # Apenas jogos não iniciados (Not Started)
    }

    log("INFO", f"Buscando jogos de {date_from} até {date_to}...")

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        log("ERROR", f"Falha na requisição à API-Football: {e}")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_apifootball.csv"), index=False)
        return 0

    fixtures = data.get('response', [])
    if not fixtures:
        log("WARN", "Nenhum jogo encontrado na API-Football para o período.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_apifootball.csv"), index=False)
        return 0

    log("INFO", f"{len(fixtures)} jogos encontrados. Filtrando e buscando odds...")

    rows = []
    odds_url = "https://v3.football.api-sports.io/odds"
    
    for fixture in fixtures:
        try:
            # CORREÇÃO: A linha abaixo foi completada para a comparação de timestamp correta.
            fixture_timestamp = fixture['fixture']['timestamp']
            if fixture_timestamp < datetime.now(timezone.utc).timestamp():
                continue

            fixture_id = fixture['fixture']['id']
            home_team = fixture['teams']['home']['name']
            away_team = fixture['teams']['away']['name']

            # Busca odds para o jogo (Bookmaker 8 = Bet365)
            odds_params = {'fixture': fixture_id, 'bookmaker': 8, 'bet': 1}
            odds_response = requests.get(odds_url, headers=headers, params=odds_params)
            odds_response.raise_for_status()
            odds_data = odds_response.json().get('response', [])
            
            if not odds_data or not odds_data[0].get('bookmakers'):
                continue

            bets = odds_data[0]['bookmakers'][0].get('bets', [])
            match_winner_odds = next((b['values'] for b in bets if b['name'] == 'Match Winner'), None)
            
            if not match_winner_odds:
                continue

            odds_dict = {o['value']: o['odd'] for o in match_winner_odds}
            
            # Garante que todas as odds (Home, Draw, Away) estão presentes
            if 'Home' in odds_dict and 'Draw' in odds_dict and 'Away' in odds_dict:
                rows.append({
                    'match_id': fixture_id, 'home': home_team, 'away': away_team,
                    'odds_home': float(odds_dict['Home']),
                    'odds_draw': float(odds_dict['Draw']),
                    'odds_away': float(odds_dict['Away']),
                })
        except (KeyError, IndexError, requests.exceptions.RequestException):
            continue

    if not rows:
        df = pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away'])
    else:
        df = pd.DataFrame(rows).dropna()
        df = df[(df['odds_home'] > 1) & (df['odds_draw'] > 1) & (df['odds_away'] > 1)]

    out_path = os.path.join(args.rodada, "odds_apifootball.csv")
    df.to_csv(out_path, index=False)
    log("INFO", f"Arquivo odds_apifootball.csv gerado com {len(df)} jogos válidos.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
