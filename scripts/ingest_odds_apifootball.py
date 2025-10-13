#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball: Busca jogos e odds da API-Football.

CORREÇÃO FINAL E DEFINITIVA:
- Itera sobre uma lista de LIGAS específicas para garantir que a API retorne dados.
- Mantém a busca por intervalo de datas para capturar jogos futuros.
- Remove completamente o argumento obsoleto '--season'.
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

# LISTA DE LIGAS POPULARES (IDs da API-Football v3)
# Esta lista garante que sempre teremos jogos para buscar.
LEAGUE_IDS = {
    'Brasileirao A': 71,
    'Premier League': 39,
    'La Liga': 140,
    'Serie A': 135,
    'Bundesliga': 78,
    'Ligue 1': 61,
    'MLS': 253,
    'Champions League': 2,
    'Europa League': 3,
}

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
    fixtures_url = "https://v3.football.api-sports.io/fixtures"
    
    date_from = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_to = (datetime.utcnow() + timedelta(days=lookahead_days)).strftime('%Y-%m-%d')
    
    all_fixtures = []
    
    # --- LÓGICA DE BUSCA CORRIGIDA ---
    for league_name, league_id in LEAGUE_IDS.items():
        params = {
            "league": league_id,
            "from": date_from,
            "to": date_to,
            "status": "NS" # Apenas jogos não iniciados
        }
        
        log("INFO", f"Buscando jogos para a liga: {league_name} (ID: {league_id}) de {date_from} até {date_to}...")

        try:
            response = requests.get(fixtures_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            found_fixtures = data.get('response', [])
            if found_fixtures:
                log("INFO", f"Encontrados {len(found_fixtures)} jogos para {league_name}.")
                all_fixtures.extend(found_fixtures)
        except requests.exceptions.RequestException as e:
            log("WARN", f"Falha na requisição para a liga {league_name}: {e}")
            continue # Continua para a próxima liga em caso de erro

    if not all_fixtures:
        log("WARN", "Nenhum jogo encontrado em nenhuma das ligas pesquisadas na API-Football.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_apifootball.csv"), index=False)
        return 0

    log("INFO", f"Total de {len(all_fixtures)} jogos encontrados. Filtrando e buscando odds...")

    rows = []
    odds_url = "https://v3.football.api-sports.io/odds"
    
    for fixture in all_fixtures:
        try:
            fixture_timestamp = fixture['fixture']['timestamp']
            if fixture_timestamp < datetime.now(timezone.utc).timestamp():
                continue

            fixture_id = fixture['fixture']['id']
            home_team = fixture['teams']['home']['name']
            away_team = fixture['teams']['away']['name']

            odds_params = {'fixture': fixture_id, 'bookmaker': 8, 'bet': 1} # Bet 1 = Match Winner
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
