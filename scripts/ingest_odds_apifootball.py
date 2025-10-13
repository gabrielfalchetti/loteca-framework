#!/usr/-bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball: Busca odds da API-Football DIRECIONADO pelos jogos
em um arquivo CSV de origem (ex: matches_sources.csv).
"""

import os
import sys
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta

def log(level, msg):
    tag = "" if level == "INFO" else f"[{level}] "
    print(f"[apifootball]{tag}{msg}", flush=True)

def find_fixture_id(headers, home_team, away_team):
    """Busca o ID de um jogo específico na API-Football."""
    url = "https://v3.football.api-sports.io/fixtures"
    date_to = (datetime.utcnow() + timedelta(days=3)).strftime('%Y-%m-%d')

    # Busca por nome de time para encontrar o jogo
    search_query = f"{home_team}-{away_team}"
    params = {'search': search_query, 'to': date_to, 'status': 'NS'}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        fixtures = response.json().get('response', [])
        
        # Procura pelo jogo que melhor corresponde
        for fixture in fixtures:
            api_home = fixture['teams']['home']['name']
            api_away = fixture['teams']['away']['name']
            if home_team.lower() in api_home.lower() or api_home.lower() in home_team.lower():
                 if away_team.lower() in api_away.lower() or api_away.lower() in away_team.lower():
                    return fixture['fixture']['id']
    except requests.exceptions.RequestException as e:
        log("WARN", f"Erro ao buscar fixtures para {home_team} vs {away_team}: {e}")
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    # CORREÇÃO: Adiciona o argumento --source_csv que estava faltando
    ap.add_argument("--source_csv", required=True, help="Caminho para o CSV com a lista de jogos")
    args = ap.parse_args()

    API_KEY = os.environ.get("API_FOOTBALL_KEY")
    if not API_KEY:
        log("CRITICAL", "Variável de ambiente API_FOOTBALL_KEY não definida.")
        sys.exit(5)

    try:
        source_df = pd.read_csv(args.source_csv)
    except FileNotFoundError:
        log("CRITICAL", f"Arquivo de origem {args.source_csv} não encontrado.")
        sys.exit(5)

    headers = {'x-apisports-key': API_KEY}
    odds_url = "https://v3.football.api-sports.io/odds"
    rows = []

    log("INFO", f"Iniciando busca direcionada para {len(source_df)} jogos do arquivo de origem.")

    for _, row in source_df.iterrows():
        home_team = row['home']
        away_team = row['away']
        log("INFO", f"Procurando jogo: {home_team} vs {away_team}")
        
        fixture_id = find_fixture_id(headers, home_team, away_team)
        
        if not fixture_id:
            log("WARN", f"Jogo não encontrado na API-Football: {home_team} vs {away_team}")
            continue

        log("INFO", f"Jogo encontrado (Fixture ID: {fixture_id}). Buscando odds...")

        try:
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
            
            if 'Home' in odds_dict and 'Draw' in odds_dict and 'Away' in odds_dict:
                rows.append({
                    'match_id': fixture_id, 'home': home_team, 'away': away_team,
                    'odds_home': float(odds_dict['Home']),
                    'odds_draw': float(odds_dict['Draw']),
                    'odds_away': float(odds_dict['Away']),
                })
        except (KeyError, IndexError, requests.exceptions.RequestException) as e:
            log("WARN", f"Não foi possível obter odds para {home_team} vs {away_team}: {e}")
            continue
    
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away'])
    
    out_path = os.path.join(args.rodada, "odds_apifootball.csv")
    df.to_csv(out_path, index=False)
    log("INFO", f"Arquivo odds_apifootball.csv gerado com {len(df)} jogos encontrados.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
