#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_theoddsapi: Busca jogos e odds da TheOddsAPI.

CORREÇÃO: Substitui o endpoint inválido 'soccer_all_leagues' por uma
lista de ligas de futebol populares e válidas.
"""

import os
import sys
import argparse
import requests
import pandas as pd
import hashlib

def log(level, msg):
    tag = "" if level == "INFO" else f"[{level}] "
    print(f"[theoddsapi]{tag}{msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="us,eu,uk,au")
    ap.add_argument("--aliases", default="")
    args = ap.parse_args()

    API_KEY = os.environ.get("THEODDS_API_KEY")
    if not API_KEY:
        log("CRITICAL", "Variável de ambiente THEODDS_API_KEY não definida.")
        sys.exit(5)

    # Lista de ligas de futebol populares (a API recomenda não usar 'all')
    SPORTS = [
        'soccer_brazil_campeonato',
        'soccer_epl', # English Premier League
        'soccer_spain_la_liga',
        'soccer_italy_serie_a',
        'soccer_germany_bundesliga',
        'soccer_france_ligue_one',
        'soccer_uefa_champs_league',
        'soccer_uefa_europa_league',
        'soccer_usa_mls'
    ]

    all_games = []
    for sport_key in SPORTS:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
        params = {
            'apiKey': API_KEY,
            'regions': args.regions,
            'markets': 'h2h', # Head-to-head (1X2)
            'oddsFormat': 'decimal'
        }
        
        log("INFO", f"Buscando jogos para a liga: {sport_key}...")

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            all_games.extend(data)
        except requests.exceptions.RequestException as e:
            log("WARN", f"Falha na requisição para {sport_key} (pode ser inativa): {e}")
            continue

    if not all_games:
        log("WARN", "Nenhum jogo encontrado na TheOddsAPI para as ligas especificadas.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_theoddsapi.csv"), index=False)
        return 0

    log("INFO", f"Total de {len(all_games)} jogos encontrados. Processando odds...")
    
    rows = []
    for game in all_games:
        try:
            home_team = game.get('home_team')
            away_team = game.get('away_team')
            
            identifier = f"{home_team}-{away_team}-{game.get('commence_time')}"
            match_id = int(hashlib.md5(identifier.encode()).hexdigest(), 16) % (10**8)

            bookmaker = next((b for b in game.get('bookmakers', []) if any(m['key'] == 'h2h' for m in b.get('markets', []))), None)
            if not bookmaker:
                continue
                
            h2h_market = next((m for m in bookmaker['markets'] if m['key'] == 'h2h'), None)
            if not h2h_market:
                continue

            outcomes = h2h_market.get('outcomes', [])
            odds_map = {o['name']: o['price'] for o in outcomes}
            
            odds_home = odds_map.get(home_team)
            odds_away = odds_map.get(away_team)
            odds_draw = odds_map.get('Draw')
            
            if not all([odds_home, odds_draw, odds_away]):
                continue

            rows.append({
                'match_id': match_id,
                'home': home_team,
                'away': away_team,
                'odds_home': float(odds_home),
                'odds_draw': float(odds_draw),
                'odds_away': float(odds_away)
            })
        except (KeyError, IndexError):
            continue

    if not rows:
        df = pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away'])
    else:
        df = pd.DataFrame(rows)

    out_path = os.path.join(args.rodada, "odds_theoddsapi.csv")
    df.to_csv(out_path, index=False)
    log("INFO", f"Arquivo odds_theoddsapi.csv gerado com {len(df)} jogos.")

    return 0

if __name__ == "__main__":
    sys.exit(main())
