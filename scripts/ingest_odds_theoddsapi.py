#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_theoddsapi: Busca jogos e odds da TheOddsAPI.

MODO PRODUÇÃO: Este script busca jogos futuros e suas odds, sem depender
de uma whitelist pré-existente.
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

    # Lista de ligas de futebol populares (pode ser expandida)
    # A TheOddsAPI usa "key" para identificar esportes/ligas
    SPORT_KEY = "soccer_all_leagues" # Chave genérica para buscar várias ligas

    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/odds"
    params = {
        'apiKey': API_KEY,
        'regions': args.regions,
        'markets': 'h2h', # Head-to-head, que inclui o empate
        'oddsFormat': 'decimal'
    }
    
    log("INFO", f"Buscando jogos de futebol para as regiões: {args.regions}...")

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        log("ERROR", f"Falha na requisição à TheOddsAPI: {e}")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_theoddsapi.csv"), index=False)
        sys.exit(0)

    if not data:
        log("WARN", "Nenhum jogo encontrado na TheOddsAPI.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_theoddsapi.csv"), index=False)
        return 0

    log("INFO", f"{len(data)} jogos encontrados. Processando odds...")
    
    rows = []
    for game in data:
        home_team = game.get('home_team')
        away_team = game.get('away_team')

        # Cria um 'match_id' estável a partir dos nomes dos times e data, já que a API não fornece um
        # O hash garante um ID único para o par de times + data do jogo
        identifier = f"{home_team}-{away_team}-{game.get('commence_time')}"
        match_id = int(hashlib.md5(identifier.encode()).hexdigest(), 16) % (10**8)

        # Encontra o bookmaker com as odds (ex: 'Betfair') ou pega o primeiro
        bookmaker = next((b for b in game['bookmakers'] if b['key'] == 'betfair'), game['bookmakers'][0])
        markets = bookmaker.get('markets', [])
        h2h_market = next((m for m in markets if m['key'] == 'h2h'), None)

        if not h2h_market:
            continue

        outcomes = h2h_market.get('outcomes', [])
        
        # As odds podem estar em ordens diferentes, então precisamos mapeá-las
        odds_map = {o['name']: o['price'] for o in outcomes}
        
        # O nome do time da casa e visitante na API corresponde à ordem
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

    if not rows:
        log("WARN", "Nenhum jogo com odds completas (1X2) encontrado.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_theoddsapi.csv"), index=False)
        return 0

    df = pd.DataFrame(rows)
    out_path = os.path.join(args.rodada, "odds_theoddsapi.csv")
    df.to_csv(out_path, index=False)
    log("INFO", f"Arquivo odds_theoddsapi.csv gerado com {len(df)} jogos.")

    return 0

if __name__ == "__main__":
    sys.exit(main())
