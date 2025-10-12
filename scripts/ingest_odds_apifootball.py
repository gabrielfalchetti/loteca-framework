#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball: Busca jogos e odds da API-Football.

MODO PRODUÇÃO: Este script busca jogos futuros e suas odds, sem depender
de uma whitelist pré-existente.
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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--aliases", default="")
    # Novo argumento para buscar dias à frente
    ap.add_argument("--lookahead_days", type=int, default=3)
    args = ap.parse_args()

    API_KEY = os.environ.get("API_FOOTBALL_KEY")
    if not API_KEY:
        log("CRITICAL", "Variável de ambiente API_FOOTBALL_KEY não definida.")
        sys.exit(5)

    headers = {'x-apisports-key': API_KEY}
    url = "https://v3.football.api-sports.io/fixtures"
    
    # Calcula a data final para a busca de jogos
    date_to = (datetime.utcnow() + timedelta(days=args.lookahead_days)).strftime('%Y-%m-%d')
    
    params = {
        "season": args.season,
        "to": date_to,
        "status": "NS" # Not Started - Apenas jogos não iniciados
    }

    log("INFO", f"Buscando jogos até {date_to}...")

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        log("ERROR", f"Falha na requisição à API-Football: {e}")
        # Cria um arquivo vazio para não quebrar o pipeline
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_apifootball.csv"), index=False)
        sys.exit(0) # Sai com sucesso para permitir que o workflow continue se possível

    fixtures = data.get('response', [])
    if not fixtures:
        log("WARN", "Nenhum jogo encontrado na API-Football para o período.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_apifootball.csv"), index=False)
        return 0

    log("INFO", f"{len(fixtures)} jogos encontrados. Buscando odds...")

    rows = []
    odds_url = "https://v3.football.api-sports.io/odds"
    
    for fixture in fixtures:
        fixture_id = fixture['fixture']['id']
        home_team = fixture['teams']['home']['name']
        away_team = fixture['teams']['away']['name']

        # Busca as odds para o jogo (Bookmaker ID 8 = Bet365, um dos mais comuns)
        odds_params = {'fixture': fixture_id, 'bookmaker': 8, 'bet': 1}
        try:
            odds_response = requests.get(odds_url, headers=headers, params=odds_params)
            odds_response.raise_for_status()
            odds_data = odds_response.json().get('response', [])
            
            if not odds_data or not odds_data[0]['bookmakers']:
                continue

            # Pega as odds de "Match Winner"
            match_winner_odds = odds_data[0]['bookmakers'][0]['bets'][0]['values']
            
            odds_dict = {o['value']: o['odd'] for o in match_winner_odds}

            rows.append({
                'match_id': fixture_id,
                'home': home_team,
                'away': away_team,
                'odds_home': float(odds_dict.get('Home')),
                'odds_draw': float(odds_dict.get('Draw')),
                'odds_away': float(odds_dict.get('Away')),
            })
        except Exception:
            # Ignora jogos sem odds e continua
            continue

    if not rows:
        log("WARN", "Nenhum jogo com odds encontrado.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_apifootball.csv"), index=False)
        return 0

    df = pd.DataFrame(rows)
    out_path = os.path.join(args.rodada, "odds_apifootball.csv")
    df.to_csv(out_path, index=False)
    log("INFO", f"Arquivo odds_apifootball.csv gerado com {len(df)} jogos.")

    return 0

if __name__ == "__main__":
    sys.exit(main())
