#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_theoddsapi: Busca odds da TheOddsAPI e filtra pelos jogos
de um arquivo CSV de origem (ex: matches_sources.csv).
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
    # CORREÇÃO: Adiciona o argumento --source_csv que estava faltando
    ap.add_argument("--source_csv", required=True, help="Caminho para o CSV com a lista de jogos")
    args = ap.parse_args()

    API_KEY = os.environ.get("THEODDS_API_KEY")
    if not API_KEY:
        log("CRITICAL", "Variável de ambiente THEODDS_API_KEY não definida.")
        sys.exit(5)
        
    try:
        source_df = pd.read_csv(args.source_csv)
        target_games = { (str(row['home']).lower(), str(row['away']).lower()) for _, row in source_df.iterrows() }
    except FileNotFoundError:
        log("CRITICAL", f"Arquivo de origem {args.source_csv} não encontrado.")
        sys.exit(5)

    SPORTS = [
        'soccer_brazil_campeonato', 'soccer_epl', 'soccer_spain_la_liga', 'soccer_italy_serie_a',
        'soccer_germany_bundesliga', 'soccer_france_ligue_one', 'soccer_uefa_champs_league',
        'soccer_uefa_europa_league', 'soccer_usa_mls', 'soccer_argentina_primera_division'
    ]

    all_games_from_api = []
    for sport_key in SPORTS:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
        params = {'apiKey': API_KEY, 'regions': args.regions, 'markets': 'h2h', 'oddsFormat': 'decimal'}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            all_games_from_api.extend(response.json())
        except requests.exceptions.RequestException:
            continue
    
    if not all_games_from_api:
        log("WARN", "Nenhum jogo encontrado na TheOddsAPI.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_theoddsapi.csv"), index=False)
        return 0

    log("INFO", f"Total de {len(all_games_from_api)} jogos encontrados na API. Filtrando pela lista de {len(target_games)} jogos...")
    
    rows = []
    for game in all_games_from_api:
        try:
            home_team = game['home_team']
            away_team = game['away_team']

            # Verifica se o jogo da API está na nossa lista de alvos
            # A busca é flexível, verificando se os nomes estão contidos um no outro
            found = False
            for target_home, target_away in target_games:
                if (target_home in home_team.lower() and target_away in away_team.lower()):
                    found = True
                    # Usa os nomes do nosso arquivo fonte para consistência
                    home_team = source_df.loc[(source_df['home'].str.lower() == target_home) & (source_df['away'].str.lower() == target_away), 'home'].iloc[0]
                    away_team = source_df.loc[(source_df['home'].str.lower() == target_home) & (source_df['away'].str.lower() == target_away), 'away'].iloc[0]
                    break
            
            if not found:
                continue
            
            log("INFO", f"Jogo da lista encontrado na API: {home_team} vs {away_team}")
            
            identifier = f"{home_team}-{away_team}-{game['commence_time']}"
            match_id = int(hashlib.md5(identifier.encode()).hexdigest(), 16) % (10**8)

            bookmaker = next(b for b in game['bookmakers'] if any(m['key'] == 'h2h' for m in b.get('markets', [])))
            h2h_market = next(m for m in bookmaker['markets'] if m['key'] == 'h2h')
            
            outcomes = {o['name']: o['price'] for o in h2h_market['outcomes']}
            
            # A API pode ter os nomes ligeiramente diferentes, então fazemos a busca correta
            odds_home = outcomes.get(game['home_team'])
            odds_away = outcomes.get(game['away_team'])
            odds_draw = outcomes.get('Draw')

            if not all([odds_home, odds_draw, odds_away]):
                continue

            rows.append({
                'match_id': match_id, 'home': home_team, 'away': away_team,
                'odds_home': float(odds_home),
                'odds_draw': float(odds_draw),
                'odds_away': float(odds_away)
            })
        except (KeyError, IndexError, StopIteration):
            continue

    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away'])
    out_path = os.path.join(args.rodada, "odds_theoddsapi.csv")
    df.to_csv(out_path, index=False)
    log("INFO", f"Arquivo odds_theoddsapi.csv gerado com {len(df)} jogos da sua lista.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
