import pandas as pd
import requests
import unidecode
import os
import json
from datetime import datetime, timedelta
import argparse

# Configurações
SPORTMONKS_BASE_URL = 'https://api.sportmonks.com/v3/football'
THEODDS_BASE_URL = 'https://api.the-odds-api.com/v4/sports/soccer_odds'

def normalize_team_name(name):
    """Normaliza nome do time, removendo estado e acentos."""
    if '/' in name:
        name = name.split('/')[0].strip()
    name = unidecode.unidecode(name.lower())
    return name.capitalize()

def load_aliases(aliases_file):
    """Carrega aliases de times de AUTO_ALIASES_JSON."""
    if os.path.exists(aliases_file):
        with open(aliases_file, 'r') as f:
            return json.load(f)
    return {}

def get_team_id_sportmonks(team_name, api_key, aliases):
    """Busca ID do time no Sportmonks, usando aliases se necessário."""
    team_name_normalized = normalize_team_name(team_name)
    alias = aliases.get(team_name_normalized.lower(), team_name_normalized)
    url = f"{SPORTMONKS_BASE_URL}/teams/search/{alias}?api_token={api_key}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200 and response.json().get('data'):
            return response.json()['data'][0]['id']
        print(f"[ingest_odds_sportmonks] Time não encontrado no Sportmonks: {team_name} (alias: {alias})")
    except Exception as e:
        print(f"[ingest_odds_sportmonks] Erro ao buscar time {team_name}: {e}")
    return None

def get_fixtures_sportmonks(league_id, date_from, date_to, home_team_id, away_team_id, api_key):
    """Busca fixtures por liga e filtra por times."""
    page = 1
    fixtures = []
    while True:
        url = (f"{SPORTMONKS_BASE_URL}/fixtures?league={league_id}&date_from={date_from}&date_to={date_to}"
               f"&page={page}&api_token={api_key}&include=participants")
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                print(f"[ingest_odds_sportmonks] Erro na busca de fixtures (liga {league_id}): {response.status_code}")
                break
            data = response.json()
            fixtures.extend(data.get('data', []))
            print(f"[ingest_odds_sportmonks] Resposta de fixtures para liga {league_id}: {len(data.get('data', []))} jogos encontrados")
            if not data.get('pagination', {}).get('has_more', False):
                break
            page += 1
        except Exception as e:
            print(f"[ingest_odds_sportmonks] Erro ao buscar fixtures (liga {league_id}): {e}")
            break
    
    for fixture in fixtures:
        participants = fixture.get('participants', [{}])
        if len(participants) >= 2:
            home_id = participants[0].get('id')
            away_id = participants[1].get('id')
            if home_id == home_team_id and away_id == away_team_id:
                return fixture
    return None

def get_odds_sportmonks(fixture_id, api_key):
    """Busca odds pre-match para um fixture."""
    url = f"{SPORTMONKS_BASE_URL}/odds/pre-match/by-fixture/{fixture_id}?api_token={api_key}&include=bookmakers"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200 and response.json().get('data'):
            odds_data = response.json()['data']
            for odd in odds_data:
                if odd.get('market_id') == 1:  # 1x2 market
                    values = {v['name']: v['value'] for v in odd.get('values', [])}
                    return {
                        'match_id': fixture_id,
                        'home_odds': float(values.get('1', 2.0)),
                        'draw_odds': float(values.get('X', 3.0)),
                        'away_odds': float(values.get('2', 2.0))
                    }
        print(f"[ingest_odds_sportmonks] Nenhuma odds encontrada para fixture {fixture_id}")
    except Exception as e:
        print(f"[ingest_odds_sportmonks] Erro ao buscar odds para fixture {fixture_id}: {e}")
    return {'match_id': fixture_id, 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}

def get_odds_theodds(team_name, opponent, region, api_key):
    """Fallback para The Odds API."""
    url = f"{THEODDS_BASE_URL}?apiKey={api_key}&regions={region}&markets=h2h"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            events = response.json()
            team_norm = normalize_team_name(team_name)
            opp_norm = normalize_team_name(opponent)
            for event in events:
                if event['home_team'].lower() == team_norm.lower() and event['away_team'].lower() == opp_norm.lower():
                    odds = event['bookmakers'][0]['markets'][0]['outcomes']
                    return {
                        'match_id': event['id'],
                        'home_odds': next(o['price'] for o in odds if o['name'].lower() == team_norm.lower()),
                        'draw_odds': next(o['price'] for o in odds if o['name'].lower() == 'draw'),
                        'away_odds': next(o['price'] for o in odds if o['name'].lower() == opp_norm.lower())
                    }
        print(f"[ingest_odds_sportmonks] Nenhuma odds encontrada no The Odds API para {team_name} x {opponent}")
    except Exception as e:
        print(f"[ingest_odds_sportmonks] Erro ao buscar odds no The Odds API: {e}")
    return {'match_id': f"{team_norm}_vs_{opp_norm}", 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}

def main():
    parser = argparse.ArgumentParser(description='Ingest odds from Sportmonks and The Odds API.')
    parser.add_argument('--rodada', required=True, help='Output directory')
    parser.add_argument('--source_csv', required=True, help='Path to matches_norm.csv')
    parser.add_argument('--api_key', required=True, help='Sportmonks API key')
    parser.add_argument('--regions', required=True, help='Regions for The Odds API')
    parser.add_argument('--aliases_file', required=True, help='Path to auto_aliases.json')
    parser.add_argument('--api_key_theodds', required=True, help='The Odds API key')
    
    args = parser.parse_args()
    
    # Configurações
    output_path = os.path.join(args.rodada, 'odds_consensus.csv')
    date_from = datetime.now().strftime('%Y-%m-%d')
    date_to = (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d')
    leagues = [71, 72, 73]  # Série A, B, C
    aliases = load_aliases(args.aliases_file)
    
    # Ler matches_norm.csv
    if not os.path.exists(args.source_csv):
        raise FileNotFoundError(f"Arquivo {args.source_csv} não encontrado")
    matches_df = pd.read_csv(args.source_csv)
    print(f"[ingest_odds_sportmonks] Colunas em {args.source_csv}: {matches_df.columns.tolist()}")
    
    odds_data = []
    for _, row in matches_df.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']
        home_team_id = get_team_id_sportmonks(home_team, args.api_key, aliases)
        away_team_id = get_team_id_sportmonks(away_team, args.api_key, aliases)
        
        if not home_team_id or not away_team_id:
            print(f"[ingest_odds_sportmonks] Usando The Odds API para {home_team} x {away_team}")
            odds = get_odds_theodds(home_team, away_team, args.regions, args.api_key_theodds)
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
            continue
        
        found = False
        for league_id in leagues:
            fixture = get_fixtures_sportmonks(league_id, date_from, date_to, home_team_id, away_team_id, args.api_key)
            if fixture:
                odds = get_odds_sportmonks(fixture['id'], args.api_key)
                odds['home_team'] = normalize_team_name(home_team)
                odds['away_team'] = normalize_team_name(away_team)
                odds['league_id'] = league_id
                odds_data.append(odds)
                found = True
                print(f"[ingest_odds_sportmonks] Odds encontradas para {home_team} x {away_team} na liga {league_id}")
                break
        
        if not found:
            print(f"[ingest_odds_sportmonks] Nenhuma odds encontrada para {home_team} x {away_team}, usando The Odds API")
            odds = get_odds_theodds(home_team, away_team, args.regions, args.api_key_theodds)
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
    
    # Salvar CSV
    df = pd.DataFrame(odds_data)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[ingest_odds_sportmonks] Odds salvas em {output_path}")

if __name__ == "__main__":
    main()