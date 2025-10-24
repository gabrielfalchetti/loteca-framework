import pandas as pd
import requests
import unidecode
import os
import json
from datetime import datetime, timedelta
import argparse
import time

# Configurações
SPORTMONKS_BASE_URL = 'https://api.sportmonks.com/v3/football'

def normalize_team_name(name):
    """Normaliza nome do time, removendo estado e acentos."""
    if not name:
        return ""
    if '/' in name:
        name = name.split('/')[0].strip()
    name = unidecode.unidecode(name.lower())
    return name.capitalize()

def get_api_data(url, api_key, params=None):
    """Função genérica para chamar API Sportmonks com retry simples."""
    full_url = f"{url}?api_token={api_key}"
    if params:
        full_url += '&' + '&'.join([f"{k}={v}" for k, v in params.items()])
    try:
        response = requests.get(full_url, timeout=10)
        if response.status_code == 200:
            return response.json().get('data', [])
        print(f"[ingest_sportmonks] Erro na API: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[ingest_sportmonks] Erro na chamada API {url}: {e}")
    return []

def get_current_season_id(league_id, api_key):
    """Busca o ID da temporada atual para a liga."""
    url = f"{SPORTMONKS_BASE_URL}/leagues/{league_id}"
    params = {'include': 'seasons'}
    data = get_api_data(url, api_key, params)
    if data and isinstance(data, dict) and 'seasons' in data and data['seasons']:
        season_id = data['seasons'][-1]['id']
        print(f"[debug] Season ID para liga {league_id}: {season_id}")
        return season_id
    print(f"[ingest_sportmonks] Season não encontrada para liga {league_id}")
    return None

def load_aliases(aliases_file, api_key):
    """Carrega aliases de times."""
    if os.path.exists(aliases_file):
        with open(aliases_file, 'r') as f:
            return json.load(f)
    else:
        print("[ingest_sportmonks] Arquivo de aliases não encontrado. Gere usando generate_aliases.py")
        raise FileNotError(f"Arquivo {aliases_file} não encontrado")

def get_team_id_sportmonks(team_name, api_key, aliases):
    """Busca ID do time no Sportmonks, usando aliases se necessário."""
    team_name_normalized = normalize_team_name(team_name)
    alias = aliases.get(team_name_normalized.lower(), team_name_normalized)
    url = f"{SPORTMONKS_BASE_URL}/teams/search/{alias}"
    data = get_api_data(url, api_key)
    if data and isinstance(data, list) and len(data) > 0:
        team_id = data[0]['id']
        print(f"[debug] Time {team_name} (alias: {alias}) encontrado com ID: {team_id}")
        return team_id
    print(f"[ingest_sportmonks] Time não encontrado no Sportmonks: {team_name} (alias: {alias})")
    return None

def fetch_all_fixtures(leagues, date_from, date_to, api_key):
    """Busca todos os fixtures das ligas no intervalo de datas."""
    all_fixtures = []
    for league_id in leagues:
        page = 1
        while True:
            url = f"{SPORTMONKS_BASE_URL}/fixtures/between/{date_from}/{date_to}"
            params = {
                'filters': f'leagueIds:{league_id}',
                'page': page,
                'include': 'participants;weatherReport;referees;venue'
            }
            data = get_api_data(url, api_key, params)
            all_fixtures.extend(data)
            print(f"[ingest_sportmonks] Fetched {len(data)} fixtures from league {league_id} page {page}")
            if not data or len(data) < 100:
                break
            page += 1
            time.sleep(1)
    return all_fixtures

def find_matching_fixture(fixtures, home_team_id, away_team_id):
    """Busca fixture correspondente aos times."""
    for fixture in fixtures:
        participants = fixture.get('participants', [{}])
        if len(participants) >= 2:
            home_id = participants[0].get('id')
            away_id = participants[1].get('id')
            if (home_id == home_team_id and away_id == away_team_id) or (home_id == away_team_id and away_id == home_team_id):
                inverted = (home_id == away_team_id and away_id == home_team_id)
                print(f"[debug] Fixture encontrado: {fixture.get('name')} (inverted: {inverted})")
                return fixture, inverted
    return None, False

def get_odds_sportmonks(fixture_id, api_key):
    """Busca odds pre-match para um fixture."""
    url = f"{SPORTMONKS_BASE_URL}/odds/pre-match/by-fixture/{fixture_id}"
    params = {'include': 'bookmakers'}
    data = get_api_data(url, api_key, params)
    for odd in data:
        if odd.get('market_id') == 1:  # 1x2 market
            values = {v['name']: v['value'] for v in odd.get('values', [])}
            return {
                'match_id': fixture_id,
                'home_odds': float(values.get('1', 2.0)),
                'draw_odds': float(values.get('X', 3.0)),
                'away_odds': float(values.get('2', 2.0))
            }
    print(f"[ingest_sportmonks] Nenhuma odds encontrada para fixture {fixture_id}")
    return {'match_id': fixture_id, 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}

def main():
    parser = argparse.ArgumentParser(description='Ingest maximum data from Sportmonks for predictions.')
    parser.add_argument('--rodada', required=True, help='Output directory')
    parser.add_argument('--source_csv', required=True, help='Path to matches_norm.csv')
    parser.add_argument('--api_key', required=True, help='Sportmonks API key')
    parser.add_argument('--aliases_file', required=True, help='Path to auto_aliases.json')
    
    args = parser.parse_args()
    
    # Configurações
    output_path = os.path.join(args.rodada, 'odds_consensus.csv')
    team_stats_path = os.path.join(args.rodada, 'team_stats.csv')
    player_stats_path = os.path.join(args.rodada, 'player_stats.csv')
    transfers_path = os.path.join(args.rodada, 'transfers.csv')
    referee_path = os.path.join(args.rodada, 'referee_stats.csv')
    
    leagues = [71, 72, 73, 76, 8]  # Série A, B, C, Copa do Brasil, EPL
    season_ids = {league: get_current_season_id(league, args.api_key) for league in leagues}
    print(f"[debug] Season IDs: {season_ids}")
    aliases = load_aliases(args.aliases_file, args.api_key)
    
    # Ler matches_norm.csv
    if not os.path.exists(args.source_csv):
        raise FileNotFoundError(f"Arquivo {args.source_csv} não encontrado")
    matches_df = pd.read_csv(args.source_csv)
    print(f"[ingest_sportmonks] Colunas em {args.source_csv}: {matches_df.columns.tolist()}")
    
    # Validar colunas esperadas
    required_columns = ['match_id', 'home', 'away', 'date']
    if not all(col in matches_df.columns for col in required_columns):
        missing = [col for col in required_columns if col not in matches_df.columns]
        raise ValueError(f"Colunas ausentes em {args.source_csv}: {missing}")
    
    # Buscar todos os fixtures uma vez
    date_from = min(matches_df['date'])
    date_to = max(matches_df['date'])
    all_fixtures = fetch_all_fixtures(leagues, date_from, date_to, args.api_key)
    
    odds_data = []
    team_stats_data = []
    player_stats_path_data = []  # Renomeado para evitar erro
    transfers_data = []
    referee_data = []
    
    for _, row in matches_df.iterrows():
        home_team = row['home']
        away_team = row['away']
        match_id = row['match_id']
        home_team_id = get_team_id_sportmonks(home_team, args.api_key, aliases)
        away_team_id = get_team_id_sportmonks(away_team, args.api_key, aliases)
        
        if not home_team_id or not away_team_id:
            print(f"[ingest_sportmonks] Time não encontrado para {home_team} x {away_team}, usando odds padrão")
            odds = {'match_id': match_id, 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
            continue
        
        # Stats de times
        for league_id in leagues:
            season_id = season_ids.get(league_id, None)
            if season_id:
                team_stats_data.append(get_team_stats(home_team_id, season_id, args.api_key))
                team_stats_data.append(get_team_stats(away_team_id, season_id, args.api_key))
        
        # Transferências
        transfers_data.extend(get_transfers(home_team_id, args.api_key))
        transfers_data.extend(get_transfers(away_team_id, args.api_key))
        
        fixture, inverted = find_matching_fixture(all_fixtures, home_team_id, away_team_id)
        if fixture:
            odds = get_odds_sportmonks(fixture['id'], args.api_key)
            odds['match_id'] = match_id
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = league_id  # Use the league where it was found
            if inverted:
                temp = odds['home_odds']
                odds['home_odds'] = odds['away_odds']
                odds['away_odds'] = temp
                print(f"[debug] Odds trocadas devido a fixture invertido para {home_team} x {away_team}")
            odds_data.append(odds)
            
            player_stats_data.append(get_player_stats(fixture['id'], args.api_key))
            
            referee_data.append(get_referee_stats(fixture['id'], args.api_key))
            
            print(f"[ingest_sportmonks] Dados completos encontrados para {home_team} x {away_team} na liga {league_id}")
        else:
            print(f"[ingest_sportmonks] Nenhuma odds encontrada para {home_team} x {away_team}, usando odds padrão")
            odds = {'match_id': match_id, 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
        
        time.sleep(1)  # Respeitar rate limits
    
    # Salvar CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pd.DataFrame(odds_data).to_csv(output_path, index=False)
    pd.DataFrame([s for s in team_stats_data if s]).to_csv(team_stats_path, index=False)
    pd.DataFrame(player_stats_data).to_csv(player_stats_path, index=False)
    pd.DataFrame(transfers_data).to_csv(transfers_path, index=False)
    pd.DataFrame([r for r in referee_data if r['referee_id']]).to_csv(referee_path, index=False)
    
    print(f"[ingest_sportmonks] Dados salvos em {output_path}, {team_stats_path}, {player_stats_path}, {transfers_path}, {referee_path}")

if __name__ == "__main__":
    main()