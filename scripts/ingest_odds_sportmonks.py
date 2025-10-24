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
THEODDS_BASE_URL = 'https://api.the-odds-api.com/v4/sports'

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

def get_team_id_sportmonks(team_name, api_key, aliases):
    """Busca ID do time no Sportmonks, usando aliases se necessário."""
    team_name_normalized = normalize_team_name(team_name)
    alias = aliases.get(team_name_normalized.lower(), team_name_normalized)
    url = f"{SPORTMONKS_BASE_URL}/teams/search/{alias}"
    data = get_api_data(url, api_key)
    if data:
        return data[0]['id']
    print(f"[ingest_sportmonks] Time não encontrado no Sportmonks: {team_name} (alias: {alias})")
    return None

def get_fixtures_sportmonks(league_id, date_from, date_to, home_team_id, away_team_id, api_key):
    """Busca fixtures por liga e filtra por times."""
    page = 1
    fixtures = []
    while True:
        url = f"{SPORTMONKS_BASE_URL}/fixtures"
        params = {
            'league': league_id,
            'date_from': date_from,
            'date_to': date_to,
            'page': page,
            'include': 'participants;weatherReport;referee;venue'
        }
        data = get_api_data(url, api_key, params)
        fixtures.extend(data)
        print(f"[ingest_sportmonks] Resposta de fixtures para liga {league_id}: {len(data)} jogos encontrados")
        if not data or not isinstance(data, list) or len(data) < 100:  # Assume 100 por página
            break
        page += 1
        time.sleep(1)  # Respeitar rate limits
    
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

def get_team_stats(team_id, season_id, api_key):
    """Extrai estatísticas de time."""
    url = f"{SPORTMONKS_BASE_URL}/statistics/teams/{team_id}"
    params = {'season': season_id, 'include': 'types'}  # Ex.: win rate, goals average
    data = get_api_data(url, api_key, params)
    stats = {
        'team_id': team_id,
        'goals_scored_avg': 0.0,
        'goals_conceded_avg': 0.0,
        'win_rate': 0.0
    }
    for stat in data.get('types', []):
        if stat['type_id'] == 42:  # Gols marcados por jogo
            stats['goals_scored_avg'] = stat.get('value', {}).get('average', 0.0)
        elif stat['type_id'] == 43:  # Gols sofridos por jogo
            stats['goals_conceded_avg'] = stat.get('value', {}).get('average', 0.0)
        elif stat['type_id'] == 44:  # Taxa de vitórias
            stats['win_rate'] = stat.get('value', {}).get('percentage', 0.0)
    return stats

def get_player_stats(fixture_id, api_key):
    """Extrai estatísticas de jogadores do lineup de um fixture."""
    url = f"{SPORTMONKS_BASE_URL}/fixtures/{fixture_id}"
    params = {'include': 'lineups.players.statistics;injury'}
    data = get_api_data(url, api_key, params)
    players = []
    for lineup in data.get('lineups', []):
        for player in lineup.get('players', []):
            stats = player.get('statistics', {})
            injury = player.get('injury', {})
            players.append({
                'player_id': player['id'],
                'goals': stats.get('goals', 0),
                'assists': stats.get('assists', 0),
                'minutes_played': stats.get('minutes', 0),
                'is_injured': injury.get('is_active', False)
            })
    return players

def get_transfers(team_id, api_key):
    """Extrai transferências recentes."""
    url = f"{SPORTMONKS_BASE_URL}/transfers"
    params = {'team': team_id, 'latest': True, 'include': 'player;fromTeam;toTeam'}
    data = get_api_data(url, api_key, params)
    transfers = []
    for transfer in data:
        transfers.append({
            'player_id': transfer.get('player', {}).get('id'),
            'from_team': transfer.get('fromTeam', {}).get('name'),
            'to_team': transfer.get('toTeam', {}).get('name'),
            'date': transfer.get('date')
        })
    return transfers

def get_referee_stats(fixture_id, api_key):
    """Extrai estatísticas de árbitro para um fixture."""
    url = f"{SPORTMONKS_BASE_URL}/fixtures/{fixture_id}"
    params = {'include': 'referee.statistics'}
    data = get_api_data(url, api_key, params)
    referee = data.get('referee', {})
    return {
        'referee_id': referee.get('id'),
        'yellow_cards_avg': referee.get('statistics', {}).get('yellow_cards', {}).get('average', 0.0),
        'red_cards_avg': referee.get('statistics', {}).get('red_cards', {}).get('average', 0.0)
    }

def get_odds_theodds(team_name, opponent, region, api_key):
    """Fallback para The Odds API."""
    url = f"{THEODDS_BASE_URL}/soccer_odds/events"
    params = {'regions': region, 'markets': 'h2h'}
    try:
        response = requests.get(url, params={'apiKey': api_key, **params}, timeout=10)
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
        print(f"[ingest_sportmonks] Nenhuma odds encontrada no The Odds API para {team_name} x {opponent}")
    except Exception as e:
        print(f"[ingest_sportmonks] Erro ao buscar odds no The Odds API: {e}")
    return {'match_id': f"{team_norm}_vs_{opp_norm}", 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}

def main():
    parser = argparse.ArgumentParser(description='Ingest maximum data from Sportmonks for predictions.')
    parser.add_argument('--rodada', required=True, help='Output directory')
    parser.add_argument('--source_csv', required=True, help='Path to matches_norm.csv')
    parser.add_argument('--api_key', required=True, help='Sportmonks API key')
    parser.add_argument('--regions', required=True, help='Regions for The Odds API')
    parser.add_argument('--aliases_file', required=True, help='Path to auto_aliases.json')
    parser.add_argument('--api_key_theodds', required=True, help='The Odds API key')
    
    args = parser.parse_args()
    
    # Configurações
    output_path = os.path.join(args.rodada, 'odds_consensus.csv')
    team_stats_path = os.path.join(args.rodada, 'team_stats.csv')
    player_stats_path = os.path.join(args.rodada, 'player_stats.csv')
    transfers_path = os.path.join(args.rodada, 'transfers.csv')
    referee_path = os.path.join(args.rodada, 'referee_stats.csv')
    
    date_from = datetime.now().strftime('%Y-%m-%d')
    date_to = (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d')
    season_id = 19735  # Brasileirão 2025, ajuste se necessário
    leagues = [71, 72, 73]  # Série A, B, C
    aliases = load_aliases(args.aliases_file)
    
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
    
    odds_data = []
    team_stats_data = []
    player_stats_data = []
    transfers_data = []
    referee_data = []
    
    for _, row in matches_df.iterrows():
        home_team = row['home']
        away_team = row['away']
        home_team_id = get_team_id_sportmonks(home_team, args.api_key, aliases)
        away_team_id = get_team_id_sportmonks(away_team, args.api_key, aliases)
        
        # Stats de times
        if home_team_id:
            team_stats_data.append(get_team_stats(home_team_id, season_id, args.api_key))
        if away_team_id:
            team_stats_data.append(get_team_stats(away_team_id, season_id, args.api_key))
        
        # Transferências
        if home_team_id:
            transfers_data.extend(get_transfers(home_team_id, args.api_key))
        if away_team_id:
            transfers_data.extend(get_transfers(away_team_id, args.api_key))
        
        # Odds e fixtures
        found = False
        for league_id in leagues:
            fixture = get_fixtures_sportmonks(league_id, date_from, date_to, home_team_id, away_team_id, args.api_key)
            if fixture:
                # Odds
                odds = get_odds_sportmonks(fixture['id'], args.api_key)
                odds['home_team'] = normalize_team_name(home_team)
                odds['away_team'] = normalize_team_name(away_team)
                odds['league_id'] = league_id
                odds_data.append(odds)
                
                # Stats de jogadores
                player_stats_data.extend(get_player_stats(fixture['id'], args.api_key))
                
                # Árbitro
                referee_data.append(get_referee_stats(fixture['id'], args.api_key))
                
                found = True
                print(f"[ingest_sportmonks] Dados completos encontrados para {home_team} x {away_team} na liga {league_id}")
                break
        
        if not found:
            print(f"[ingest_sportmonks] Usando The Odds API para {home_team} x {away_team}")
            odds = get_odds_theodds(home_team, away_team, args.regions, args.api_key_theodds)
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
        
        time.sleep(1)  # Respeitar rate limits
    
    # Salvar dados
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pd.DataFrame(odds_data).to_csv(output_path, index=False)
    pd.DataFrame(team_stats_data).to_csv(team_stats_path, index=False)
    pd.DataFrame(player_stats_data).to_csv(player_stats_path, index=False)
    pd.DataFrame(transfers_data).to_csv(transfers_path, index=False)
    pd.DataFrame(referee_data).to_csv(referee_path, index=False)
    
    print(f"[ingest_sportmonks] Dados salvos: {output_path}, {team_stats_path}, {player_stats_path}, {transfers_path}, {referee_path}")

if __name__ == "__main__":
    main()