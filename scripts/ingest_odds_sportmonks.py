import pandas as pd
import requests
import unidecode
import os
import json
from datetime import datetime, timedelta
import argparse
import time  # Para delays entre chamadas API

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

def get_api_data(url, api_key, params=None):
    """Função genérica para chamar API Sportmonks com retry simples."""
    full_url = f"{url}?api_token={api_key}"
    if params:
        full_url += '&' + '&'.join([f"{k}={v}" for k,v in params.items()])
    try:
        response = requests.get(full_url, timeout=10)
        if response.status_code == 200:
            return response.json().get('data', [])
        print(f"[ingest_sportmonks] Erro na API: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[ingest_sportmonks] Erro na chamada API: {e}")
    return []

# Funções existentes (get_team_id_sportmonks, get_fixtures_sportmonks, get_odds_sportmonks, get_odds_theodds) permanecem as mesmas

def get_team_stats(team_id, season_id, api_key):
    """Extrai stats de time."""
    url = f"{SPORTMONKS_BASE_URL}/statistics/teams/{team_id}"
    params = {'season': season_id, 'include': 'types'}  # Ex.: win rate, goals average
    return get_api_data(url, api_key, params)

def get_player_stats(player_id, api_key):
    """Extrai stats de jogador."""
    url = f"{SPORTMONKS_BASE_URL}/statistics/players/{player_id}"
    params = {'include': 'types;injury'}  # Stats + lesões
    return get_api_data(url, api_key, params)

def get_transfers(team_id, api_key):
    """Extrai transferências recentes."""
    url = f"{SPORTMONKS_BASE_URL}/transfers"
    params = {'team': team_id, 'latest': True, 'include': 'player;fromTeam;toTeam'}
    return get_api_data(url, api_key, params)

def get_referee_stats(fixture_id, api_key):
    """Extrai stats de árbitro para um fixture."""
    url = f"{SPORTMONKS_BASE_URL}/referees"  # Ou linke via fixture include=referee
    params = {'include': 'statistics'}  # Cartões médios, etc.
    return get_api_data(url, api_key, params)

def main():
    parser = argparse.ArgumentParser(description='Ingest maximum data from Sportmonks for predictions.')
    # Argumentos existentes...
    args = parser.parse_args()
    
    # Configurações existentes...
    season_id = 19735  # Exemplo para Brasileirão 2025; ajuste dinamicamente via /seasons
    leagues = [71, 72, 73]  # Série A, B, C
    
    # Ler matches_norm.csv e validar (como antes)
    matches_df = pd.read_csv(args.source_csv)
    # ... validação
    
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
        
        # Extração existente de fixtures e odds...
        # (código original aqui)
        
        # Nova extração: Stats de times
        home_stats = get_team_stats(home_team_id, season_id, args.api_key)
        away_stats = get_team_stats(away_team_id, season_id, args.api_key)
        team_stats_data.extend([{'team_id': home_team_id, 'stats': home_stats}, {'team_id': away_team_id, 'stats': away_stats}])
        
        # Stats de jogadores (ex.: top 11 por lineup preditivo)
        fixture = get_fixtures_sportmonks(...)  # Do código original
        if fixture:
            params = {'include': 'lineups.players.statistics'}  # Lineups com stats
            lineup_data = get_api_data(f"{SPORTMONKS_BASE_URL}/fixtures/{fixture['id']}", args.api_key, params)
            for player in lineup_data.get('lineups', []).get('players', []):
                player_stats = get_player_stats(player['id'], args.api_key)
                player_stats_data.append({'player_id': player['id'], 'stats': player_stats})
        
        # Transferências
        home_transfers = get_transfers(home_team_id, args.api_key)
        away_transfers = get_transfers(away_team_id, args.api_key)
        transfers_data.extend(home_transfers + away_transfers)
        
        # Árbitros (se disponível no fixture)
        if fixture:
            referee_stats = get_referee_stats(fixture['id'], args.api_key)
            referee_data.append({'fixture_id': fixture['id'], 'referee_stats': referee_stats})
        
        time.sleep(1)  # Delay para rate limits
        
    # Salvar dados existentes (odds_consensus.csv)
    # ...
    
    # Salvar novos dados
    pd.DataFrame(team_stats_data).to_csv(os.path.join(args.rodada, 'team_stats.csv'), index=False)
    pd.DataFrame(player_stats_data).to_csv(os.path.join(args.rodada, 'player_stats.csv'), index=False)
    pd.DataFrame(transfers_data).to_csv(os.path.join(args.rodada, 'transfers.csv'), index=False)
    pd.DataFrame(referee_data).to_csv(os.path.join(args.rodada, 'referee_stats.csv'), index=False)
    
    print("[ingest_sportmonks] Dados extras salvas: team_stats, player_stats, transfers, referee_stats")

if __name__ == "__main__":
    main()