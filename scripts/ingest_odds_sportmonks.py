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

def generate_auto_aliases(api_key, leagues=[71, 72, 73, 8]):
    """Gera aliases automáticos usando Sportmonks."""
    aliases = {
        "atletico": "Atlético Mineiro",
        "athletic club": "Athletic Club MG",
        "america": "América Mineiro",
        "sao paulo": "São Paulo",
        "vitoria": "Vitória",
        "vila nova": "Vila Nova",
        "aston villa": "Aston Villa",
        "manchester city": "Manchester City",
        "palmeiras": "Palmeiras",
        "cruzeiro": "Cruzeiro",
        "ceara": "Ceará",
        "corinthians": "Corinthians",
        "fluminense": "Fluminense",
        "internacional": "Internacional",
        "sport": "Sport",
        "mirassol": "Mirassol",
        "fortaleza": "Fortaleza",
        "flamengo": "Flamengo",
        "botafogo": "Botafogo",
        "santos": "Santos",
        "gremio": "Grêmio",
        "juventude": "Juventude",
        "crb": "CRB",
        "bragantino": "Red Bull Bragantino",
        "vasco da gama": "Vasco da Gama",
        "ferroviaria": "Ferroviária"
    }
    for league_id in leagues:
        url = f"{SPORTMONKS_BASE_URL}/teams"
        params = {'league': league_id, 'include': 'name;short_name;alternativeNames'}
        data = get_api_data(url, api_key, params)
        for team in data:
            name = normalize_team_name(team.get('name', ''))
            short_name = normalize_team_name(team.get('short_name', ''))
            alt_names = [normalize_team_name(alt) for alt in team.get('alternativeNames', []) or []]
            if name:
                aliases[name.lower()] = name
            if short_name:
                aliases[short_name.lower()] = name
            for alt in alt_names:
                if alt:
                    aliases[alt.lower()] = name
        time.sleep(1)  # Respeitar rate limits
    return aliases

def load_aliases(aliases_file, api_key):
    """Carrega ou gera aliases de times."""
    if os.path.exists(aliases_file):
        with open(aliases_file, 'r') as f:
            return json.load(f)
    else:
        print("[ingest_sportmonks] Gerando aliases automáticos via Sportmonks...")
        aliases = generate_auto_aliases(api_key)
        os.makedirs(os.path.dirname(aliases_file), exist_ok=True)
        with open(aliases_file, 'w') as f:
            json.dump(aliases, f, indent=4)
        return aliases

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

def get_fixtures_sportmonks(league_id, date_from, date_to, home_team_id, away_team_id, api_key):
    """Busca fixtures por liga e filtra por times."""
    page = 1
    fixtures = []
    while True:
        url = f"{SPORTMONKS_BASE_URL}/fixtures/between/{date_from}/{date_to}"
        params = {
            'filters': f'leagueIds:{league_id}',
            'page': page,
            'include': 'participants;weatherReport;referees;venue'
        }
        data = get_api_data(url, api_key, params)
        fixtures.extend(data)
        print(f"[ingest_sportmonks] Resposta de fixtures para liga {league_id} de {date_from} a {date_to}: {len(data)} jogos encontrados")
        if not data or len(data) < 100:  # Assume 100 por página
            break
        page += 1
        time.sleep(1)  # Respeitar rate limits
    
    for fixture in fixtures:
        participants = fixture.get('participants', [{}])
        if len(participants) >= 2:
            home_id = participants[0].get('id')
            away_id = participants[1].get('id')
            print(f"[debug] Fixture: {fixture.get('name')} - home_id = {home_id} ({participants[0].get('name')}), away_id = {away_id} ({participants[1].get('name')})")
            if home_id == home_team_id and away_id == away_team_id:
                return fixture, False  # Not inverted
            elif home_id == away_team_id and away_id == home_team_id:
                return fixture, True  # Inverted
    return None, False

def get_odds_sportmonks(fixture_id, api_key, inverted=False):
    """Busca odds pre-match para um fixture."""
    url = f"{SPORTMONKS_BASE_URL}/odds/pre-match/by-fixture/{fixture_id}"
    params = {'include': 'bookmakers'}
    data = get_api_data(url, api_key, params)
    for odd in data:
        if odd.get('market_id') == 1:  # 1x2 market
            values = {v['name']: v['value'] for v in odd.get('values', [])}
            odds = {
                'match_id': fixture_id,
                'home_odds': float(values.get('1', 2.0)),
                'draw_odds': float(values.get('X', 3.0)),
                'away_odds': float(values.get('2', 2.0))
            }
            if inverted:
                # Swap home and away odds
                temp = odds['home_odds']
                odds['home_odds'] = odds['away_odds']
                odds['away_odds'] = temp
                print(f"[debug] Fixture invertido, trocando odds home/away")
            return odds
    print(f"[ingest_sportmonks] Nenhuma odds encontrada para fixture {fixture_id}")
    return {'match_id': fixture_id, 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}

def get_team_stats(team_id, season_id, api_key):
    """Extrai estatísticas de time."""
    url = f"{SPORTMONKS_BASE_URL}/teams/{team_id}"
    params = {'include': 'statistics', 'season': season_id}
    data = get_api_data(url, api_key, params)
    stats = {
        'team_id': team_id,
        'goals_scored