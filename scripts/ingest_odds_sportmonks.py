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
    for attempt in range(3):
        try:
            response = requests.get(full_url, timeout=10)
            if response.status_code == 200:
                return response.json().get('data', [])
            print(f"[ingest_sportmonks] Erro na API (tentativa {attempt+1}): {response.status_code} - {response.text}")
        except Exception as e:
            print(f"[ingest_sportmonks] Erro na chamada API {url} (tentativa {attempt+1}): {e}")
        time.sleep(2)
    return []

def get_brazil_leagues(api_key):
    """Busca IDs das ligas brasileiras (countryId 30 para Brasil)."""
    url = f"{SPORTMONKS_BASE_URL}/leagues"
    params = {'filters': 'countryIds:30'}
    data = get_api_data(url, api_key, params)
    leagues = []
    for league in data:
        name = league.get('name', '').lower()
        if 'serie a' in name or 'serie b' in name or 'serie c' in name or 'copa do brasil' in name:
            league_id = league.get('id')
            leagues.append(league_id)
            print(f"[debug] Liga encontrada: {league['name']} (ID: {league_id})")
    if not leagues:
        print("[ingest_sportmonks] Nenhuma liga brasileira encontrada. Usando IDs padrão.")
        leagues = [71, 72, 73, 76]
    return leagues

def get_season_id_for_year(league_id, year, api_key):
    """Busca o ID da temporada para um ano específico."""
    url = f"{SPORTMONKS_BASE_URL}/seasons/search/{year}"
    params = {'filters': f'leagueIds:{league_id}'}
    data = get_api_data(url, api_key, params)
    if data and isinstance(data, list) and len(data) > 0:
        season_id = data[0]['id']
        print(f"[debug] Season ID para liga {league_id} em {year}: {season_id}")
        return season_id
    print(f"[ingest_sportmonks] Season não encontrada para liga {league_id} em {year}")
    return None

def generate_auto_aliases(api_key, leagues):
    """Gera aliases automáticos usando Sportmonks."""
    aliases = {
        "atletico": "Atlético Mineiro",
        "atletico/mg": "Atlético Mineiro",
        "atletico/go": "Atlético Goianiense",
        "athletic club": "Athletic Club",
        "athletic club mg": "Athletic Club",
        "america": "América Mineiro",
        "america/mg": "América Mineiro",
        "sao paulo": "São Paulo",
        "sao paulo/sp": "São Paulo",
        "vitoria": "Vitória",
        "vitoria/ba": "Vitória",
        "vila nova": "Vila Nova",
        "vila nova/go": "Vila Nova",
        "aston villa": "Aston Villa",
        "manchester city": "Manchester City",
        "palmeiras": "Palmeiras",
        "palmeiras/sp": "Palmeiras",
        "cruzeiro": "Cruzeiro",
        "cruzeiro/mg": "Cruzeiro",
        "ceara": "Ceará",
        "ceara/ce": "Ceará",
        "corinthians": "Corinthians",
        "corinthians/sp": "Corinthians",
        "fluminense": "Fluminense",
        "fluminense/rj": "Fluminense",
        "internacional": "Internacional",
        "internacional/rs": "Internacional",
        "sport": "Sport",
        "sport/pe": "Sport",
        "mirassol": "Mirassol",
        "mirassol/sp": "Mirassol",
        "fortaleza": "Fortaleza",
        "fortaleza/ce": "Fortaleza",
        "flamengo": "Flamengo",
        "flamengo/rj": "Flamengo",
        "botafogo": "Botafogo",
        "botafogo/rj": "Botafogo",
        "santos": "Santos",
        "santos/sp": "Santos",
        "gremio": "Grêmio",
        "gremio/rs": "Grêmio",
        "juventude": "Juventude",
        "juventude/rs": "Juventude",
        "crb": "CRB",
        "crb/al": "CRB",
        "bragantino": "Red Bull Bragantino",
        "bragantino/sp": "Red Bull Bragantino",
        "red bull bragantino": "Red Bull Bragantino",
        "vasco da gama": "Vasco da Gama",
        "vasco da gama/rj": "Vasco da Gama",
        "ferroviaria": "Ferroviária",
        "ferroviaria/sp": "Ferroviária"
    }
    for league_id in leagues:
        url = f"{SPORTMONKS_BASE_URL}/teams"
        params = {'filters': f'leagueIds:{league_id}', 'include': 'name;short_name;alternativeNames'}
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

def load_aliases(aliases_file, api_key, leagues):
    """Carrega ou gera aliases de times."""
    if os.path.exists(aliases_file):
        with open(aliases_file, 'r') as f:
            return json.load(f)
    else:
        print("[ingest_sportmonks] Gerando aliases automáticos via Sportmonks...")
        aliases = generate_auto_aliases(api_key, leagues)
        os.makedirs(os.path.dirname(aliases_file), exist_ok=True)
        with open(aliases_file, 'w') as f:
            json.dump(aliases, f, indent=4)
        return aliases

# Resto do script permanece o mesmo, mas substitua a chamada em main para leagues = get_brazil_leagues(args.api_key) + [8]  # EPL para Aston Villa
# e season_ids = {league: get_season_id_for_year(league, 2025, args.api_key) for league in leagues}
# Para a busca de times, remova o filtro leagueIds em get_team_id_sportmonks e use params = {'filters': 'countryIds:30'} para Brasil, ou sem filtro para EPL.

def get_team_id_sportmonks(team_name, api_key, aliases):
    """Busca ID do time no Sportmonks, usando aliases se necessário."""
    team_name_normalized = normalize_team_name(team_name)
    alias = aliases.get(team_name_normalized.lower(), team_name_normalized)
    url = f"{SPORTMONKS_BASE_URL}/teams/search/{alias}"
    params = {'filters': 'countryIds:30'}  # Restrict to Brazil
    data = get_api_data(url, api_key, params)
    if data and isinstance(data, list) and len(data) > 0:
        team_id = data[0]['id']
        print(f"[debug] Time {team_name} (alias: {alias}) encontrado com ID: {team_id}")
        return team_id
    params = {}  # Try without filter for non-Brazilian teams
    data = get_api_data(url, api_key, params)
    if data and isinstance(data, list) and len(data) > 0:
        team_id = data[0]['id']
        print(f"[debug] Time {team_name} (alias: {alias}) encontrado com ID: {team_id} (sem filtro)")
        return team_id
    print(f"[ingest_sportmonks] Time não encontrado no Sportmonks: {team_name} (alias: {alias})")
    return None

# The rest of the script is the same as your last version.

# In main, use year = 2025 or extract from match_date.

year = datetime.now().year  # Use current year, but since simulation is 2025, it will be 2025
season_ids = {league: get_season_id_for_year(league, year, args.api_key) for league in leagues}