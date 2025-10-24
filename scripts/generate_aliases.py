import argparse
import json
import os
import requests
import unidecode
import time

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
            print(f"[generate_aliases] Erro na API (tentativa {attempt+1}): {response.status_code} - {response.text}")
        except Exception as e:
            print(f"[generate_aliases] Erro na chamada API {url} (tentativa {attempt+1}): {e}")
        time.sleep(2)
    return []

def generate_auto_aliases(api_key, leagues=[71, 72, 73, 76, 8]):
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

def main():
    parser = argparse.ArgumentParser(description='Generate automatic aliases for teams using Sportmonks API.')
    parser.add_argument('--api_key', required=True, help='Sportmonks API key')
    parser.add_argument('--aliases_file', required=True, help='Path to save auto_aliases.json')
    
    args = parser.parse_args()
    
    aliases = generate_auto_aliases(args.api_key)
    os.makedirs(os.path.dirname(args.aliases_file), exist_ok=True)
    with open(args.aliases_file, 'w') as f:
        json.dump(aliases, f, indent=4)
    print(f"[generate_aliases] Aliases salvos em {args.aliases_file}")

if __name__ == "__main__":
    main()