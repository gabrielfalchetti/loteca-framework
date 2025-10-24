import pandas as pd
import requests
import unidecode
import os
from datetime import datetime, timedelta

# Configurações da API Sportmonks (substitua pelo seu token)
SPORTMONKS_API_TOKEN = os.getenv('SPORTMONKS_API_TOKEN', 'your_api_token_here')
BASE_URL = 'https://api.sportmonks.com/v3/football'

def normalize_team_name(name):
    """Remove estado, acentos e padroniza nome do time."""
    if '/' in name:
        name = name.split('/')[0].strip()
    name = unidecode.unidecode(name.lower())
    return name.capitalize()

def get_team_id(team_name):
    """Busca ID do time no Sportmonks."""
    url = f"{BASE_URL}/teams/search/{normalize_team_name(team_name)}?api_token={SPORTMONKS_API_TOKEN}"
    response = requests.get(url)
    if response.status_code == 200 and response.json()['data']:
        return response.json()['data'][0]['id']
    return None

def get_fixtures(league_id, date_from, date_to, home_team_id, away_team_id):
    """Busca fixtures para uma liga e filtra por times."""
    page = 1
    fixtures = []
    while True:
        url = (f"{BASE_URL}/fixtures?league={league_id}&date_from={date_from}&date_to={date_to}"
               f"&page={page}&api_token={SPORTMONKS_API_TOKEN}&include=participants")
        response = requests.get(url)
        if response.status_code != 200:
            print(f"[ingest_odds_sportmonks] Erro na busca de fixtures: {response.status_code}")
            break
        data = response.json()
        fixtures.extend(data.get('data', []))
        print(f"[ingest_odds_sportmonks] Resposta de fixtures para liga {league_id}: {len(data.get('data', []))} jogos encontrados")
        if not data.get('pagination', {}).get('has_more', False):
            break
        page += 1
    
    # Filtrar por home e away team IDs
    for fixture in fixtures:
        home_id = fixture.get('participants', [{}])[0].get('id')
        away_id = fixture.get('participants', [{}])[1].get('id')
        if home_id == home_team_id and away_id == away_team_id:
            return fixture
    return None

def get_odds(fixture_id):
    """Busca odds pre-match para um fixture."""
    url = f"{BASE_URL}/odds/pre-match/by-fixture/{fixture_id}?api_token={SPORTMONKS_API_TOKEN}&include=bookmakers"
    response = requests.get(url)
    if response.status_code == 200 and response.json()['data']:
        odds_data = response.json()['data']
        # Assumindo 1x2 market (ID=1, exemplo comum)
        for odd in odds_data:
            if odd.get('market_id') == 1:  # 1x2
                values = {v['name']: v['value'] for v in odd.get('values', [])}
                return {
                    'home_odds': values.get('1', 2.0),  # Default 2.0 se não encontrado
                    'draw_odds': values.get('X', 3.0),
                    'away_odds': values.get('2', 2.0)
                }
    return {'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}  # Defaults

def main():
    # Configurações do workflow
    output_path = os.getenv('CONSENSUS_CSV', 'data/out/odds_sportmonks.csv')
    look_ahead_days = int(os.getenv('LOOKAHEAD_DAYS', 3))
    date_from = datetime.now().strftime('%Y-%m-%d')
    date_to = (datetime.now() + timedelta(days=look_ahead_days)).strftime('%Y-%m-%d')
    leagues = [71, 72, 73]  # Série A, B, C

    # Jogos da Loteca (exemplo do log)
    matches = [
        ('palmeiras/sp', 'cruzeiro/mg'),
        ('atletico/mg', 'ceara/ce'),
        ('vitoria/ba', 'corinthians/sp'),
        ('athletic club/mg', 'america/mg'),
        ('fluminense/rj', 'internacional/rs'),
        ('sport/pe', 'mirassol/sp'),
        ('fortaleza/ce', 'flamengo/rj'),
        ('sao paulo/sp', 'bahia/ba'),
        ('aston villa', 'manchester city'),  # Premier League
        ('botafogo/rj', 'santos/sp'),
        ('gremio/rs', 'juventude/rs'),
        ('crb/al', 'atletico/go'),
        ('bragantino/sp', 'vasco da gama/rj'),
        ('vila nova/go', 'ferroviaria/sp')
    ]

    # DataFrame para armazenar odds
    odds_data = []
    
    for home_team, away_team in matches:
        home_team_id = get_team_id(home_team)
        away_team_id = get_team_id(away_team)
        if not home_team_id or not away_team_id:
            print(f"[ingest_odds_sportmonks] Time não encontrado: {home_team} ou {away_team}")
            continue

        found = False
        for league_id in leagues:
            fixture = get_fixtures(league_id, date_from, date_to, home_team_id, away_team_id)
            if fixture:
                fixture_id = fixture['id']
                odds = get_odds(fixture_id)
                odds_data.append({
                    'match_id': fixture_id,  # Usa fixture_id como match_id
                    'home_team': normalize_team_name(home_team),
                    'away_team': normalize_team_name(away_team),
                    'home_odds': odds['home_odds'],
                    'draw_odds': odds['draw_odds'],
                    'away_odds': odds['away_odds'],
                    'league_id': league_id
                })
                found = True
                print(f"[ingest_odds_sportmonks] Odds encontradas para {home_team} x {away_team} na liga {league_id}")
                break
        
        if not found:
            print(f"[ingest_odds_sportmonks] Nenhuma odds encontrada para {home_team} x {away_team}, usando valores padrão")
            odds_data.append({
                'match_id': f"{normalize_team_name(home_team)}_vs_{normalize_team_name(away_team)}",
                'home_team': normalize_team_name(home_team),
                'away_team': normalize_team_name(away_team),
                'home_odds': 2.0,
                'draw_odds': 3.0,
                'away_odds': 2.0,
                'league_id': None
            })

    # Salvar CSV
    df = pd.DataFrame(odds_data)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[ingest_odds_sportmonks] Odds Sportmonks salvos em {output_path}")

if __name__ == "__main__":
    main()