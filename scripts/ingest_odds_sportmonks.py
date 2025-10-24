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
    """Busca fixtures por liga e filtra restritivamente por times."""
    page = 1
    fixtures = []
    while True:
        url = f"{SPORTMONKS_BASE_URL}/fixtures/between/{date_from}/{date_to}"
        params = {
            'filters': f'leagueIds:{league_id};participantIds:{home_team_id},{away_team_id}',
            'page': page,
            'include': 'participants;weatherReport;referees;venue'
        }
        data = get_api_data(url, api_key, params)
        fixtures.extend(data)
        print(f"[ingest_sportmonks] Resposta de fixtures para liga {league_id}, times {home_team_id},{away_team_id}: {len(data)} jogos encontrados")
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
                print(f"[debug] Fixture invertido encontrado para {home_team_id} x {away_team_id}")
                return fixture, True  # Inverted
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

def get_team_stats(team_id, season_id, api_key):
    """Extrai estatísticas de time."""
    url = f"{SPORTMONKS_BASE_URL}/teams/{team_id}"
    params = {'include': 'statistics', 'season': season_id}
    data = get_api_data(url, api_key, params)
    stats = {
        'team_id': team_id,
        'goals_scored_avg': 0.0,
        'goals_conceded_avg': 0.0,
        'win_rate': 0.0
    }
    if isinstance(data, list) and data:
        data = data[0]
    statistics = data.get('statistics', []) if isinstance(data, dict) else []
    for stat in statistics:
        if stat.get('type_id') == 42:  # Gols marcados por jogo
            stats['goals_scored_avg'] = stat.get('value', {}).get('average', 0.0)
        elif stat.get('type_id') == 43:  # Gols sofridos por jogo
            stats['goals_conceded_avg'] = stat.get('value', {}).get('average', 0.0)
        elif stat.get('type_id') == 44:  # Taxa de vitórias
            stats['win_rate'] = stat.get('value', {}).get('percentage', 0.0)
    return stats

def get_player_stats(fixture_id, api_key):
    """Extrai estatísticas de jogadores do lineup de um fixture."""
    url = f"{SPORTMONKS_BASE_URL}/fixtures/{fixture_id}"
    params = {'include': 'lineups;players;statistics;players.injury'}
    data = get_api_data(url, api_key, params)
    players = []
    if isinstance(data, list) and data:
        data = data[0]
    lineups = data.get('lineups', []) if isinstance(data, dict) else []
    for lineup in lineups:
        for player in lineup.get('players', []):
            stats = player.get('statistics', {})
            injury = player.get('injury', {})
            players.append({
                'player_id': player.get('id', 0),
                'goals': stats.get('goals', 0),
                'assists': stats.get('assists', 0),
                'minutes_played': stats.get('minutes', 0),
                'is_injured': injury.get('is_active', False)
            })
    return players

def get_transfers(team_id, api_key):
    """Extrai transferências recentes."""
    url = f"{SPORTMONKS_BASE_URL}/transfers"
    params = {'filters': f'teamIds:{team_id}', 'include': 'player;fromTeam;toTeam'}
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
    params = {'include': 'referees;referees.statistics'}
    data = get_api_data(url, api_key, params)
    if isinstance(data, list) and data:
        data = data[0]
    referees = data.get('referees', []) if isinstance(data, dict) else []
    if referees:
        referee = referees[0]
        return {
            'referee_id': referee.get('id'),
            'yellow_cards_avg': referee.get('statistics', {}).get('yellow_cards', {}).get('average', 0.0),
            'red_cards_avg': referee.get('statistics', {}).get('red_cards', {}).get('average', 0.0)
        }
    return {'referee_id': None, 'yellow_cards_avg': 0.0, 'red_cards_avg': 0.0}

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
    
    # Ligas
    leagues = [71, 72, 73, 8]  # Série A, B, C, EPL
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
    
    # Validar formato das datas
    for date in matches_df['date']:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            print(f"[ingest_sportmonks] Formato de data inválido no CSV: {date}")
    
    odds_data = []
    team_stats_data = []
    player_stats_data = []
    transfers_data = []
    referee_data = []
    
    for _, row in matches_df.iterrows():
        home_team = row['home']
        away_team = row['away']
        match_date = row['date']
        home_team_id = get_team_id_sportmonks(home_team, args.api_key, aliases)
        away_team_id = get_team_id_sportmonks(away_team, args.api_key, aliases)
        
        if not home_team_id or not away_team_id:
            print(f"[ingest_sportmonks] Time não encontrado para {home_team} x {away_team}, usando odds padrão")
            odds = {'match_id': f"{home_team}_vs_{away_team}", 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
            continue
        
        # Stats de times (usar season_id da liga correspondente ou default)
        for league_id in leagues:
            season_id = season_ids.get(league_id, 19735)  # Default season_id
            team_stats_data.append(get_team_stats(home_team_id, season_id, args.api_key))
            team_stats_data.append(get_team_stats(away_team_id, season_id, args.api_key))
        
        # Transferências
        transfers_data.extend(get_transfers(home_team_id, args.api_key))
        transfers_data.extend(get_transfers(away_team_id, args.api_key))
        
        found = False
        try:
            date_from = datetime.strptime(match_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            date_to = (datetime.strptime(match_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        except ValueError:
            print(f"[ingest_sportmonks] Data inválida para {home_team} x {away_team}: {match_date}, usando odds padrão")
            odds = {'match_id': f"{home_team}_vs_{away_team}", 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
            continue
        
        for league_id in leagues:
            fixture, inverted = get_fixtures_sportmonks(league_id, date_from, date_to, home_team_id, away_team_id, args.api_key)
            if fixture:
                odds = get_odds_sportmonks(fixture['id'], args.api_key)
                odds['home_team'] = normalize_team_name(home_team)
                odds['away_team'] = normalize_team_name(away_team)
                odds['league_id'] = league_id
                if inverted:
                    # Trocar home e away odds se invertido
                    temp = odds['home_odds']
                    odds['home_odds'] = odds['away_odds']
                    odds['away_odds'] = temp
                    print(f"[debug] Odds trocadas devido a fixture invertido para {home_team} x {away_team}")
                odds_data.append(odds)
                
                player_stats_data.extend(get_player_stats(fixture['id'], args.api_key))
                
                referee_data.append(get_referee_stats(fixture['id'], args.api_key))
                
                found = True
                print(f"[ingest_sportmonks] Dados completos encontrados para {home_team} x {away_team} na liga {league_id}")
                break
        
        if not found:
            print(f"[ingest_sportmonks] Nenhuma odds encontrada para {home_team} x {away_team}, usando odds padrão")
            odds = {'match_id': f"{home_team}_vs_{away_team}", 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}
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