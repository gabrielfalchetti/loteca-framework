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

def get_leagues_for_country(api_key, country_name):
    """Busca IDs de ligas para um país específico."""
    url = f"{SPORTMONKS_BASE_URL}/leagues/search/{country_name}"
    params = {'include': 'country;seasons'}
    data = get_api_data(url, api_key, params)
    leagues = []
    for league in data:
        if league.get('country', {}).get('name', '').lower() == country_name.lower():
            season_id = None
            if league.get('seasons'):
                season_id = league['seasons'][-1].get('id') if league['seasons'][-1].get('id') else None
            leagues.append({
                'id': league.get('id'),
                'name': league.get('name'),
                'season_id': season_id
            })
    print(f"[debug] Ligas encontradas para {country_name}: {leagues}")
    return leagues

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
    for league_id in [league['id'] for league in leagues]:
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

def get_team_id_sportmonks(team_name, api_key, aliases, leagues):
    """Busca ID do time no Sportmonks, usando aliases se necessário, filtrando por ligas."""
    team_name_normalized = normalize_team_name(team_name)
    alias = aliases.get(team_name_normalized.lower(), team_name_normalized)
    for league in leagues:
        league_id = league['id']
        url = f"{SPORTMONKS_BASE_URL}/teams/search/{alias}"
        params = {'filters': f'leagueIds:{league_id}'}
        data = get_api_data(url, api_key, params)
        if data and isinstance(data, list) and len(data) > 0:
            team_id = data[0]['id']
            print(f"[debug] Time {team_name} (alias: {alias}) encontrado com ID: {team_id} na liga {league_id} ({league['name']})")
            return team_id
        print(f"[debug] Time {team_name} (alias: {alias}) não encontrado na liga {league_id} ({league['name']})")
    print(f"[ingest_sportmonks] Time não encontrado no Sportmonks: {team_name} (alias: {alias})")
    return None

def fetch_matches_sportmonks(league_id, league_name, date_from, date_to, home_team_id, away_team_id, api_key):
    """Busca partidas na API restritas a dois times específicos."""
    page = 1
    matches = []
    team_ids_str = f"{home_team_id},{away_team_id}"
    while True:
        url = f"{SPORTMONKS_BASE_URL}/fixtures/between/{date_from}/{date_to}"
        params = {
            'filters': f'leagueIds:{league_id};participantIds:{team_ids_str}',
            'page': page,
            'include': 'participants;league'
        }
        data = get_api_data(url, api_key, params)
        print(f"[debug] Resposta da API para league_id {league_id} ({league_name}), teams {team_ids_str}: {json.dumps(data, indent=2)}")
        if not data:
            break
        for fixture in data:
            participants = fixture.get('participants', [{}])
            if len(participants) >= 2:
                fixture_home_id = participants[0].get('id')
                fixture_away_id = participants[1].get('id')
                # Restrict to only matches where both teams are the exact pair
                if set([fixture_home_id, fixture_away_id]) == set([home_team_id, away_team_id]):
                    match = {
                        'fixture_id': fixture.get('id'),
                        'home_team_id': fixture_home_id,
                        'home_team_name': normalize_team_name(participants[0].get('name', '')),
                        'away_team_id': fixture_away_id,
                        'away_team_name': normalize_team_name(participants[1].get('name', '')),
                        'date': fixture.get('starting_at', '').split(' ')[0],
                        'league_id': league_id
                    }
                    matches.append(match)
                    print(f"[debug] Partida encontrada na API: {match['home_team_name']} x {match['away_team_name']} (league_id: {league_id}, date: {match['date']})")
        if len(data) < 100:  # Assume 100 por página
            break
        page += 1
        time.sleep(1)  # Respeitar rate limits
    return matches

def match_fixtures(csv_row, api_matches, aliases):
    """Compara uma partida do CSV com partidas da API, retornando a correspondência mais próxima."""
    home_team = normalize_team_name(csv_row['home'])
    away_team = normalize_team_name(csv_row['away'])
    csv_date = csv_row['date']
    
    try:
        csv_date_obj = datetime.strptime(csv_date, '%Y-%m-%d')
        date_window = [csv_date_obj + timedelta(days=x) for x in [-1, 0, 1]]
        date_window_str = [d.strftime('%Y-%m-%d') for d in date_window]
    except ValueError:
        print(f"[ingest_sportmonks] Data inválida no CSV para {home_team} x {away_team}: {csv_date}")
        return None, False
    
    home_alias = aliases.get(home_team.lower(), home_team)
    away_alias = aliases.get(away_team.lower(), away_team)
    
    for api_match in api_matches:
        api_home = normalize_team_name(api_match['home_team_name'])
        api_away = normalize_team_name(api_match['away_team_name'])
        api_date = api_match['date']
        
        # Verifica correspondência de times (considerando aliases) e data na janela
        if (api_home.lower() in [home_alias.lower(), home_team.lower()] and
            api_away.lower() in [away_alias.lower(), away_team.lower()] and
            api_date in date_window_str):
            print(f"[debug] Correspondência encontrada: {home_team} x {away_team} (CSV) com {api_home} x {api_away} (API) em {api_date}")
            return api_match, False
        elif (api_home.lower() in [away_alias.lower(), away_team.lower()] and
              api_away.lower() in [home_alias.lower(), home_team.lower()] and
              api_date in date_window_str):
            print(f"[debug] Correspondência invertida encontrada: {home_team} x {away_team} (CSV) com {api_home} x {api_away} (API) em {api_date}")
            return api_match, True
    
    print(f"[ingest_sportmonks] Nenhuma correspondência encontrada para {home_team} x {away_team} em {csv_date}")
    return None, False

def get_odds_sportmonks(fixture_id, api_key):
    """Busca odds pre-match para um fixture."""
    url = f"{SPORTMONKS_BASE_URL}/odds/pre-match/by-fixture/{fixture_id}"
    params = {'include': 'bookmakers'}
    data = get_api_data(url, api_key, params)
    print(f"[debug] Resposta da API para odds do fixture {fixture_id}: {json.dumps(data, indent=2)}")
    for odd in data:
        if odd.get('market_id') == 1:  # 1x2 market
            values = {v['name']: v['value'] for v in odd.get('values', [])}
            odds = {
                'match_id': fixture_id,
                'home_odds': float(values.get('1', 2.0)),
                'draw_odds': float(values.get('X', 3.0)),
                'away_odds': float(values.get('2', 2.0))
            }
            print(f"[debug] Odds encontradas para fixture {fixture_id}: {odds}")
            return odds
    print(f"[ingest_sportmonks] Nenhuma odds encontrada para fixture {fixture_id}")
    return {'match_id': fixture_id, 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}

def get_team_stats(team_id, season_id, api_key):
    """Extrai estatísticas de time."""
    url = f"{SPORTMONKS_BASE_URL}/teams/{team_id}"
    params = {'include': f'statistics;season:{season_id}'}
    data = get_api_data(url, api_key, params)
    stats = {
        'team_id': team_id,
        'goals_scored_avg': 0.0,
        'goals_conceded_avg': 0.0,
        'win_rate': 0.0
    }
    if isinstance(data, dict):
        statistics = data.get('statistics', [])
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
    if isinstance(data, dict):
        lineups = data.get('lineups', [])
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
    if isinstance(data, dict):
        referees = data.get('referees', [])
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
    parser.add_argument('--source_csv', required=True, help='Path to matches_source.csv')
    parser.add_argument('--api_key', required=True, help='Sportmonks API key')
    parser.add_argument('--aliases_file', required=True, help='Path to auto_aliases.json')
    parser.add_argument('--lookahead_days', type=int, default=3, help='Days to look ahead for fixtures')
    
    args = parser.parse_args()
    
    # Configurações
    output_path = os.path.join(args.rodada, 'odds_consensus.csv')
    team_stats_path = os.path.join(args.rodada, 'team_stats.csv')
    player_stats_path = os.path.join(args.rodada, 'player_stats.csv')
    transfers_path = os.path.join(args.rodada, 'transfers.csv')
    referee_path = os.path.join(args.rodada, 'referee_stats.csv')
    
    # Fetch leagues for Brazil and EPL (for Aston Villa vs Manchester City)
    brazil_leagues = get_leagues_for_country(args.api_key, 'Brazil')
    epl_leagues = get_leagues_for_country(args.api_key, 'England')
    leagues = brazil_leagues + [league for league in epl_leagues if league['name'] == 'Premier League']
    if not leagues:
        print("[error] Nenhuma liga encontrada na API (Brasil ou EPL). Verifique a chave API ou a cobertura.")
        leagues = [{'id': 71, 'name': 'Fallback Serie A', 'season_id': None}]  # Fallback para evitar falha
    
    season_ids = {league['id']: league['season_id'] or get_current_season_id(league['id'], args.api_key) for league in leagues}
    print(f"[debug] Season IDs: {season_ids}")
    aliases = load_aliases(args.aliases_file, args.api_key, leagues)
    
    # Ler matches_source.csv
    if not os.path.exists(args.source_csv):
        raise FileNotFoundError(f"Arquivo {args.source_csv} não encontrado")
    matches_df = pd.read_csv(args.source_csv)
    print(f"[ingest_sportmonks] Colunas em {args.source_csv}: {matches_df.columns.tolist()}")
    print(f"[ingest_sportmonks] Partidas em matches_source.csv:\n{matches_df.to_string()}")
    
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
        match_date = row['date']
        match_id = row['match_id']
        
        home_team_id = get_team_id_sportmonks(home_team, args.api_key, aliases, leagues)
        away_team_id = get_team_id_sportmonks(away_team, args.api_key, aliases, leagues)
        
        if not home_team_id or not away_team_id:
            print(f"[ingest_sportmonks] Time não encontrado para {home_team} x {away_team}, usando odds padrão")
            odds = {'match_id': match_id, 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
            continue
        
        # Stats de times
        for league in leagues:
            league_id = league['id']
            season_id = season_ids.get(league_id)
            if season_id:
                team_stats_data.append(get_team_stats(home_team_id, season_id, args.api_key))
                team_stats_data.append(get_team_stats(away_team_id, season_id, args.api_key))
        
        # Transferências
        transfers_data.extend(get_transfers(home_team_id, args.api_key))
        transfers_data.extend(get_transfers(away_team_id, args.api_key))
        
        # Buscar partidas específicas para esta dupla de times
        api_matches = []
        try:
            date_from = (datetime.strptime(match_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            date_to = (datetime.strptime(match_date, '%Y-%m-%d') + timedelta(days=args.lookahead_days)).strftime('%Y-%m-%d')
            for league in leagues:
                league_id = league['id']
                league_name = league['name']
                matches = fetch_matches_sportmonks(league_id, league_name, date_from, date_to, home_team_id, away_team_id, args.api_key)
                api_matches.extend(matches)
        except ValueError:
            print(f"[ingest_sportmonks] Data inválida para {home_team} x {away_team}: {match_date}, usando odds padrão")
            odds = {'match_id': match_id, 'home_odds': 2.0, 'draw_odds': 3.0, 'away_odds': 2.0}
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = None
            odds_data.append(odds)
            continue
        
        # Tentar casar a partida do CSV com as partidas da API
        api_match, inverted = match_fixtures(row, api_matches, aliases)
        if api_match:
            fixture_id = api_match['fixture_id']
            odds = get_odds_sportmonks(fixture_id, args.api_key)
            odds['home_team'] = normalize_team_name(home_team)
            odds['away_team'] = normalize_team_name(away_team)
            odds['league_id'] = api_match['league_id']
            odds['match_id'] = match_id
            if inverted:
                temp = odds['home_odds']
                odds['home_odds'] = odds['away_odds']
                odds['away_odds'] = temp
                print(f"[debug] Odds trocadas devido a fixture invertido para {home_team} x {away_team}")
            odds_data.append(odds)
            
            player_stats_data.extend(get_player_stats(fixture_id, args.api_key))
            
            referee_data.append(get_referee_stats(fixture_id, args.api_key))
            
            print(f"[ingest_sportmonks] Dados completos encontrados para {home_team} x {away_team} na liga {api_match['league_id']}")
        else:
            print(f"[ingest_sportmonks] Nenhuma partida encontrada na API para {home_team} x {away_team} em {match_date}, usando odds padrão")
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