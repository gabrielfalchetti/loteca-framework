# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import requests
import os
import json
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[ingest_odds_apifootball] {msg}", flush=True)

def ingest_odds_apifootball(rodada, source_csv, api_key, regions, aliases_file, api_key_theodds=None):
    if not os.path.isfile(source_csv):
        _log(f"Arquivo {source_csv} não encontrado")
        return

    try:
        matches = pd.read_csv(source_csv)
        _log(f"Conteúdo de {source_csv}:\n{matches.to_string()}")
    except Exception as e:
        _log(f"Erro ao ler {source_csv}: {e}")
        return

    try:
        with open(aliases_file, 'r') as f:
            aliases = json.load(f)
    except Exception as e:
        _log(f"Erro ao ler {aliases_file}: {e}")
        aliases = {}

    # Verificar validade da chave
    try:
        status_url = f"https://v3.football.api-sports.io/status?apiKey={api_key}"
        response = requests.get(status_url, timeout=10)
        _log(f"Resposta do /status: {response.status_code} - {response.text}")
        response.raise_for_status()
        status = response.json()
        _log(f"Status da API-Football: {json.dumps(status, indent=2)}")
    except Exception as e:
        _log(f"Erro ao verificar status da API-Football: {e}")
        _log("Chave API_FOOTBALL_KEY inválida. Usando valores padrão para todos os jogos.")
        odds_data = []
        for _, match in matches.iterrows():
            home_team = match.get('home', match.get('team_home', ''))
            away_team = match.get('away', match.get('team_away', ''))
            odds_data.append({
                'home_team': home_team,
                'away_team': away_team,
                'home_odds': 2.0,
                'draw_odds': 3.0,
                'away_odds': 2.5
            })
        df_odds = pd.DataFrame(odds_data)
        os.makedirs(rodada, exist_ok=True)
        df_odds.to_csv(f"{rodada}/odds_apifootball.csv", index=False)
        _log(f"Odds APIFootball salvos em {rodada}/odds_apifootball.csv")
        return

    odds_data = []
    for _, match in matches.iterrows():
        home_team = match.get('home', match.get('team_home', ''))
        away_team = match.get('away', match.get('team_away', ''))
        norm_home = unidecode(home_team).lower().strip()
        norm_away = unidecode(away_team).lower().strip()
        home_aliases = aliases.get(norm_home, [home_team])
        away_aliases = aliases.get(norm_away, [away_team])

        leagues = [
            {'league': 71, 'season': 2025},  # Série A
            {'league': 72, 'season': 2025},  # Série B
            {'league': 73, 'season': 2025},  # Copa do Brasil
            {'league': 100, 'season': 2025}, # Libertadores
            {'league': 2, 'season': 2025}    # Europa League
        ]
        found = False
        for league in leagues:
            try:
                fixtures_url = f"https://v3.football.api-sports.io/fixtures?league={league['league']}&season={league['season']}&apiKey={api_key}"
                _log(f"Tentando /fixtures para liga {league['league']} e {home_team} x {away_team}")
                response = requests.get(fixtures_url, timeout=10)
                response.raise_for_status()
                fixtures = response.json()
                _log(f"Resposta do /fixtures para liga {league['league']}: {json.dumps(fixtures, indent=2)}")
                for fixture in fixtures.get('response', []):
                    teams = fixture.get('teams', {})
                    if any(h.lower() in unidecode(teams.get('home', {}).get('name', '')).lower() for h in home_aliases) and \
                       any(a.lower() in unidecode(teams.get('away', {}).get('name', '')).lower() for a in away_aliases):
                        # Tentar endpoint /odds
                        odds_url = f"https://v3.football.api-sports.io/odds?league={league['league']}&season={league['season']}&apiKey={api_key}"
                        try:
                            response = requests.get(odds_url, timeout=10)
                            response.raise_for_status()
                            odds = response.json()
                            _log(f"Resposta da API para liga {league['league']}: {json.dumps(odds, indent=2)}")
                            for odd in odds.get('response', []):
                                if any(h.lower() in unidecode(odd.get('fixture', {}).get('teams', {}).get('home', {}).get('name', '')).lower() for h in home_aliases) and \
                                   any(a.lower() in unidecode(odd.get('fixture', {}).get('teams', {}).get('away', {}).get('name', '')).lower() for a in away_aliases):
                                    odds_data.append({
                                        'home_team': home_team,
                                        'away_team': away_team,
                                        'home_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[0].get('odd', 2.0),
                                        'draw_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[1].get('odd', 3.0),
                                        'away_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[2].get('odd', 2.5)
                                    })
                                    _log(f"Odds encontrados na liga {league['league']} para {home_team} x {away_team}")
                                    found = True
                                    break
                            if found:
                                break
                        except Exception as e:
                            _log(f"Erro na liga {league['league']} para {home_team} x {away_team}: {e}")
                            odds_data.append({
                                'home_team': home_team,
                                'away_team': away_team,
                                'home_odds': 2.0,
                                'draw_odds': 3.0,
                                'away_odds': 2.5
                            })
                            found = True
                            break
                if found:
                    break
            except Exception as e:
                _log(f"Erro no /fixtures para liga {league['league']} e {home_team} x {away_team}: {e}")
        if found:
            break
   if not found:
       _log(f"Nenhuma odds encontrada para {home_team} x {away_team}, usando valores padrão")
       odds_data.append({
           'home_team': home_team,
           'away_team': away_team,
           'home_odds': 2.0,
           'draw_odds': 3.0,
           'away_odds': 2.5
       })