# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import requests
import os
import json
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[ingest_odds_sportmonks] {msg}", flush=True)

def ingest_odds_sportmonks(rodada, source_csv, api_key, regions, aliases_file, api_key_theodds=None):
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
        status_url = f"https://api.sportmonks.com/v3/core/status?api_token={api_key}"
        response = requests.get(status_url, timeout=10)
        _log(f"Resposta do /status: {response.status_code} - {response.text}")
        response.raise_for_status()
        status = response.json()
        _log(f"Status do Sportmonks: {json.dumps(status, indent=2)}")
    except Exception as e:
        _log(f"Erro ao verificar status do Sportmonks: {e}")
        _log("Chave SPORTMONKS_API_KEY inválida. Usando valores padrão para todos os jogos.")
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
        df_odds.to_csv(f"{rodada}/odds_sportmonks.csv", index=False)
        _log(f"Odds Sportmonks salvos em {rodada}/odds_sportmonks.csv")
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
            {'league': 71, 'name': 'Série A'},  # Série A
            {'league': 72, 'name': 'Série B'},  # Série B
            {'league': 73, 'name': 'Copa do Brasil'},  # Copa do Brasil
            {'league': 100, 'name': 'Libertadores'},  # Libertadores
            {'league': 2, 'name': 'Europa League'}  # Europa League
        ]
        found = False
        for league in leagues:
            try:
                fixtures_url = f"https://api.sportmonks.com/v3/football/fixtures?api_token={api_key}&timezone=America/Sao_Paulo&filters=leagues:{league['league']}&include=participants;odds"
                _log(f"Tentando /fixtures para liga {league['name']} e {home_team} x {away_team}")
                response = requests.get(fixtures_url, timeout=10)
                response.raise_for_status()
                fixtures = response.json()
                _log(f"Resposta do /fixtures para liga {league['name']}: {len(fixtures.get('data', []))} jogos encontrados")
                for fixture in fixtures.get('data', []):
                    teams = fixture.get('participants', [])
                    if len(teams) >= 2:
                        home_team_api = teams[0].get('name', '').lower()
                        away_team_api = teams[1].get('name', '').lower()
                        if any(h.lower() in unidecode(home_team_api) for h in home_aliases) and \
                           any(a.lower() in unidecode(away_team_api) for a in away_aliases):
                            odds = fixture.get('odds', [])
                            for odd in odds:
                                if odd.get('bookmaker_id') and odd.get('type_id') == 1:  # 1X2 market
                                    values = {o['name']: o['value'] for o in odd.get('values', [])}
                                    odds_data.append({
                                        'home_team': home_team,
                                        'away_team': away_team,
                                        'home_odds': float(values.get('1', 2.0)),
                                        'draw_odds': float(values.get('X', 3.0)),
                                        'away_odds': float(values.get('2', 2.5))
                                    })
                                    _log(f"Odds encontrados na liga {league['name']} para {home_team} x {away_team}")
                                    found = True
                                    break
                            if found:
                                break
                if found:
                    break
            except Exception as e:
                _log(f"Erro no /fixtures para liga {league['name']} e {home_team} x {away_team}: {e}")
        if not found:
            _log(f"Nenhuma odds encontrada para {home_team} x {away_team}, usando valores padrão")
            odds_data.append({
                'home_team': home_team,
                'away_team': away_team,
                'home_odds': 2.0,
                'draw_odds': 3.0,
                'away_odds': 2.5
            })

    if odds_data:
        df_odds = pd.DataFrame(odds_data)
        os.makedirs(rodada, exist_ok=True)
        df_odds.to_csv(f"{rodada}/odds_sportmonks.csv", index=False)
        _log(f"Odds Sportmonks salvos em {rodada}/odds_sportmonks.csv")
    else:
        _log("Nenhum dado de odds Sportmonks obtido, criando arquivo vazio")
        pd.DataFrame(columns=['home_team', 'away_team', 'home_odds', 'draw_odds', 'away_odds']).to_csv(f"{rodada}/odds_sportmonks.csv", index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", required=True, help="Chave API do Sportmonks")
    ap.add_argument("--regions", required=True)
    ap.add_argument("--aliases_file", required=True)
    ap.add_argument("--api_key_theodds", nargs="?", default=None, help="Chave API da TheOddsAPI (opcional)")
    args = ap.parse_args()

    ingest_odds_sportmonks(args.rodada, args.source_csv, args.api_key, args.regions, args.aliases_file, args.api_key_theodds)

if __name__ == "__main__":
    main()