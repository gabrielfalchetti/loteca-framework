# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import requests
import os
import json
import ssl
import urllib3

def _log(msg: str) -> None:
    print(f"[ingest_odds_theoddsapi] {msg}", flush=True)

def ingest_odds_theoddsapi(rodada, source_csv, api_key, regions, aliases_file, api_key_theodds):
    try:
        matches = pd.read_csv(source_csv)
    except Exception as e:
        _log(f"Erro ao ler {source_csv}: {e}")
        return

    try:
        from unidecode import unidecode
    except ImportError:
        _log("Módulo unidecode não encontrado. Usando nomes sem normalização.")
        unidecode = lambda x: x.lower().strip()

    try:
        with open(aliases_file, 'r') as f:
            aliases = json.load(f)
    except Exception as e:
        _log(f"Erro ao ler {aliases_file}: {e}")
        aliases = {}

    # Configurar contexto SSL
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    odds_data = []
    for _, row in matches.iterrows():
        home_team = row['team_home'] if 'team_home' in row else row['home']
        away_team = row['team_away'] if 'team_away' in row else row['away']
        norm_home = unidecode(home_team).lower().strip()
        norm_away = unidecode(away_team).lower().strip()
        home_aliases = aliases.get(norm_home, [home_team])
        away_aliases = aliases.get(norm_away, [away_team])

        sport_keys = ['soccer_brazil_serie_a', 'soccer_brazil_serie_b', 'soccer_brazil_copa_do_brasil']
        for sport_key in sport_keys:
            url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/?apiKey={api_key}&regions={regions}"
            try:
                _log(f"Tentando conectar a {url}")
                response = requests.get(url, timeout=10, verify=ssl_context)
                response.raise_for_status()
                data = response.json()
                for game in data:
                    if any(h.lower() in game.get('home_team', '').lower() for h in home_aliases) and \
                       any(a.lower() in game.get('away_team', '').lower() for a in away_aliases):
                        odds_data.append({
                            'team_home': home_team,
                            'team_away': away_team,
                            'odds_home': game.get('bookmakers', [{}])[0].get('markets', [{}])[0].get('outcomes', [{}])[0].get('price', 2.0),
                            'odds_draw': game.get('bookmakers', [{}])[0].get('markets', [{}])[0].get('outcomes', [{}])[1].get('price', 3.0),
                            'odds_away': game.get('bookmakers', [{}])[0].get('markets', [{}])[0].get('outcomes', [{}])[2].get('price', 2.5)
                        })
                        break
                else:
                    _log(f"Sem odds para {home_team} x {away_team} em {sport_key}")
                    odds_data.append({
                        'team_home': home_team,
                        'team_away': away_team,
                        'odds_home': 2.0,
                        'odds_draw': 3.0,
                        'odds_away': 2.5
                    })
                break
            except Exception as e:
                _log(f"Erro ao buscar odds para {home_team} x {away_team} em {sport_key}: {e}")
                odds_data.append({
                    'team_home': home_team,
                    'team_away': away_team,
                    'odds_home': 2.0,
                    'odds_draw': 3.0,
                    'odds_away': 2.5
                })

    output_file = os.path.join(rodada, 'odds_theoddsapi.csv')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    pd.DataFrame(odds_data).to_csv(output_file, index=False)
    _log(f"Odds TheOddsAPI salvos em {output_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", required=True)
    ap.add_argument("--api_key_theodds", required=True)
    ap.add_argument("--regions", required=True)
    ap.add_argument("--aliases_file", required=True)
    args = ap.parse_args()

    ingest_odds_theoddsapi(args.rodada, args.source_csv, args.api_key, args.regions, args.aliases_file, args.api_key_theodds)

if __name__ == "__main__":
    main()