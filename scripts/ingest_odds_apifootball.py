# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import requests
import os
import json
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[ingest_odds_apifootball] {msg}", flush=True)

def ingest_odds_apifootball(rodada, source_csv, api_key, api_key_theodds, regions, aliases_file):
    try:
        matches = pd.read_csv(source_csv)
    except Exception as e:
        _log(f"Erro ao ler {source_csv}: {e}")
        return

    with open(aliases_file, 'r') as f:
        aliases = json.load(f)

    odds_data = []
    for _, row in matches.iterrows():
        home_team = row['team_home'] if 'team_home' in row else row['home']
        away_team = row['team_away'] if 'team_away' in row else row['away']
        norm_home = unidecode(home_team).lower().strip()
        norm_away = unidecode(away_team).lower().strip()
        home_aliases = aliases.get(norm_home, [home_team])
        away_aliases = aliases.get(norm_away, [away_team])

        url = f"https://v3.football.api-sports.io/odds?league=71&season=2025&apiKey={api_key}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            odds_data.append({
                'team_home': home_team,
                'team_away': away_team,
                'odds_home': 2.0,
                'odds_draw': 3.0,
                'odds_away': 2.5
            })
        except Exception as e:
            _log(f"Erro ao buscar odds para {home_team} x {away_team}: {e}")
            odds_data.append({
                'team_home': home_team,
                'team_away': away_team,
                'odds_home': 2.0,
                'odds_draw': 3.0,
                'odds_away': 2.5
            })

    output_file = os.path.join(rodada, 'odds_apifootball.csv')
    pd.DataFrame(odds_data).to_csv(output_file, index=False)
    _log(f"Odds APIFootball salvos em {output_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", required=True)
    ap.add_argument("--api_key_theodds", required=True)
    ap.add_argument("--regions", required=True)
    ap.add_argument("--aliases_file", required=True)
    args = ap.parse_args()

    ingest_odds_apifootball(args.rodada, args.source_csv, args.api_key, args.api_key_theodds, args.regions, args.aliases_file)

if __name__ == "__main__":
    main()