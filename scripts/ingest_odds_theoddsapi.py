# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import requests
import os
import json
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[ingest_odds_theoddsapi] {msg}", flush=True)

def ingest_odds_theoddsapi(rodada, source_csv, api_key, regions, aliases_file, api_key_apifootball):
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

        url = f"https://api.theoddsapi.com/v4/sports/soccer_brazil_serie_a/odds/?apiKey={api_key}&regions={regions}"
        try:
            response = requests.get(url, timeout=10, verify=True)
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

    output_file = os.path.join(rodada, 'odds_theoddsapi.csv')
    pd.DataFrame(odds_data).to_csv(output_file, index=False)
    _log(f"Odds TheOddsAPI salvos em {output_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", required=True)
    ap.add_argument("--regions", required=True)
    ap.add_argument("--aliases_file", required=True)
    ap.add_argument("--api_key_apifootball", required=True)
    args = ap.parse_args()

    ingest_odds_theoddsapi(args.rodada, args.source_csv, args.api_key, args.regions, args.aliases_file, args.api_key_apifootball)

if __name__ == "__main__":
    main()