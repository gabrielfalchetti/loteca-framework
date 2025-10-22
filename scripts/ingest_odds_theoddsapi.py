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
    if not os.path.isfile(source_csv):
        _log(f"Arquivo {source_csv} não encontrado")
        return

    try:
        matches = pd.read_csv(source_csv)
    except Exception as e:
        _log(f"Erro ao ler {source_csv}: {e}")
        return

    with open(aliases_file, 'r') as f:
        aliases = json.load(f)

    odds_data = []
    for _, match in matches.iterrows():
        home_team = match.get('home', match.get('team_home', ''))
        away_team = match.get('away', match.get('team_away', ''))
        try:
            url = f"https://api.the-odds-api.com/v4/sports/soccer_brazil_serie_a/odds/?apiKey={api_key}&regions={regions}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            odds = response.json()
            if odds and 'data' in odds:
                for odd in odds['data']:
                    if (aliases.get(home_team, home_team).lower() in odd['teams'].lower() or
                        aliases.get(away_team, away_team).lower() in odd['teams'].lower()):
                        odds_data.append({
                            'home_team': home_team,
                            'away_team': away_team,
                            'home_odds': odd.get('sites', [{}])[0].get('odds', {}).get('h2h', [0])[0] or 2.0,
                            'draw_odds': odd.get('sites', [{}])[0].get('odds', {}).get('h2h', [0])[1] or 3.0,
                            'away_odds': odd.get('sites', [{}])[0].get('odds', {}).get('h2h', [0])[2] or 2.5
                        })
                        break
            _log(f"Odds obtidos para {home_team} x {away_team}")
        except Exception as e:
            _log(f"Erro ao buscar odds para {home_team} x {away_team}: {e}")
            odds_data.append({
                'home_team': home_team,
                'away_team': away_team,
                'home_odds': 2.0,
                'draw_odds': 3.0,
                'away_odds': 2.5
            })

    if odds_data:
        df_odds = pd.DataFrame(odds_data)
        consensus = df_odds.groupby(['home_team', 'away_team']).mean().reset_index()
        os.makedirs(rodada, exist_ok=True)
        consensus.to_csv(f"{rodada}/odds_theoddsapi.csv", index=False)
        _log(f"Odds TheOddsAPI salvos em {rodada}/odds_theoddsapi.csv")
    else:
        _log("Nenhum dado de odds TheOddsAPI obtido, criando arquivo vazio")
        pd.DataFrame(columns=['home_team', 'away_team', 'home_odds', 'draw_odds', 'away_odds']).to_csv(f"{rodada}/odds_theoddsapi.csv", index=False)

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