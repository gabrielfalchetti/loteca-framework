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
            url = f"https://v3.football.api-sports.io/odds?league=71&season=2025&apiKey={api_key}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            odds = response.json()
            if odds and 'response' in odds:
                for odd in odds['response']:
                    teams = odd.get('teams', {})
                    if (unidecode(home_team).lower() in unidecode(teams.get('home', {}).get('name', '')).lower() or
                        unidecode(away_team).lower() in unidecode(teams.get('away', {}).get('name', '')).lower()):
                        odds_data.append({
                            'home_team': home_team,
                            'away_team': away_team,
                            'home_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[0].get('odd', 2.0),
                            'draw_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[1].get('odd', 3.0),
                            'away_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[2].get('odd', 2.5)
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
        os.makedirs(rodada, exist_ok=True)
        df_odds.to_csv(f"{rodada}/odds_apifootball.csv", index=False)
        _log(f"Odds APIFootball salvos em {rodada}/odds_apifootball.csv")
    else:
        _log("Nenhum dado de odds APIFootball obtido, criando arquivo vazio")
        pd.DataFrame(columns=['home_team', 'away_team', 'home_odds', 'draw_odds', 'away_odds']).to_csv(f"{rodada}/odds_apifootball.csv", index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", required=True, help="Chave API da API-Football (API_FOOTBALL_KEY)")
    ap.add_argument("--regions", required=True)
    ap.add_argument("--aliases_file", required=True)
    ap.add_argument("--api_key_theodds", nargs="?", default=None, help="Chave API da TheOddsAPI (opcional, para compatibilidade)")
    args = ap.parse_args()

    ingest_odds_apifootball(args.rodada, args.source_csv, args.api_key, args.regions, args.aliases_file, args.api_key_theodds)

if __name__ == "__main__":
    main()