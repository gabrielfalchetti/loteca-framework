# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import requests
import json

def _log(msg: str) -> None:
    print(f"[ingest_odds_apifootball] {msg}", flush=True)

def ingest_odds_apifootball(rodada, source_csv, api_key, api_key_theodds, regions, aliases_file):
    if not os.path.isfile(source_csv):
        _log(f"Arquivo {source_csv} não encontrado")
        sys.exit(5)

    try:
        matches = pd.read_csv(source_csv)
    except Exception as e:
        _log(f"Erro ao ler {source_csv}: {e}")
        sys.exit(5)

    if matches.empty:
        _log("Arquivo de jogos vazio")
        sys.exit(5)

    # Carregar aliases
    aliases = {}
    if os.path.isfile(aliases_file):
        try:
            with open(aliases_file, 'r') as f:
                aliases = json.load(f)
        except Exception as e:
            _log(f"Erro ao ler {aliases_file}: {e}, usando aliases vazios")
            aliases = {}

    # Buscar odds da API-Football
    odds_data = []
    for _, match in matches.iterrows():
        match_id = match.get('match_id', 0)
        home_team = match.get('home', match.get('team_home', ''))
        away_team = match.get('away', match.get('team_away', ''))
        try:
            url = f"https://v3.football.api-sports.io/odds?fixture={match_id}&apiKey={api_key}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            odds = response.json()
            if odds and 'response' in odds:
                for odd in odds['response']:
                    if (aliases.get(home_team, home_team).lower() in odd['teams'].lower() or
                        aliases.get(away_team, away_team).lower() in odd['teams'].lower()):
                        odds_data.append({
                            'match_id': match_id,
                            'home_team': home_team,
                            'away_team': away_team,
                            'home_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[0].get('odd', 2.1) or 2.1,
                            'draw_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[1].get('odd', 3.1) or 3.1,
                            'away_odds': odd.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[2].get('odd', 2.6) or 2.6
                        })
                        break
            _log(f"Odds obtidos para {home_team} x {away_team}")
        except Exception as e:
            _log(f"Erro ao buscar odds para {home_team} x {away_team}: {e}")
            odds_data.append({
                'match_id': match_id,
                'home_team': home_team,
                'away_team': away_team,
                'home_odds': 2.1,
                'draw_odds': 3.1,
                'away_odds': 2.6
            })

    # Consolidar odds
    if odds_data:
        df_odds = pd.DataFrame(odds_data)
        consensus = df_odds.groupby(['match_id', 'home_team', 'away_team']).mean().reset_index()
        os.makedirs(rodada, exist_ok=True)
        consensus.to_csv(f"{rodada}/odds_apifootball.csv", index=False)
        _log(f"Odds APIFootball salvos em {rodada}/odds_apifootball.csv")
    else:
        _log("Nenhum dado de odds APIFootball obtido, criando arquivo vazio")
        pd.DataFrame(columns=['match_id', 'home_team', 'away_team', 'home_odds', 'draw_odds', 'away_odds']).to_csv(f"{rodada}/odds_apifootball.csv", index=False)

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