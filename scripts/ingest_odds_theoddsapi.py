# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import requests
import os
import json
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[ingest_odds_theoddsapi] {msg}", flush=True)

def ingest_odds_theoddsapi(rodada, source_csv, api_key, regions, aliases_file, api_key_theodds=None):
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

    # Listar esportes disponíveis
    try:
        sports_url = f"https://api.the-odds-api.com/v4/sports/?apiKey={api_key}"
        response = requests.get(sports_url, timeout=10)
        response.raise_for_status()
        sports = response.json()
        _log(f"Esportes disponíveis na TheOddsAPI: {json.dumps(sports, indent=2)}")
        sport_keys = [sport['key'] for sport in sports if 'soccer' in sport['key'].lower()]
    except Exception as e:
        _log(f"Erro ao listar esportes disponíveis: {e}")
        sport_keys = [
            'soccer_brazil_serie_a',
            'soccer_brazil_serie_b',
            'soccer_brazil_copa_do_brasil',
            'soccer_southamerica_libertadores',
            'soccer_uefa_europa_league'
        ]

    odds_data = []
    for _, match in matches.iterrows():
        home_team = match.get('home', match.get('team_home', ''))
        away_team = match.get('away', match.get('team_away', ''))
        norm_home = unidecode(home_team).lower().strip()
        norm_away = unidecode(away_team).lower().strip()
        home_aliases = aliases.get(norm_home, [home_team])
        away_aliases = aliases.get(norm_away, [away_team])

        found = False
        for sport_key in sport_keys:
            try:
                url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/?apiKey={api_key}&regions={regions}"
                _log(f"Tentando {sport_key} para {home_team} x {away_team}")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                odds = response.json()
                _log(f"Resposta da API para {sport_key}: {json.dumps(odds, indent=2)}")
                for game in odds:
                    if any(h.lower() in unidecode(game.get('home_team', '')).lower() for h in home_aliases) and \
                       any(a.lower() in unidecode(game.get('away_team', '')).lower() for a in away_aliases):
                        odds_data.append({
                            'home_team': home_team,
                            'away_team': away_team,
                            'home_odds': game.get('bookmakers', [{}])[0].get('markets', [{}])[0].get('outcomes', [{}])[0].get('price', 2.0),
                            'draw_odds': game.get('bookmakers', [{}])[0].get('markets', [{}])[0].get('outcomes', [{}])[1].get('price', 3.0),
                            'away_odds': game.get('bookmakers', [{}])[0].get('markets', [{}])[0].get('outcomes', [{}])[2].get('price', 2.5)
                        })
                        _log(f"Odds encontrados em {sport_key} para {home_team} x {away_team}")
                        found = True
                        break
                if found:
                    break
            except Exception as e:
                _log(f"Erro em {sport_key} para {home_team} x {away_team}: {e}")
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
        df_odds.to_csv(f"{rodada}/odds_theoddsapi.csv", index=False)
        _log(f"Odds TheOddsAPI salvos em {rodada}/odds_theoddsapi.csv")
    else:
        _log("Nenhum dado de odds TheOddsAPI obtido, criando arquivo vazio")
        pd.DataFrame(columns=['home_team', 'away_team', 'home_odds', 'draw_odds', 'away_odds']).to_csv(f"{rodada}/odds_theoddsapi.csv", index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", required=True, help="Chave API da TheOddsAPI (THEODDS_API_KEY)")
    ap.add_argument("--regions", required=True)
    ap.add_argument("--aliases_file", required=True)
    ap.add_argument("--api_key_theodds", nargs="?", default=None, help="Chave API da TheOddsAPI (opcional, para compatibilidade)")
    args = ap.parse_args()

    ingest_odds_theoddsapi(args.rodada, args.source_csv, args.api_key, args.regions, args.aliases_file, args.api_key_theodds)

if __name__ == "__main__":
    main()