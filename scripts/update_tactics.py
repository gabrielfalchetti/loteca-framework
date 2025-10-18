# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import json
import os
import requests
from datetime import datetime, timedelta

def _log(msg: str) -> None:
    print(f"[tactics] {msg}", flush=True)

def fetch_tactics(history_file: str, api_key: str) -> dict:
    """Busca formações táticas da API-Football."""
    if not os.path.isfile(history_file):
        _log(f"{history_file} não encontrado")
        sys.exit(1)

    df = pd.read_csv(history_file)
    if df.empty:
        _log("Arquivo de histórico vazio — falhando.")
        sys.exit(1)

    teams = set(df['team_home']).union(set(df['team_away']))
    tactics = {}
    url = "https://v3.football.api-sports.io/fixtures/lineups"
    headers = {"x-apisports-key": api_key}

    for team in teams:
        # Buscar última partida do time para obter formação
        team_matches = df[(df['team_home'] == team) | (df['team_away'] == team)]
        if team_matches.empty:
            _log(f"Nenhuma partida encontrada para {team}, usando formação padrão")
            tactics[team] = {"formation": "4-2-3-1"}  # Fallback
            continue

        latest_match = team_matches.sort_values(by='date', ascending=False).iloc[0]
        match_id = latest_match['match_id']
        params = {"fixture": match_id}
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=25)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.HTTPError as e:
            _log(f"Erro HTTP na API-Football para fixture {match_id}: {e}")
            tactics[team] = {"formation": "4-2-3-1"}  # Fallback
            continue
        except requests.RequestException as e:
            _log(f"Erro de conexão na API-Football para fixture {match_id}: {e}")
            tactics[team] = {"formation": "4-2-3-1"}  # Fallback
            continue

        if not data.get("response"):
            _log(f"Nenhuma formação retornada para {team}, usando padrão")
            tactics[team] = {"formation": "4-2-3-1"}  # Fallback
            continue

        formation = data["response"][0]["formation"] if data["response"] else "4-2-3-1"
        tactics[team] = {"formation": formation}

    if not tactics:
        _log("Nenhuma tática gerada para qualquer time — falhando.")
        sys.exit(1)

    _log(f"Geradas táticas para {len(tactics)} times")
    return tactics

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="Arquivo CSV de histórico")
    ap.add_argument("--out", required=True, help="Arquivo JSON de saída")
    ap.add_argument("--api_key", default=os.getenv("API_FOOTBALL_KEY"), help="Chave API-Football")
    args = ap.parse_args()

    if not args.api_key:
        _log("API_FOOTBALL_KEY não definida")
        sys.exit(1)

    tactics = fetch_tactics(args.history, args.api_key)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(tactics, f, ensure_ascii=False, indent=2)
    _log(f"OK — gerado {args.out} com {len(tactics)} times")

if __name__ == "__main__":
    main()