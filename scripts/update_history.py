# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
from datetime import datetime, timedelta
import requests
import os

def _log(msg: str) -> None:
    print(f"[update_history] {msg}", flush=True)

def fetch_matches(since_days: int, api_key: str) -> pd.DataFrame:
    """Busca partidas finalizadas da API-Football."""
    since = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y-%m-%d")
    until = datetime.utcnow().strftime("%Y-%m-%d")
    _log(f"buscando partidas finalizadas de {since} até {until} (UTC) …")
    
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }
    params = {
        "from": since,
        "to": until,
        "status": "FT"  # Partidas finalizadas
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    
    if not data.get("response"):
        _log("API retornou 0 partidas — falhando.")
        sys.exit(1)
    
    matches = []
    for game in data["response"]:
        matches.append({
            "match_id": game["fixture"]["id"],
            "team_home": game["teams"]["home"]["name"],
            "team_away": game["teams"]["away"]["name"],
            "score_home": game["goals"]["home"],
            "score_away": game["goals"]["away"],
            "date": game["fixture"]["date"]
        })
    
    df = pd.DataFrame(matches)
    if df.empty:
        _log("Nenhuma partida válida coletada — falhando.")
        sys.exit(1)
    
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since_days", type=int, required=True, help="Dias para buscar histórico")
    ap.add_argument("--out", required=True, help="Arquivo CSV de saída")
    ap.add_argument("--api_key", default=os.getenv("API_FOOTBALL_KEY"), help="Chave API-Football")
    args = ap.parse_args()

    if not args.api_key:
        _log("API_FOOTBALL_KEY não definida")
        sys.exit(1)

    df = fetch_matches(args.since_days, args.api_key)
    df.to_csv(args.out, index=False, encoding="utf-8")
    _log(f"OK — gerado {args.out} com {len(df)} partidas")

if __name__ == "__main__":
    main()