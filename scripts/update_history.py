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
    """Busca partidas finalizadas da API-Football via RapidAPI."""
    since = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y-%m-%d")
    until = datetime.utcnow().strftime("%Y-%m-%d")
    _log(f"Buscando partidas finalizadas de {since} até {until} (UTC) …")
    
    # Ligas Brasileirão Série A (ID 71) e Série B (ID 72)
    leagues = [71, 72]
    matches = []
    for league_id in leagues:
        url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
        headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }
        params = {
            "from": since,
            "to": until,
            "status": "FT",  # Partidas finalizadas
            "league": league_id,
            "season": 2025
        }
        try:
            response = requests.get(url, headers=headers, params=params, timeout=25)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                _log(f"Erro 403: Chave API-Football inválida ou limite excedido para liga {league_id}. Verifique API_FOOTBALL_KEY e assinatura no RapidAPI.")
            else:
                _log(f"Erro na API-Football para liga {league_id}: {e}")
            sys.exit(1)
        except requests.RequestException as e:
            _log(f"Erro de conexão na API-Football para liga {league_id}: {e}")
            sys.exit(1)
        
        if not data.get("response"):
            _log(f"Nenhuma partida retornada para liga {league_id}")
            continue
        
        for game in data["response"]:
            matches.append({
                "match_id": game["fixture"]["id"],
                "team_home": game["teams"]["home"]["name"],
                "team_away": game["teams"]["away"]["name"],
                "score_home": game["goals"]["home"],
                "score_away": game["goals"]["away"],
                "date": game["fixture"]["date"],
                "league_id": league_id
            })
    
    df = pd.DataFrame(matches)
    if df.empty:
        _log("Nenhuma partida válida coletada para qualquer liga — falhando. Verifique API_FOOTBALL_KEY e plano da API.")
        sys.exit(1)
    
    _log(f"Coletadas {len(df)} partidas")
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
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False, encoding="utf-8")
    _log(f"OK — gerado {args.out} com {len(df)} partidas")

if __name__ == "__main__":
    main()