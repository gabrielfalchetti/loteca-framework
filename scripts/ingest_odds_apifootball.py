# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import requests
import os
from rapidfuzz import fuzz
from datetime import datetime, timedelta

def _log(msg: str) -> None:
    print(f"[apifootball] {msg}", flush=True)

def match_team(api_name: str, source_teams: list, threshold: float = 80) -> str:
    for source_team in source_teams:
        if fuzz.ratio(api_name.lower(), source_team.lower()) > threshold:
            return source_team
    return None

def fetch_stats(rodada: str, source_csv: str, api_key: str) -> pd.DataFrame:
    matches_df = pd.read_csv(source_csv)
    home_col = 'team_home' if 'team_home' in matches_df.columns else 'home'
    away_col = 'team_away' if 'team_away' in matches_df.columns else 'away'
    if home_col not in matches_df.columns or away_col not in matches_df.columns:
        _log("Colunas team_home/team_away ou home/away ausentes")
        sys.exit(5)

    source_teams = set(matches_df[home_col].tolist() + matches_df[away_col].tolist())
    stats = []
    headers = {"x-apisports-key": api_key}
    
    # Buscar fixtures
    url_fixtures = "https://v3.football.api-sports.io/fixtures"
    dates = ["2025-10-18", "2025-10-19"]  # Loteca 1216
    seasons = [2024, 2023]  # Fallback para 2023
    fixture_map = {}
    
    for date in dates:
        for season in seasons:
            params = {"date": date, "season": season, "timezone": "America/Sao_Paulo"}
            try:
                response = requests.get(url_fixtures, headers=headers, params=params, timeout=25)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.HTTPError as e:
                _log(f"Erro HTTP ao buscar fixtures (date={date}, season={season}): {e}")
                continue
            except requests.RequestException as e:
                _log(f"Erro de conexão ao buscar fixtures: {e}")
                continue

            if not data.get("response"):
                _log(f"Nenhum fixture retornado para date={date}, season={season}")
                continue

            _log(f"Fixtures retornados para date={date}, season={season}: {len(data['response'])}")
            for game in data["response"][:5]:
                _log(f"Fixture ID: {game['fixture']['id']}, Jogo: {game['teams']['home']['name']} x {game['teams']['away']['name']}")

            for game in data["response"]:
                home_team = game["teams"]["home"]["name"]
                away_team = game["teams"]["away"]["name"]
                fixture_id = game["fixture"]["id"]
                home_matched = match_team(home_team, source_teams)
                away_matched = match_team(away_team, source_teams)
                if home_matched and away_matched:
                    fixture_map[(home_matched, away_matched)] = fixture_id

    if not fixture_map:
        _log("Nenhum jogo pareado. Verifique times em source_csv ou API_FOOTBALL_KEY.")
        sys.exit(5)

    # Buscar stats
    url_stats = "https://v3.football.api-sports.io/fixtures/statistics"
    for _, row in matches_df.iterrows():
        home_team = row[home_col]
        away_team = row[away_col]
        fixture_id = fixture_map.get((home_team, away_team))
        if not fixture_id:
            _log(f"Fixture não encontrado para {home_team} x {away_team}")
            continue

        try:
            response = requests.get(url_stats, headers=headers, params={"fixture": fixture_id}, timeout=25)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.HTTPError as e:
            _log(f"Erro HTTP para fixture {fixture_id}: {e}")
            continue
        except requests.RequestException as e:
            _log(f"Erro de conexão para fixture {fixture_id}: {e}")
            continue

        if data.get("response") and len(data["response"]) >= 2:
            stats.append({
                "match_id": fixture_id,
                "team_home": home_team,
                "team_away": away_team,
                "xG_home": data["response"][0]["statistics"].get("xG", 0),
                "xG_away": data["response"][1]["statistics"].get("xG", 0),
                "lesions_home": len(data["response"][0].get("players", {}).get("injured", [])),
                "lesions_away": len(data["response"][1].get("players", {}).get("injured", []))
            })

    df = pd.DataFrame(stats)
    if df.empty:
        _log("Nenhum jogo processado — falhando. Verifique times em source_csv ou API_FOOTBALL_KEY.")
        sys.exit(5)

    out_file = f"{rodada}/odds_apifootball.csv"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    df.to_csv(out_file, index=False)
    _log(f"Arquivo {out_file} gerado com {len(df)} jogos")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", default=os.getenv("API_FOOTBALL_KEY"))
    args = ap.parse_args()

    if not args.api_key:
        _log("API_FOOTBALL_KEY não definida")
        sys.exit(5)

    fetch_stats(args.rodada, args.source_csv, args.api_key)

if __name__ == "__main__":
    main()