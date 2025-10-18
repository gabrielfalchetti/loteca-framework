# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import requests
import os
import json
from rapidfuzz import fuzz
from datetime import datetime, timedelta

def _log(msg: str) -> None:
    print(f"[apifootball] {msg}", flush=True)

def match_team(api_name: str, source_teams: list, threshold: float = 70) -> str:
    for source_team in source_teams:
        if fuzz.ratio(api_name.lower(), source_team.lower()) > threshold:
            return source_team
    return None

def fetch_fallback_theoddsapi(regions: str, api_key_theodds: str) -> list:
    sports = [
        "soccer_brazil_campeonato",  # Série A/B
        "soccer_italy_serie_a",     # Serie A italiana
        "soccer_epl",               # Premier League
        "soccer_spain_la_liga",     # La Liga
        "soccer_conmebol_copa_libertadores"  # Libertadores
    ]
    all_games = []
    for sport in sports:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds?regions={regions}&markets=h2h&dateFormat=iso&oddsFormat=decimal&apiKey={api_key_theodds}"
        try:
            response = requests.get(url, timeout=25)
            response.raise_for_status()
            games = response.json()
            _log(f"TheOddsAPI retornou {len(games)} jogos para {sport}")
            all_games.extend(games)
        except Exception as e:
            _log(f"Falha ao buscar {sport} no TheOddsAPI: {e}")
    return all_games

def fetch_stats(rodada: str, source_csv: str, api_key: str, api_key_theodds: str, regions: str) -> pd.DataFrame:
    matches_df = pd.read_csv(source_csv)
    home_col = next((col for col in ['team_home', 'home'] if col in matches_df.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in matches_df.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas")
        sys.exit(5)

    source_teams = set(matches_df[home_col].tolist() + matches_df[away_col].tolist())
    stats = []
    
    # Tentar API-Football
    url_fixtures = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key}
    since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    until = (datetime.utcnow() + timedelta(days=60)).strftime("%Y-%m-%d")
    params = {
        "from": since,
        "to": until,
        "season": 2025,
        "league": "71,72,203,70,74,77,39,140,13,2",
        "timezone": "America/Sao_Paulo"
    }
    
    fixtures_data = None
    try:
        response = requests.get(url_fixtures, headers=headers, params=params, timeout=25)
        response.raise_for_status()
        fixtures_data = response.json()
    except Exception as e:
        _log(f"Erro ao buscar fixtures da API-Football: {e}")

    if not fixtures_data or not fixtures_data.get("response"):
        _log(f"Nenhum fixture retornado pela API-Football para ligas {params['league']} no período {since} a {until}")
        fixtures_data = fetch_fallback_theoddsapi(regions, api_key_theodds)
        if not fixtures_data:
            _log("Nenhum dado de fixtures obtido de nenhuma API")
            sys.exit(5)
        fixtures = [
            {
                "fixture": {"id": game["id"]},
                "teams": {
                    "home": {"name": game["home_team"]},
                    "away": {"name": game["away_team"]}
                },
                "odds": game.get("bookmakers", [{}])[0].get("markets", [{}])[0].get("outcomes", [])
            } for game in fixtures_data
        ]
    else:
        fixtures = fixtures_data["response"]
        _log(f"Fixtures retornados pela API-Football: {len(fixtures)}")
        for game in fixtures[:5]:
            _log(f"Fixture ID: {game['fixture']['id']}, Jogo: {game['teams']['home']['name']} x {game['teams']['away']['name']}")

    # Mapear match_id por time
    fixture_map = {}
    for game in fixtures:
        home_team = game["teams"]["home"]["name"]
        away_team = game["teams"]["away"]["name"]
        fixture_id = game["fixture"]["id"]
        home_matched = match_team(home_team, source_teams)
        away_matched = match_team(away_team, source_teams)
        if home_matched and away_matched:
            fixture_map[(home_matched, away_matched)] = fixture_id
        else:
            _log(f"Não pareado: {home_team} x {away_team}")

    if not fixture_map:
        _log("Nenhum jogo pareado com source_csv")
        sys.exit(5)

    # Buscar stats (API-Football) ou odds (TheOddsAPI)
    url_stats = "https://v3.football.api-sports.io/fixtures/statistics"
    url_injuries = "https://v3.football.api-sports.io/injuries"
    url_lineups = "https://v3.football.api-sports.io/fixtures/lineups"
    for _, row in matches_df.iterrows():
        home_team = row[home_col]
        away_team = row[away_col]
        fixture_id = fixture_map.get((home_team, away_team))
        if not fixture_id:
            _log(f"Fixture não encontrado para {home_team} x {away_team}")
            continue

        # Buscar stats, injuries, lineups (API-Football)
        stats_data, injuries_data, lineups_data = None, None, None
        if fixtures_data.get("response"):  # API-Football
            try:
                response = requests.get(url_stats, headers=headers, params={"fixture": fixture_id}, timeout=25)
                response.raise_for_status()
                stats_data = response.json()
            except Exception as e:
                _log(f"Erro ao buscar stats para fixture {fixture_id}: {e}")

            try:
                response = requests.get(url_injuries, headers=headers, params={"fixture": fixture_id}, timeout=25)
                response.raise_for_status()
                injuries_data = response.json()
            except Exception as e:
                _log(f"Erro ao buscar injuries para fixture {fixture_id}: {e}")

            try:
                response = requests.get(url_lineups, headers=headers, params={"fixture": fixture_id}, timeout=25)
                response.raise_for_status()
                lineups_data = response.json()
            except Exception as e:
                _log(f"Erro ao buscar lineups para fixture {fixture_id}: {e}")

        # Buscar odds (TheOddsAPI ou API-Football)
        odds_data = None
        for game in fixtures_data if not fixtures_data.get("response") else []:
            if game["id"] == fixture_id:
                odds_data = game.get("bookmakers", [{}])[0].get("markets", [{}])[0].get("outcomes", [])
                break

        if stats_data and stats_data.get("response") and len(stats_data["response"]) >= 2:
            stats.append({
                "match_id": fixture_id,
                "team_home": home_team,
                "team_away": away_team,
                "xG_home": stats_data["response"][0]["statistics"].get("xG", 0) if stats_data["response"][0].get("statistics") else 0,
                "xG_away": stats_data["response"][1]["statistics"].get("xG", 0) if stats_data["response"][1].get("statistics") else 0,
                "lesions_home": len(injuries_data["response"][0].get("players", {}).get("injured", [])) if injuries_data and injuries_data.get("response") else 0,
                "lesions_away": len(injuries_data["response"][1].get("players", {}).get("injured", [])) if injuries_data and injuries_data.get("response") else 0,
                "formation_home": lineups_data["response"][0].get("formation", "unknown") if lineups_data and lineups_data.get("response") else "unknown",
                "formation_away": lineups_data["response"][1].get("formation", "unknown") if lineups_data and lineups_data.get("response") else "unknown",
                "odds_home": odds_data[0]["price"] if odds_data and len(odds_data) > 0 else 0,
                "odds_draw": odds_data[1]["price"] if odds_data and len(odds_data) > 1 else 0,
                "odds_away": odds_data[2]["price"] if odds_data and len(odds_data) > 2 else 0
            })

    df = pd.DataFrame(stats)
    if df.empty:
        _log("Nenhum jogo processado. Verifique times em source_csv ou chaves API.")
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
    ap.add_argument("--api_key_theodds", default=os.getenv("THEODDS_API_KEY"))
    ap.add_argument("--regions", default="uk,eu,us,au")
    args = ap.parse_args()

    if not args.api_key or not args.api_key_theodds:
        _log("API_FOOTBALL_KEY ou THEODDS_API_KEY não definida")
        sys.exit(5)

    fetch_stats(args.rodada, args.source_csv, args.api_key, args.api_key_theodds, args.regions)

if __name__ == "__main__":
    main()