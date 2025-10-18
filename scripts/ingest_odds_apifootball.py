# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import requests
import os
import json
from rapidfuzz import fuzz
from unidecode import unidecode
from datetime import datetime, timedelta

def _log(msg: str) -> None:
    print(f"[apifootball] {msg}", flush=True)

def normalize_team_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = unidecode(name).lower().strip()
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo").replace("inter de milao", "inter").replace("manchester united", "manchester utd").replace("ldu quito", "ldu")
    return name.capitalize()

def match_team(api_name: str, source_teams: list, aliases: dict, threshold: float = 60) -> str:
    api_norm = normalize_team_name(api_name)
    for source_team in source_teams:
        source_norm = normalize_team_name(source_team)
        if api_norm in aliases.get(source_norm, []) or fuzz.ratio(api_norm, source_norm) > threshold:
            return source_team
    return None

def fetch_stats(rodada: str, source_csv: str, api_key: str, aliases_file: str) -> pd.DataFrame:
    matches_df = pd.read_csv(source_csv)
    home_col = next((col for col in ['team_home', 'home'] if col in matches_df.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in matches_df.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas")
        sys.exit(5)

    matches_df[home_col] = matches_df[home_col].apply(normalize_team_name)
    matches_df[away_col] = matches_df[away_col].apply(normalize_team_name)
    source_teams = set(matches_df[home_col].tolist() + matches_df[away_col].tolist())

    aliases = {}
    if os.path.exists(aliases_file):
        with open(aliases_file, 'r') as f:
            aliases = json.load(f)
    
    stats = []
    url_fixtures = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key}
    since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    until = (datetime.utcnow() + timedelta(days=60)).strftime("%Y-%m-%d")
    params = {
        "from": since,
        "to": until,
        "season": 2025,
        "league": "71,72,203,70,74,77,39,140,13,2,112",
        "timezone": "America/Sao_Paulo"
    }
    
    try:
        response = requests.get(url_fixtures, headers=headers, params=params, timeout=25)
        response.raise_for_status()
        fixtures_data = response.json()
    except Exception as e:
        _log(f"Erro ao buscar fixtures: {e}")
        fixtures_data = {"response": []}

    if not fixtures_data.get("response"):
        _log(f"Nenhum fixture retornado para ligas {params['league']} no período {since} a {until}")
    else:
        _log(f"Fixtures retornados pela API-Football: {len(fixtures_data['response'])}")
        for game in fixtures_data["response"][:5]:
            _log(f"Fixture ID: {game['fixture']['id']}, Jogo: {game['teams']['home']['name']} x {game['teams']['away']['name']}")

    fixtures = fixtures_data["response"]
    fixture_map = {}
    for game in fixtures:
        home_team = normalize_team_name(game["teams"]["home"]["name"])
        away_team = normalize_team_name(game["teams"]["away"]["name"])
        fixture_id = game["fixture"]["id"]
        home_matched = match_team(home_team, source_teams, aliases)
        away_matched = match_team(away_team, source_teams, aliases)
        if home_matched and away_matched:
            fixture_map[(home_matched, away_matched)] = fixture_id
        else:
            _log(f"Não pareado: {home_team} x {away_team}")

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

        stats_data, injuries_data, lineups_data = None, None, None
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

        stats.append({
            "match_id": fixture_id,
            "team_home": home_team,
            "team_away": away_team,
            "xG_home": stats_data["response"][0]["statistics"].get("xG", 0) if stats_data and stats_data.get("response") and len(stats_data["response"]) >= 2 else 0,
            "xG_away": stats_data["response"][1]["statistics"].get("xG", 0) if stats_data and stats_data.get("response") and len(stats_data["response"]) >= 2 else 0,
            "lesions_home": len(injuries_data["response"][0].get("players", {}).get("injured", [])) if injuries_data and injuries_data.get("response") else 0,
            "lesions_away": len(injuries_data["response"][1].get("players", {}).get("injured", [])) if injuries_data and injuries_data.get("response") else 0,
            "formation_home": lineups_data["response"][0].get("formation", "unknown") if lineups_data and lineups_data.get("response") else "unknown",
            "formation_away": lineups_data["response"][1].get("formation", "unknown") if lineups_data and lineups_data.get("response") else "unknown"
        })

    df = pd.DataFrame(stats)
    if len(df) < 14:  # Concurso 1216 tem 14 jogos
        _log(f"Apenas {len(df)} jogos pareados, esperado 14. Verifique times em source_csv ou API key.")
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
    ap.add_argument("--aliases_file", default="data/aliases/auto_aliases.json")
    args = ap.parse_args()

    if not args.api_key:
        _log("API_FOOTBALL_KEY não definida")
        sys.exit(5)

    fetch_stats(args.rodada, args.source_csv, args.api_key, args.aliases_file)

if __name__ == "__main__":
    main()