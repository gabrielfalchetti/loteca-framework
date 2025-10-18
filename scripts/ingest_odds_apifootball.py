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
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "").replace("/pe", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo").replace("inter de milao", "inter").replace("manchester united", "manchester utd")
    name = name.replace("sport recife", "sport").replace("atletico mineiro", "atlético").replace("bragantino-sp", "bragantino").replace("vasco da gama", "vasco").replace("fluminense", "fluminense").replace("santos", "santos").replace("vitoria", "vitória").replace("mirassol", "mirassol").replace("gremio", "grêmio").replace("juventude", "juventude").replace("roma", "roma").replace("getafe", "getafe").replace("real madrid", "real madrid").replace("liverpool", "liverpool")
    name = name.replace("atalanta bergamas", "atalanta").replace("fiorentina", "fiorentina").replace("osasuna", "osasuna")
    return name.capitalize()

def match_team(api_name: str, source_teams: list, aliases: dict, threshold: float = 50) -> str:
    api_norm = normalize_team_name(api_name).lower()
    for source_team in source_teams:
        source_norm = normalize_team_name(source_team).lower()
        if api_norm in [normalize_team_name(alias).lower() for alias in aliases.get(source_norm, [])] or fuzz.partial_ratio(api_norm, source_norm) > threshold:
            return source_team
    return None

def fetch_stats(rodada: str, source_csv: str, api_key: str, aliases_file: str, api_key_theodds: str, regions: str) -> pd.DataFrame:
    matches_df = pd.read_csv(source_csv)
    home_col = next((col for col in ['team_home', 'home'] if col in matches_df.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in matches_df.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas")
        sys.exit(5)
    if len(matches_df) != 14:
        _log(f"Arquivo {source_csv} contém {len(matches_df)} jogos, esperado 14")
        sys.exit(5)

    matches_df[home_col] = matches_df[home_col].apply(normalize_team_name)
    matches_df[away_col] = matches_df[away_col].apply(normalize_team_name)
    source_teams = set(matches_df[home_col].tolist() + matches_df[away_col].tolist())

    aliases = {}
    if os.path.exists(aliases_file):
        with open(aliases_file, 'r') as f:
            aliases = json.load(f)
    url_teams = "https://v3.football.api-sports.io/teams"
    headers = {"x-apisports-key": api_key}
    leagues = ["71", "72", "203", "70", "74", "77", "39", "140", "13", "2", "112"]
    for league in leagues:
        try:
            response = requests.get(url_teams, headers=headers, params={"league": league, "season": 2025}, timeout=25)
            response.raise_for_status()
            teams_data = response.json().get("response", [])
            for team in teams_data:
                team_name = normalize_team_name(team["team"]["name"])
                aliases[team_name] = [team_name, normalize_team_name(team["team"].get("code", team_name))]
        except Exception as e:
            _log(f"Erro ao buscar times da liga {league}: {e}")

    stats = []
    url_fixtures = "https://v3.football.api-sports.io/fixtures"
    dates = matches_df['date'].unique() if 'date' in matches_df.columns else [
        (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d"),
        (datetime.utcnow() + timedelta(days=2)).strftime("%Y-%m-%d")
    ]
    if len(dates) > 0:
        since = min(datetime.strptime(d, '%Y-%m-%d') for d in dates).strftime('%Y-%m-%d')
        until = max(datetime.strptime(d, '%Y-%m-%d') for d in dates).strftime('%Y-%m-%d')
    else:
        since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        until = (datetime.utcnow() + timedelta(days=60)).strftime("%Y-%m-%d")

    fixtures = []
    params = {
        "from": since,
        "to": until,
        "season": 2025,
        "league": ",".join(leagues),
        "timezone": "America/Sao_Paulo"
    }
    try:
        response = requests.get(url_fixtures, headers=headers, params=params, timeout=25)
        response.raise_for_status()
        fixtures_data = response.json()
        if fixtures_data.get("response"):
            fixtures.extend(fixtures_data["response"])
            _log(f"Fixtures retornados pela API-Football: {len(fixtures)}")
    except Exception as e:
        _log(f"Erro ao buscar fixtures: {e}")

    if not fixtures:
        _log("Nenhum fixture retornado pela API-Football")
        sports = ["soccer_brazil_campeonato", "soccer_italy_serie_a", "soccer_epl", "soccer_spain_la_liga", "soccer_conmebol_copa_libertadores"]
        for sport in sports:
            url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds?regions={regions}&markets=h2h&dateFormat=iso&oddsFormat=decimal&apiKey={api_key_theodds}"
            try:
                response = requests.get(url, timeout=25)
                response.raise_for_status()
                games = response.json()
                _log(f"TheOddsAPI retornou {len(games)} jogos para {sport}")
                fixtures.extend(games)
            except Exception as e:
                _log(f"Erro ao buscar {sport}: {e}")
        if not fixtures:
            _log("Nenhum dado de fixtures obtido de nenhuma API")
            sys.exit(5)

    fixture_map = {}
    for game in fixtures:
        home_team = normalize_team_name(game["home_team"] if isinstance(game, dict) and "home_team" in game else game["teams"]["home"]["name"])
        away_team = normalize_team_name(game["away_team"] if isinstance(game, dict) and "away_team" in game else game["teams"]["away"]["name"])
        fixture_id = game["id"] if isinstance(game, dict) and "id" in game else game["fixture"]["id"]
        home_matched = match_team(home_team, source_teams, aliases)
        away_matched = match_team(away_team, source_teams, aliases)
        if home_matched and away_matched:
            fixture_map[(home_matched, away_matched)] = fixture_id

    unmatched_csv = set(matches_df.apply(lambda row: (row[home_col], row[away_col]), axis=1)) - set(fixture_map.keys())
    if unmatched_csv:
        _log(f"Jogos do CSV não pareados: {unmatched_csv}")

    url_stats = "https://v3.football.api-sports.io/fixtures/statistics"
    url_injuries = "https://v3.football.api-sports.io/injuries"
    url_lineups = "https://v3.football.api-sports.io/fixtures/lineups"
    for _, row in matches_df.iterrows():
        home_team = row[home_col]
        away_team = row[away_col]
        fixture_id = fixture_map.get((home_team, away_team))
        if not fixture_id:
            continue

        stats_data, injuries_data, lineups_data, odds_data = None, None, None, None
        if fixtures_data and fixtures_data.get("response"):
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
        else:
            for game in fixtures:
                if game["id"] == fixture_id:
                    odds_data = game.get("bookmakers", [{}])[0].get("markets", [{}])[0].get("outcomes", [])
                    break

        stats.append({
            "match_id": fixture_id,
            "team_home": home_team,
            "team_away": away_team,
            "xG_home": stats_data["response"][0]["statistics"].get("xG", 0) if stats_data and stats_data.get("response") and len(stats_data["response"]) >= 2 else 0,
            "xG_away": stats_data["response"][1]["statistics"].get("xG", 0) if stats_data and stats_data.get("response") and len(stats_data["response"]) >= 2 else 0,
            "lesions_home": len(injuries_data["response"][0].get("players", {}).get("injured", [])) if injuries_data and injuries_data.get("response") else 0,
            "lesions_away": len(injuries_data["response"][1].get("players", {}).get("injured", [])) if injuries_data and injuries_data.get("response") else 0,
            "formation_home": lineups_data["response"][0].get("formation", "unknown") if lineups_data and lineups_data.get("response") else "unknown",
            "formation_away": lineups_data["response"][1].get("formation", "unknown") if lineups_data and lineups_data.get("response") else "unknown",
            "odds_home": odds_data[0]["price"] if odds_data and len(odds_data) > 0 else 0,
            "odds_draw": odds_data[1]["price"] if odds_data and len(odds_data) > 1 else 0,
            "odds_away": odds_data[2]["price"] if odds_data and len(odds_data) > 2 else 0
        })

    df = pd.DataFrame(stats)
    if len(df) < 14:
        unmatched_csv = set(matches_df.apply(lambda row: (row[home_col], row[away_col]), axis=1)) - set(df.apply(lambda row: (row['team_home'], row['team_away']), axis=1))
        _log(f"Jogos do CSV não pareados: {unmatched_csv}")
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
    ap.add_argument("--aliases_file", default="data/aliases/auto_aliases.json")
    args = ap.parse_args()

    if not args.api_key or not args.api_key_theodds:
        _log("API_FOOTBALL_KEY ou THEODDS_API_KEY não definida")
        sys.exit(5)

    fetch_stats(args.rodada, args.source_csv, args.api_key, args.aliases_file, args.api_key_theodds, args.regions)

if __name__ == "__main__":
    main()