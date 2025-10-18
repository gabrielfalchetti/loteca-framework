# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import requests
import os
import json
from rapidfuzz import fuzz
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[theoddsapi] {msg}", flush=True)

def normalize_team_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = unidecode(name).lower().strip()
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "").replace("/pe", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo").replace("inter de milao", "inter").replace("manchester united", "manchester utd").replace("ldu quito", "ldu")
    return name.capitalize()

def match_team(api_name: str, source_teams: list, aliases: dict, threshold: float = 60) -> str:
    api_norm = normalize_team_name(api_name)
    for source_team in source_teams:
        source_norm = normalize_team_name(source_team)
        if api_norm in aliases.get(source_norm, []) or fuzz.ratio(api_norm, source_norm) > threshold:
            return source_team
    return None

def fetch_odds(rodada: str, source_csv: str, api_key: str, regions: str, aliases_file: str, api_key_apifootball: str) -> pd.DataFrame:
    matches_df = pd.read_csv(source_csv)
    home_col = next((col for col in ['team_home', 'home'] if col in matches_df.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in matches_df.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas")
        sys.exit(6)
    if len(matches_df) != 14:
        _log(f"Arquivo {source_csv} contém {len(matches_df)} jogos, esperado 14")
        sys.exit(6)

    matches_df[home_col] = matches_df[home_col].apply(normalize_team_name)
    matches_df[away_col] = matches_df[away_col].apply(normalize_team_name)
    source_teams = set(matches_df[home_col].tolist() + matches_df[away_col].tolist())

    # Gerar aliases automaticamente usando API-Football
    aliases = {}
    if os.path.exists(aliases_file):
        with open(aliases_file, 'r') as f:
            aliases = json.load(f)
    url_teams = "https://v3.football.api-sports.io/teams"
    headers = {"x-apisports-key": api_key_apifootball}
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

    odds = []
    sports = [
        "soccer_brazil_campeonato",
        "soccer_italy_serie_a",
        "soccer_epl",
        "soccer_spain_la_liga",
        "soccer_conmebol_copa_libertadores",
        "soccer_ecuador_liga_pro"
    ]
    for sport in sports:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds?regions={regions}&markets=h2h&dateFormat=iso&oddsFormat=decimal&apiKey={api_key}"
        try:
            response = requests.get(url, timeout=25)
            response.raise_for_status()
            games = response.json()
            _log(f"TheOddsAPI retornou {len(games)} jogos para {sport}")
            for game in games:
                home_team = normalize_team_name(game["home_team"])
                away_team = normalize_team_name(game["away_team"])
                home_matched = match_team(home_team, source_teams, aliases)
                away_matched = match_team(away_team, source_teams, aliases)
                if home_matched and away_matched:
                    odds_values = next((market for market in game["bookmakers"][0]["markets"] if market["key"] == "h2h"), None) if game.get("bookmakers") else None
                    if odds_values:
                        odds.append({
                            "match_id": game["id"],
                            "team_home": home_matched,
                            "team_away": away_matched,
                            "odds_home": odds_values["outcomes"][0]["price"] if len(odds_values["outcomes"]) > 0 else 0,
                            "odds_draw": odds_values["outcomes"][1]["price"] if len(odds_values["outcomes"]) > 1 else 0,
                            "odds_away": odds_values["outcomes"][2]["price"] if len(odds_values["outcomes"]) > 2 else 0
                        })
        except Exception as e:
            _log(f"Erro ao buscar {sport}: {e}")

    df = pd.DataFrame(odds)
    if len(df) < 14:
        _log(f"Apenas {len(df)} jogos pareados, esperado 14. Verifique times em source_csv.")
        sys.exit(6)

    out_file = f"{rodada}/odds_theoddsapi.csv"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    df.to_csv(out_file, index=False)
    _log(f"Arquivo {out_file} gerado com {len(df)} jogos")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", default=os.getenv("THEODDS_API_KEY"))
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--aliases_file", default="data/aliases/auto_aliases.json")
    ap.add_argument("--api_key_apifootball", default=os.getenv("API_FOOTBALL_KEY"))
    args = ap.parse_args()

    if not args.api_key or not args.api_key_apifootball:
        _log("THEODDS_API_KEY ou API_FOOTBALL_KEY não definida")
        sys.exit(6)

    fetch_odds(args.rodada, args.source_csv, args.api_key, args.regions, args.aliases_file, args.api_key_apifootball)

if __name__ == "__main__":
    main()