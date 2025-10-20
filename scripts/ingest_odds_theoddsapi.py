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
    print(f"[theoddsapi] {msg}", flush=True)

def normalize_team_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = unidecode(name).lower().strip()
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "").replace("/pe", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo").replace("inter de milao", "inter").replace("manchester united", "manchester utd")
    name = name.replace("sport recife", "sport").replace("atletico mineiro", "atlético").replace("bragantino-sp", "bragantino").replace("vasco da gama", "vasco")
    name = name.replace("fluminense", "fluminense").replace("santos", "santos").replace("vitoria", "vitória").replace("mirassol", "mirassol").replace("gremio", "grêmio")
    name = name.replace("juventude", "juventude").replace("roma", "roma").replace("getafe", "getafe").replace("real madrid", "real madrid").replace("liverpool", "liverpool")
    name = name.replace("atalanta bergamas", "atalanta").replace("fiorentina", "fiorentina").replace("osasuna", "osasuna")
    return name.capitalize()

def match_team(api_name: str, source_teams: list, aliases: dict, threshold: float = 70) -> str:
    api_norm = normalize_team_name(api_name).lower()
    for source_team in source_teams:
        source_norm = normalize_team_name(source_team).lower()
        if api_norm in [normalize_team_name(alias).lower() for alias in aliases.get(source_norm, [])]:
            _log(f"Match encontrado para {api_name} -> {source_team} (alias direto)")
            return source_team
        score = fuzz.ratio(api_norm, source_norm)
        if score > threshold:
            _log(f"Match encontrado para {api_name} -> {source_team} (score={score})")
            return source_team
    _log(f"Sem match para {api_name}")
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
    _log(f"Times no CSV após normalização: {source_teams}")

    aliases = {}
    if os.path.exists(aliases_file):
        with open(aliases_file, 'r') as f:
            aliases = json.load(f)
    else:
        _log(f"Arquivo de aliases {aliases_file} não encontrado")
        sys.exit(6)

    odds = []
    sports = [
        "soccer_brazil_campeonato",
        "soccer_brazil_serie_b",
        "soccer_italy_serie_a",
        "soccer_epl",
        "soccer_spain_la_liga",
        "soccer_conmebol_copa_libertadores"
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
                
                # Verificar se o jogo pareado existe no CSV original
                if home_matched and away_matched:
                    game_tuple = (home_matched, away_matched)
                    csv_tuples = matches_df.apply(lambda row: (row[home_col], row[away_col]), axis=1).tolist()
                    if game_tuple in csv_tuples:
                        # Extrair odds do primeiro bookmaker com mercado h2h
                        odds_values = None
                        if game.get("bookmakers"):
                            for bookmaker in game["bookmakers"]:
                                for market in bookmaker.get("markets", []):
                                    if market.get("key") == "h2h":
                                        odds_values = market.get("outcomes", [])
                                        break
                                if odds_values:
                                    break
                        
                        if odds_values and len(odds_values) >= 3:
                            odds.append({
                                "match_id": game["id"],
                                "team_home": home_matched,
                                "team_away": away_matched,
                                "odds_home": odds_values[0]["price"],
                                "odds_draw": odds_values[1]["price"],
                                "odds_away": odds_values[2]["price"]
                            })
                            _log(f"Jogo pareado: {home_matched} x {away_matched} (match_id={game['id']})")
        except Exception as e:
            _log(f"Erro ao buscar {sport}: {e}")

    df = pd.DataFrame(odds)
    if len(df) == 0:
        _log("Nenhum jogo pareado encontrado")
        sys.exit(6)

    # CORREÇÃO: Usar .tolist() em vez de .to_list()
    matches_set = set(matches_df.apply(lambda row: (row[home_col], row[away_col]), axis=1).tolist())
    df_set = set(df.apply(lambda row: (row['team_home'], row['team_away']), axis=1).tolist())
    
    if len(df) < 14:
        unmatched_csv = matches_set - df_set
        _log(f"Jogos do CSV não pareados: {unmatched_csv}")

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

    if not args.api_key:
        _log("THEODDS_API_KEY não definida")
        sys.exit(6)

    fetch_odds(args.rodada, args.source_csv, args.api_key, args.regions, args.aliases_file, args.api_key_apifootball)

if __name__ == "__main__":
    main()