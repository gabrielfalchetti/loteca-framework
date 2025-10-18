# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import requests
import os
import json  # Added missing import for json.dumps
from rapidfuzz import fuzz
from datetime import datetime, timedelta

"""
Busca estatísticas da API-Football para os jogos da Loteca, usando fuzzy matching para parear times.
"""

def _log(msg: str) -> None:
    print(f"[apifootball] {msg}", flush=True)

def match_team(api_name: str, source_teams: list, threshold: float = 80) -> str:
    """Pareia nomes de times usando fuzzy matching."""
    for source_team in source_teams:
        if fuzz.ratio(api_name.lower(), source_team.lower()) > threshold:
            return source_team
    return None

def fetch_stats(rodada: str, source_csv: str, api_key: str) -> pd.DataFrame:
    """Busca estatísticas da API-Football para os jogos."""
    matches_df = pd.read_csv(source_csv)
    
    # Verificação de colunas
    home_col = 'team_home' if 'team_home' in matches_df.columns else 'home' if 'home' in matches_df.columns else None
    away_col = 'team_away' if 'team_away' in matches_df.columns else 'away' if 'away' in matches_df.columns else None
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas em source_csv")
        sys.exit(5)

    source_teams = set(matches_df[home_col].tolist() + matches_df[away_col].tolist())
    stats = []
    
    # Buscar fixtures para encontrar match_id válidos
    url_fixtures = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key}
    since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    until = (datetime.utcnow() + timedelta(days=60)).strftime("%Y-%m-%d")
    params = {
        "from": since,
        "to": until,
        "season": 2024,  # Changed to 2024 as 2025 may not have data yet
        "league": "71,72,203,70,39,140,13,2"  # Série A, Série B, Copa do Brasil, Carioca, Premier League, La Liga, Libertadores, Champions League
    }
    
    try:
        response = requests.get(url_fixtures, headers=headers, params=params, timeout=25)
        response.raise_for_status()
        fixtures_data = response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 403:
            _log(f"Erro 403: Chave API-Football inválida ou limite excedido. Verifique API_FOOTBALL_KEY.")
        else:
            _log(f"Erro HTTP ao buscar fixtures: {e}")
        sys.exit(5)
    except requests.RequestException as e:
        _log(f"Erro de conexão ao buscar fixtures: {e}")
        sys.exit(5)

    if not fixtures_data.get("response"):
        _log("Nenhum fixture retornado pela API-Football para ligas 71,72,203,70,39,140,13,2 no período {} a {}".format(since, until))
        sys.exit(5)

    # Logar fixtures retornados
    _log(f"Fixtures retornados: {len(fixtures_data['response'])}")
    for game in fixtures_data["response"][:5]:  # Logar primeiros 5 para depuração
        _log(f"Fixture ID: {game['fixture']['id']}, Jogo: {game['teams']['home']['name']} x {game['teams']['away']['name']}")

    # Mapear match_id por time
    fixture_map = {}
    for game in fixtures_data["response"]:
        home_team = game["teams"]["home"]["name"]
        away_team = game["teams"]["away"]["name"]
        fixture_id = game["fixture"]["id"]
        home_matched = match_team(home_team, source_teams)
        away_matched = match_team(away_team, source_teams)
        if home_matched and away_matched:
            fixture_map[(home_matched, away_matched)] = fixture_id
        else:
            _log(f"Não pareado: {home_team} x {away_team}")

    # Buscar stats para cada jogo
    url_stats = "https://v3.football.api-sports.io/fixtures/statistics"
    for _, row in matches_df.iterrows():
        home_team = row[home_col]
        away_team = row[away_col]
        fixture_id = fixture_map.get((home_team, away_team))
        if not fixture_id:
            _log(f"Fixture não encontrado para {home_team} x {away_team}")
            continue

        params = {"fixture": fixture_id}
        try:
            response = requests.get(url_stats, headers=headers, params=params, timeout=25)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                _log(f"Erro 403 para fixture {fixture_id}: Chave API-Football inválida.")
            else:
                _log(f"Erro HTTP para fixture {fixture_id}: {e}")
            continue
        except requests.RequestException as e:
            _log(f"Erro de conexão para fixture {fixture_id}: {e}")
            continue

        if data.get("response") and len(data["response"]) >= 2:
            home_matched = match_team(data["response"][0]["team"]["name"], source_teams)
            away_matched = match_team(data["response"][1]["team"]["name"], source_teams)
            if home_matched and away_matched:
                stats.append({
                    "match_id": fixture_id,
                    "team_home": home_matched,
                    "team_away": away_matched,
                    "xG_home": data["response"][0]["statistics"].get("xG", 0) if data["response"][0].get("statistics") else 0,
                    "xG_away": data["response"][1]["statistics"].get("xG", 0) if data["response"][1].get("statistics") else 0,
                    "lesions_home": len(data["response"][0].get("players", {}).get("injured", [])),
                    "lesions_away": len(data["response"][1].get("players", {}).get("injured", []))
                })

    df = pd.DataFrame(stats)
    if df.empty:
        _log("Nenhum jogo processado — falhando. Verifique times em source_csv, datas ou API_FOOTBALL_KEY.")
        sys.exit(5)

    out_file = f"{rodada}/odds_apifootball.csv"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    df.to_csv(out_file, index=False)
    _log(f"Arquivo {out_file} gerado com {len(df)} jogos encontrados")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída")
    ap.add_argument("--source_csv", required=True, help="CSV com jogos")
    ap.add_argument("--api_key", default=os.getenv("API_FOOTBALL_KEY"), help="Chave API-Football")
    args = ap.parse_args()

    if not args.api_key:
        _log("API_FOOTBALL_KEY não definida")
        sys.exit(5)

    fetch_stats(args.rodada, args.source_csv, args.api_key)

if __name__ == "__main__":
    main()