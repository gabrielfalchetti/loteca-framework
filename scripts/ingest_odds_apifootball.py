# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import requests
import os
from rapidfuzz import fuzz

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
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {
        "x-apisports-key": api_key
    }
    matches_df = pd.read_csv(source_csv)
    source_teams = set(matches_df["team_home"].tolist() + matches_df["team_away"].tolist())
    stats = []
    for _, row in matches_df.iterrows():
        params = {
            "fixture": row["match_id"],
            "season": 2025
        }
        try:
            response = requests.get(url, headers=headers, params=params, timeout=25)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                _log(f"Erro 403: Chave API-Football inválida ou limite excedido para fixture {row['match_id']}. Verifique API_FOOTBALL_KEY.")
            else:
                _log(f"Erro HTTP para fixture {row['match_id']}: {e}")
            continue
        except requests.RequestException as e:
            _log(f"Erro de conexão para fixture {row['match_id']}: {e}")
            continue

        if data.get("response"):
            home_team = match_team(data["response"][0]["team"]["name"], source_teams)
            away_team = match_team(data["response"][1]["team"]["name"], source_teams)
            if home_team and away_team:
                stats.append({
                    "match_id": row["match_id"],
                    "team_home": home_team,
                    "team_away": away_team,
                    "xG_home": data["response"][0]["statistics"].get("xG", 0) if data["response"][0].get("statistics") else 0,
                    "xG_away": data["response"][1]["statistics"].get("xG", 0) if data["response"][1].get("statistics") else 0,
                    "lesions_home": len(data["response"][0].get("injuries", [])),
                    "lesions_away": len(data["response"][1].get("injuries", []))
                })

    df = pd.DataFrame(stats)
    if df.empty:
        _log("Nenhum jogo processado — falhando. Verifique match_id em source_csv ou API_FOOTBALL_KEY.")
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