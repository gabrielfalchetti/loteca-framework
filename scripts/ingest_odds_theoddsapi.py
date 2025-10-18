# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import requests
import os
import json

def _log(msg: str) -> None:
    print(f"[theoddsapi] {msg}", flush=True)

def fetch_odds(rodada: str, regions: str, source_csv: str, api_key: str) -> pd.DataFrame:
    """Busca odds do TheOddsAPI."""
    url = f"https://api.the-odds-api.com/v4/sports/soccer_brazil_campeonato/odds?regions={regions}&markets=h2h&dateFormat=iso&oddsFormat=decimal&apiKey={api_key}"
    response = requests.get(url)
    data = response.json()

    matches_df = pd.read_csv(source_csv)
    paired = []
    for event in data:
        home_team = event["home_team"]
        away_team = event["away_team"]
        odds = next((m for m in event["bookmakers"][0]["markets"] if m["key"] == "h2h"), None)
        if odds:
            paired.append({
                "team_home": home_team,
                "team_away": away_team,
                "odds_home": odds["outcomes"][0]["price"],
                "odds_draw": odds["outcomes"][1]["price"],
                "odds_away": odds["outcomes"][2]["price"]
            })

    df = pd.DataFrame(paired)
    if df.empty:
        _log("Nenhum jogo pareado — falhando.")
        sys.exit(5)

    out_file = f"{rodada}/odds_theoddsapi.csv"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    df.to_csv(out_file, index=False)
    _log(f"Eventos={len(data)} | jogoselecionados={len(matches_df)} | pareados={len(df)} — salvo em {out_file}")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída")
    ap.add_argument("--regions", required=True, help="Regiões para odds")
    ap.add_argument("--source_csv", required=True, help="CSV com jogos")
    ap.add_argument("--api_key", default=os.getenv("THEODDS_API_KEY"), help="Chave TheOddsAPI")
    args = ap.parse_args()

    if not args.api_key:
        _log("THEODDS_API_KEY não definida")
        sys.exit(5)

    fetch_odds(args.rodada, args.regions, args.source_csv, args.api_key)

if __name__ == "__main__":
    main()