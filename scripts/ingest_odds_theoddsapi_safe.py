#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ingest_odds_theoddsapi_safe.py
---------------------------------
Coleta odds 1X2 de múltiplas regiões via TheOddsAPI.
Gera data/out/<rodada>/odds_theoddsapi.csv
Compatível com consensus_odds_safe.py (usa team_home, team_away, match_key)
"""

import os
import sys
import csv
import json
import time
import argparse
import requests
import pandas as pd
from datetime import datetime, timezone

API_URL = "https://api.the-odds-api.com/v4/sports/soccer/odds"


def safe_filename(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def log(msg):
    print(f"[theoddsapi] {msg}", flush=True)


def fetch_odds(api_key, regions, debug=False):
    all_rows = []
    for reg in regions.split(","):
        params = {"apiKey": api_key, "regions": reg, "markets": "h2h"}
        try:
            if debug:
                print(f"[theoddsapi][DEBUG] GET {API_URL} {params}")
            r = requests.get(API_URL, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log(f"ERRO região {reg}: {e}")
            continue

        for ev in data:
            home = ev.get("home_team")
            away = ev.get("away_team")
            odds = ev.get("bookmakers", [{}])[0].get("markets", [{}])[0].get("outcomes", [])
            if not home or not away or len(odds) < 2:
                continue

            row = {
                "match_id": len(all_rows) + 1,
                "team_home": home,
                "team_away": away,
                "match_key": f"{home.lower().replace(' ', '-') }__vs__{away.lower().replace(' ', '-')}",
                "region": reg,
                "sport": ev.get("sport_key", "soccer"),
                "odds_home": odds[0].get("price") if len(odds) > 0 else None,
                "odds_draw": odds[1].get("price") if len(odds) > 1 else None,
                "odds_away": odds[2].get("price") if len(odds) > 2 else None,
                "last_update": ev.get("commence_time"),
                "source": "theoddsapi",
            }
            all_rows.append(row)
        time.sleep(0.5)
    return pd.DataFrame(all_rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="Diretório de saída da rodada (ex: data/out/1234)")
    p.add_argument("--regions", default="uk,eu,us,au", help="Regiões da API")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    api_key = os.getenv("THEODDS_API_KEY")
    if not api_key:
        print("::error::THEODDS_API_KEY não definido em Secrets/Vars.")
        sys.exit(4)

    df = fetch_odds(api_key, args.regions, args.debug)
    out_path = safe_filename(os.path.join(args.rodada, "odds_theoddsapi.csv"))
    if df.empty:
        log("AVISO: nenhum dado retornado da API.")
        df = pd.DataFrame(columns=[
            "match_id","team_home","team_away","match_key","region","sport",
            "odds_home","odds_draw","odds_away","last_update","source"
        ])
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    log(f"OK -> {out_path} (linhas={len(df)})")

    if args.debug:
        print(df.head(10))


if __name__ == "__main__":
    main()