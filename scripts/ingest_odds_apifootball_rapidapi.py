#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/ingest_odds_apifootball_rapidapi.py — versão corrigida STRICT
Extrai odds 1X2 da API-Football (RapidAPI) com colunas padronizadas:
home, away, odds_home, odds_draw, odds_away
"""

import os, sys, requests, pandas as pd, json

API_URL = "https://api-football-v1.p.rapidapi.com/v3/odds"
HEADERS = {
    "x-rapidapi-host": "api-football-v1.p.rapidapi.com",
    "x-rapidapi-key": os.getenv("X_RAPIDAPI_KEY", "")
}

EXIT_OK = 0
EXIT_FAIL = 99

def log(msg): print(msg, flush=True)
def err(msg): print(f"::error::{msg}", flush=True)

def fetch_odds():
    params = {"bookmaker": "1", "bet": "1X2", "timezone": "America/Sao_Paulo"}
    r = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
    if r.status_code != 200:
        err(f"[apifootball] Falha HTTP {r.status_code}")
        sys.exit(EXIT_FAIL)
    data = r.json().get("response", [])
    rows = []
    for fix in data:
        try:
            home = fix["teams"]["home"]["name"]
            away = fix["teams"]["away"]["name"]
            bets = fix["bookmakers"][0]["bets"][0]["values"]
            odds_map = {b["value"]: float(b["odd"]) for b in bets if "odd" in b}
            rows.append({
                "home": home,
                "away": away,
                "odds_home": odds_map.get("Home"),
                "odds_draw": odds_map.get("Draw"),
                "odds_away": odds_map.get("Away")
            })
        except Exception:
            continue
    return pd.DataFrame(rows)

def main():
    out_dir = os.environ.get("OUT_DIR", "data/out/tmp")
    os.makedirs(out_dir, exist_ok=True)
    df = fetch_odds()
    if df.empty or any(c not in df.columns for c in ["home","away","odds_home","odds_draw","odds_away"]):
        err("[apifootball] Dados incompletos ou vazios.")
        sys.exit(EXIT_FAIL)
    out_path = os.path.join(out_dir, "odds_apifootball.csv")
    df.to_csv(out_path, index=False)
    log(f"[apifootball] ✅ {len(df)} linhas salvas em {out_path}")
    sys.exit(EXIT_OK)

if __name__ == "__main__":
    main()