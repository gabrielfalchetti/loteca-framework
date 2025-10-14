# scripts/build_team_catalog.py
# baixa catálogo de times do API-Football e salva em parquet
from __future__ import annotations
import os
import sys
import time
import argparse
import requests
import pandas as pd
from typing import List, Dict, Any
from _utils_norm import norm_name

API_URL = "https://v3.football.api-sports.io/teams"

def fetch_teams(api_key: str, season: int, country: str) -> List[Dict[str, Any]]:
    headers = {"x-apisports-key": api_key}
    params = {"country": country, "season": season}
    out = []
    page = 1
    while True:
        params["page"] = page
        r = requests.get(API_URL, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(2.0)
            continue
        r.raise_for_status()
        data = r.json()
        resp = data.get("response", [])
        if not resp:
            break
        for item in resp:
            team = item.get("team", {}) or {}
            venue = item.get("venue", {}) or {}
            out.append({
                "team_id": team.get("id"),
                "name": team.get("name"),
                "code": team.get("code"),
                "country": country,
                "founded": team.get("founded"),
                "city": venue.get("city"),
                "name_norm": norm_name(team.get("name","")),
            })
        page += 1
        # API normalmente termina sem "paging.total", mas por segurança:
        if page > 50:
            break
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--countries", type=str, default="Brazil,England,Spain,Italy,France,Germany,Portugal,Argentina")
    ap.add_argument("--out", type=str, default="data/ref/teams_catalog.parquet")
    args = ap.parse_args()

    api_key = os.getenv("API_FOOTBALL_KEY", "")
    if not api_key:
        print("::error::API_FOOTBALL_KEY não configurada")
        sys.exit(1)

    countries = [c.strip() for c in args.countries.split(",") if c.strip()]
    all_rows: List[Dict[str, Any]] = []
    for c in countries:
        try:
            rows = fetch_teams(api_key, args.season, c)
            all_rows.extend(rows)
        except requests.HTTPError as e:
            print(f"[catalog][WARN] Falha {c}: {e}")
        time.sleep(0.25)

    if not all_rows:
        print("::error::catálogo vazio")
        sys.exit(2)

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["team_id"]).reset_index(drop=True)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"[catalog] OK — {len(df)} times salvos em {args.out}")

if __name__ == "__main__":
    main()