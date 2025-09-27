# scripts/features_matchstats.py
# Coleta estatísticas de jogos (via API-Football / RapidAPI) e salva features por jogo
from __future__ import annotations
import argparse, os, requests
import pandas as pd
from pathlib import Path

API_HOST = "api-football-v1.p.rapidapi.com"
API_URL = f"https://{API_HOST}/v3/fixtures"

def fetch_stats(fixture_id: int, headers: dict) -> dict:
    url = f"https://{API_HOST}/v3/fixtures/statistics?fixture={fixture_id}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        return {}
    data = r.json().get("response", [])
    stats = {}
    for team_stats in data:
        team_id = team_stats.get("team", {}).get("id")
        for s in team_stats.get("statistics", []):
            key = s["type"].lower().replace(" ", "_")
            stats[f"{team_id}_{key}"] = s["value"]
    return stats

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    rodada = args.rodada
    base = Path(f"data/out/{rodada}")
    base.mkdir(parents=True, exist_ok=True)
    matches_path = base / "matches.csv"
    if not matches_path.exists():
        raise RuntimeError(f"[matchstats] matches.csv ausente: {matches_path}")

    matches = pd.read_csv(matches_path)
    if "fixture_id" not in matches.columns:
        raise RuntimeError("[matchstats] matches.csv precisa ter col fixture_id (mapeado antes pelo ingest_odds_apifootball).")

    key = os.getenv("RAPIDAPI_KEY")
    if not key:
        raise RuntimeError("[matchstats] RAPIDAPI_KEY ausente nos Secrets.")

    headers = {"X-RapidAPI-Key": key, "X-RapidAPI-Host": API_HOST}
    rows = []
    for _, r in matches.iterrows():
        fid = int(r["fixture_id"])
        stats = fetch_stats(fid, headers)
        row = {"match_id": r["match_id"], "home": r["home"], "away": r["away"], **stats}
        rows.append(row)

    df = pd.DataFrame(rows)
    out_path = base / "matchstats.csv"
    df.to_csv(out_path, index=False)
    print(f"[matchstats] OK -> {out_path}")

if __name__ == "__main__":
    main()
