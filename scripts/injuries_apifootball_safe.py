# scripts/injuries_apifootball_safe.py
from __future__ import annotations
import os, csv
from pathlib import Path
from datetime import datetime
import requests
from unidecode import unidecode

from scripts.csv_utils import count_csv_rows

RAPID_HOST = "api-football-v1.p.rapidapi.com"

def _get_rapidapi_key() -> str:
    # Priorizar seu segredo configurado como X_RAPIDAPI_KEY; manter compatibilidade com RAPIDAPI_KEY
    return (os.environ.get("X_RAPIDAPI_KEY")
            or os.environ.get("RAPIDAPI_KEY")
            or "").strip()

def _read_matches(rodada: str) -> list[dict]:
    src = Path(f"data/in/{rodada}/matches_source.csv")
    rows: list[dict] = []
    if not src.exists():
        return rows
    with src.open("r", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

def fetch_injuries(league_id: str, season: str, date_iso: str, api_key: str) -> list[dict]:
    url = "https://api-football-v1.p.rapidapi.com/v3/injuries"
    params = {"league": league_id, "season": season, "date": date_iso}
    headers = {
        "x-rapidapi-host": RAPID_HOST,
        "x-rapidapi-key": api_key,
    }
    r = requests.get(url, params=params, headers=headers, timeout=25)
    if r.status_code != 200:
        return []
    try:
        return r.json().get("response", []) or []
    except Exception:
        return []

def _norm(s: str) -> str:
    return unidecode((s or "").strip().lower())

def main() -> None:
    rodada = (os.environ.get("RODADA") or "").strip()
    season = (os.environ.get("SEASON") or "2025").strip()
    league_id = (os.environ.get("APIFOOT_LEAGUE_ID") or "71").strip()  # Série A = 71
    debug = (os.environ.get("DEBUG") or "false").lower() == "true"

    api_key = _get_rapidapi_key()

    out_dir = Path(f"data/out/{rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "injuries.csv"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "home", "away", "home_injuries", "away_injuries"])

        if not api_key:
            print("[inj] AVISO: RAPIDAPI_KEY/X_RAPIDAPI_KEY ausente — CSV vazio (SAFE).")
            return

        matches = _read_matches(rodada)
        today = datetime.utcnow().strftime("%Y-%m-%d")

        for m in matches:
            mid = (m.get("match_id") or m.get("id") or "").strip()
            home = (m.get("home") or m.get("home_team") or "").strip()
            away = (m.get("away") or m.get("away_team") or "").strip()
            date_iso = (m.get("date") or today)[:10]

            home_n = _norm(home)
            away_n = _norm(away)

            home_cnt = away_cnt = 0
            try:
                resp = fetch_injuries(league_id, season, date_iso, api_key)
                for it in resp:
                    tname = _norm((it.get("team") or {}).get("name") or "")
                    if tname == home_n:
                        home_cnt += 1
                    elif tname == away_n:
                        away_cnt += 1
            except Exception as e:
                if debug:
                    print(f"[inj] erro {home} x {away}: {e}")

            w.writerow([mid, home, away, home_cnt, away_cnt])
            if debug:
                print(f"[inj] {home} x {away}: {home_cnt}/{away_cnt}")

    print(f"[inj] OK -> {out_path} ({count_csv_rows(out_path)} linhas)")

if __name__ == "__main__":
    main()
