# scripts/news_ingest_safe.py
from __future__ import annotations
import os, csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

from scripts.csv_utils import count_csv_rows

def _read_matches(rodada: str) -> list[dict]:
    src = Path(f"data/in/{rodada}/matches_source.csv")
    rows: list[dict] = []
    if not src.exists():
        return rows
    with src.open("r", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

def _news_hits(q: str, api_key: str, from_dt: str, to_dt: str, lang: str = "pt") -> int:
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": q,
        "from": from_dt,
        "to": to_dt,
        "language": lang,
        "sortBy": "relevancy",
        "pageSize": 100,
    }
    r = requests.get(url, params=params, headers={"X-Api-Key": api_key}, timeout=20)
    if r.status_code != 200:
        return 0
    data = r.json()
    return int(data.get("totalResults", 0) or 0)

def main() -> None:
    rodada = (os.environ.get("RODADA") or "").strip()
    debug = (os.environ.get("DEBUG") or "false").lower() == "true"
    api_key = (os.environ.get("NEWSAPI_KEY") or "").strip()

    out_dir = Path(f"data/out/{rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "news.csv"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "home", "away", "news_hits"])

        if not api_key:
            print("[news] AVISO: NEWSAPI_KEY ausente — gerando CSV vazio (SAFE).")
            return

        rows = _read_matches(rodada)

        # janela temporal: hoje ± 2 dias (se a planilha não trouxer data exata)
        now_utc = datetime.now(timezone.utc)
        default_from = (now_utc - timedelta(days=2)).strftime("%Y-%m-%d")
        default_to = (now_utc + timedelta(days=2)).strftime("%Y-%m-%d")

        for r in rows:
            mid = (r.get("match_id") or r.get("id") or "").strip()
            home = (r.get("home") or r.get("home_team") or "").strip()
            away = (r.get("away") or r.get("away_team") or "").strip()
            if not home or not away:
                continue

            date = (r.get("date") or "").strip()
            from_dt = date[:10] or default_from
            to_dt = date[:10] or default_to

            q = f'"{home}" "{away}" futebol'
            try:
                hits = _news_hits(q, api_key, from_dt, to_dt)
            except Exception as e:
                if debug:
                    print(f"[news] erro {home} x {away}: {e}")
                hits = 0

            w.writerow([mid, home, away, hits])
            if debug:
                print(f"[news] {home} x {away}: {hits}")

    print(f"[news] OK -> {out_path} ({count_csv_rows(out_path)} linhas)")

if __name__ == "__main__":
    main()
