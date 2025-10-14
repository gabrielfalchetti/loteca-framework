# scripts/ingest_fixtures_apifootball.py
import os, sys, argparse, datetime as dt, requests, csv
from typing import List

API_BASE = "https://v3.football.api-sports.io"

def req(endpoint: str, params: dict):
    headers = {"x-apisports-key": os.environ.get("API_FOOTBALL_KEY", "")}
    r = requests.get(f"{API_BASE}/{endpoint}", headers=headers, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("errors"):
        print(f"[apifootball][WARN] API errors={j['errors']} params={params}", file=sys.stderr)
    return j.get("response", [])

def fetch_fixtures(days: int) -> List[dict]:
    out = []
    today = dt.date.today()
    for i in range(days+1):
        day = today + dt.timedelta(days=i)
        resp = req("fixtures", {"date": day.isoformat(), "timezone": "UTC"})
        for fx in resp:
            status = (fx.get("fixture", {}).get("status", {}) or {}).get("short", "")
            if status in ("NS", "TBD", "PST"):  # not started / to be defined / postponed
                home = fx.get("teams", {}).get("home", {}).get("name")
                away = fx.get("teams", {}).get("away", {}).get("name")
                if home and away:
                    out.append({"home": home, "away": away})
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, required=True)
    ap.add_argument("--out", type=str, required=True)
    args = ap.parse_args()

    if not os.environ.get("API_FOOTBALL_KEY"):
        print("[apifootball][ERROR] API_FOOTBALL_KEY vazio.", file=sys.stderr)
        return 2

    rows = fetch_fixtures(args.days)
    if not rows:
        print("[apifootball][WARN] Nenhum fixture encontrado.", file=sys.stderr)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id","home","away"])
        for i, r in enumerate(rows, start=1):
            w.writerow([i, r["home"], r["away"]])
    print(f"[apifootball] OK — gravado {len(rows)} em {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())