# -*- coding: utf-8 -*-
import argparse, csv, sys, os, datetime as dt, requests
from dateutil.relativedelta import relativedelta

API_URL = "https://v3.football.api-sports.io/fixtures"

def _headers():
    h = {}
    key = os.getenv("API_FOOTBALL_KEY") or os.getenv("X_RAPIDAPI_KEY")
    if key:
        # API-Sports aceita 'x-apisports-key'; via RapidAPI use também host
        h["x-apisports-key"] = key
    return h

def fetch_finished_since(days: int):
    to = dt.datetime.utcnow().date()
    frm = to - dt.timedelta(days=days)
    params = {
        "from": frm.isoformat(),
        "to": to.isoformat(),
        "status": "FT",
    }
    r = requests.get(API_URL, headers=_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("response", [])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since_days", type=int, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    try:
        resp = fetch_finished_since(args.since_days)
    except Exception as e:
        print(f"[update_history][ERROR] {e}", file=sys.stderr)
        # deixamos o workflow criar stub
        sys.exit(1)

    rows = []
    for fx in resp:
        t = fx.get("teams", {})
        g = fx.get("goals", {})
        home = t.get("home", {}).get("name", "")
        away = t.get("away", {}).get("name", "")
        hg = g.get("home", None)
        ag = g.get("away", None)
        date_iso = fx.get("fixture", {}).get("date", "")
        date = date_iso[:10] if date_iso else ""
        if home and away and hg is not None and ag is not None and date:
            rows.append((date, home, away, hg, ag))

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","home","away","home_goals","away_goals"])
        w.writerows(rows)
    print(f"[update_history] OK — gravadas {len(rows)} partidas em {args.out}")

if __name__ == "__main__":
    main()