# scripts/update_history.py
import os, sys, argparse, datetime as dt, requests, csv

API_BASE = "https://v3.football.api-sports.io"

def req(endpoint: str, params: dict):
    headers = {"x-apisports-key": os.environ.get("API_FOOTBALL_KEY", "")}
    r = requests.get(f"{API_BASE}/{endpoint}", headers=headers, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j.get("response", [])

def fetch_finished(from_d: dt.date, to_d: dt.date):
    rows = []
    cur = from_d
    while cur <= to_d:
        resp = req("fixtures", {"date": cur.isoformat(), "timezone": "UTC"})
        for fx in resp:
            st = fx.get("fixture", {}).get("status", {}).get("short", "")
            if st == "FT":
                home = fx.get("teams", {}).get("home", {}).get("name")
                away = fx.get("teams", {}).get("away", {}).get("name")
                goals = fx.get("goals", {})
                hg, ag = goals.get("home"), goals.get("away")
                if home and away and hg is not None and ag is not None:
                    rows.append({
                        "date": fx.get("fixture", {}).get("date", "")[:10],
                        "home": home,
                        "away": away,
                        "home_goals": hg,
                        "away_goals": ag,
                    })
        cur += dt.timedelta(days=1)
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since_days", type=int, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    try_api = bool(os.environ.get("API_FOOTBALL_KEY"))
    rows = []
    if try_api:
        today = dt.date.today()
        from_d = today - dt.timedelta(days=args.since_days)
        print(f"[update_history][INFO] fetching finished fixtures for window {from_d} -> {today} (UTC) via API-Sports...")
        try:
            rows = fetch_finished(from_d, today)
            print(f"[update_history][INFO] API-Sports retornou {len(rows)} partidas finalizadas.")
        except Exception as e:
            print(f"[update_history][WARN] Falha na API: {e}", file=sys.stderr)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    if not rows:
        print("[update_history][WARN] Nenhuma partida encontrada — salvando stub BOOT x BOOT 0-0.", file=sys.stderr)
        rows = [{"date":"1970-01-01","home":"BOOT","away":"BOOT","home_goals":0,"away_goals":0}]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["date","home","away","home_goals","away_goals"])
        wr.writeheader()
        wr.writerows(rows)
    print(f"[update_history] OK — gravadas {len(rows)} partidas em {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())