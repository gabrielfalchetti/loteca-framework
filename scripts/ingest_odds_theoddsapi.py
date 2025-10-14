# scripts/ingest_odds_theoddsapi.py
import os, sys, argparse, requests, csv, re
from rapidfuzz import fuzz
from unidecode import unidecode

API = "https://api.the-odds-api.com/v4/sports/upcoming/odds"

def norm(s: str) -> str:
    s = unidecode(s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def fetch_upcoming(regions: str, apikey: str):
    params = {
        "apiKey": apikey,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal"
    }
    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    return [e for e in r.json() if (e.get("sport_key","").startswith("soccer_"))]

def best_match(home_t, away_t, events):
    # devolve (event, score)
    top = (None, 0)
    h0, a0 = norm(home_t), norm(away_t)
    for e in events:
        comp = e.get("home_team"), [t for t in e.get("away_team",""), e.get("commence_time","")]
        home = e.get("home_team","")
        away = ""
        # TheOdds define "home_team" e o outro vem em "away_team" implícito na lista de "teams"
        teams = e.get("teams") or []
        if len(teams)==2:
            away = teams[1] if teams[0]==home else teams[0]
        h1, a1 = norm(home), norm(away)
        s = (fuzz.token_sort_ratio(h0, h1) + fuzz.token_sort_ratio(a0, a1)) / 2
        if s > top[1]:
            top = (e, s)
    return top

def consensus_prices(e):
    # agrega odds H2H (1X2) por mediana simples
    import statistics as stats
    prices = {"home": [], "draw": [], "away": []}
    for bk in e.get("bookmakers", []):
        for mk in bk.get("markets", []):
            if mk.get("key") == "h2h":
                outcomes = mk.get("outcomes", [])
                # outcomes: [{name:'Team A', price:1.8}, {name:'Team B', price:2.0}, {name:'Draw', price:3.2}]
                # Mapear por nome...
                for oc in outcomes:
                    nm = oc.get("name","")
                    pr = oc.get("price")
                    if pr is None: 
                        continue
                    n = norm(nm)
                    if "draw" in n or "empate" in n:
                        prices["draw"].append(float(pr))
                    # como saber quem é home ou away?
                    # comparando com event teams:
                # fallback heurístico: se 3 outcomes, ordenar alfabeticamente e assumir [home,away,draw] NÃO é seguro.
                # Melhor: reprocessar outcomes com nomes vs event home/away:
                home = norm(e.get("home_team",""))
                teams = e.get("teams") or []
                if len(teams)==2:
                    other = teams[1] if teams[0]==e.get("home_team") else teams[0]
                    away = norm(other)
                    for oc in outcomes:
                        nm = norm(oc.get("name",""))
                        pr = oc.get("price")
                        if pr is None: continue
                        if nm == home:
                            prices["home"].append(float(pr))
                        elif nm == away:
                            prices["away"].append(float(pr))
                        elif "draw" in nm or "empate" in nm:
                            if float(pr) not in prices["draw"]:
                                prices["draw"].append(float(pr))
    def med(x): 
        try:
            from statistics import median
            return round(median(x), 4)
        except Exception:
            return ""
    return med(prices["home"]), med(prices["draw"]), med(prices["away"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", required=True)
    ap.add_argument("--source_csv", required=True)
    args = ap.parse_args()

    key = os.environ.get("THEODDS_API_KEY", "")
    if not key:
        print("[theoddsapi][ERROR] THEODDS_API_KEY vazio.", file=sys.stderr)
        return 2

    assert os.path.exists(args.source_csv), f"{args.source_csv} not found"

    # whitelist
    wl = []
    with open(args.source_csv, encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            wl.append({"match_id": r["match_id"], "home": r["home"], "away": r["away"]})

    events = fetch_upcoming(args.regions, key)
    print(f"[theoddsapi]Total de {len(events)} eventos (soccer). Fazendo matching com {len(wl)} jogos...")

    rows = []
    for r in wl:
        ev, score = best_match(r["home"], r["away"], events)
        if not ev or score < 80:  # limiar conservador
            continue
        oh, od, oa = consensus_prices(ev)
        if not (oh and oa and od):
            continue
        rows.append({
            "match_id": r["match_id"],
            "team_home": r["home"],
            "team_away": r["away"],
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
        })

    os.makedirs(args.rodada, exist_ok=True)
    outp = os.path.join(args.rodada, "odds_theoddsapi.csv")
    with open(outp, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","team_home","team_away","odds_home","odds_draw","odds_away"])
        wr.writeheader()
        wr.writerows(rows)

    print(f"[theoddsapi]Arquivo odds_theoddsapi.csv gerado com {len(rows)} jogos pareados.")
    return 0

if __name__ == "__main__":
    sys.exit(main())