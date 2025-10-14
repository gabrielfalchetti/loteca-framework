# -*- coding: utf-8 -*-
import argparse, os, sys, csv, requests, statistics
from _utils_norm import norm_name
from rapidfuzz import fuzz

def best_h2h(books):
    # retorna odds_home, odds_draw, odds_away via mediana across bookmakers (robusto)
    hh, dd, aa = [], [], []
    for b in books or []:
        for m in b.get("markets", []):
            if m.get("key") == "h2h":
                prices = m.get("outcomes", [])
                # outcomes: [{name:"Team A", price:1.95}, {name:"Team B"...}] ou com "Draw"
                # Vamos só guardar preços e depois mapeamos nomes pelo evento
                names = [o.get("name","").strip() for o in prices]
                vals  = [float(o.get("price",0)) for o in prices]
                if len(vals)==2: # sem empate
                    hh.append(vals[0]); aa.append(vals[1])
                elif len(vals)==3:
                    hh.append(vals[0]); dd.append(vals[1]); aa.append(vals[2])
    def med(L): 
        return round(statistics.median(L), 4) if L else None
    return med(hh), med(dd), med(aa)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", required=True)
    ap.add_argument("--source_csv", required=True)
    args = ap.parse_args()

    api_key = os.getenv("THEODDS_API_KEY")
    if not api_key:
        print("[theoddsapi][ERROR] THEODDS_API_KEY vazio.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.rodada, exist_ok=True)
    out_csv = os.path.join(args.rodada, "odds_theoddsapi.csv")

    try:
        url = "https://api.the-odds-api.com/v4/sports/upcoming/odds"
        params = {
            "apiKey": api_key,
            "regions": args.regions,
            "markets": "h2h",
            "oddsFormat": "decimal",
            "sport": "soccer"  # alguns proxies aceitam esse filtro; se ignorado, filtramos depois
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        print(f"[theoddsapi][ERROR] Falha ao consultar TheOddsAPI: {e}", file=sys.stderr)
        # ainda assim, gravamos arquivo vazio para debugging
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["team_home","team_away","odds_home","odds_draw","odds_away","commence_time"])
        sys.exit(1)

    # Le a whitelist / matches_norm
    import pandas as pd
    src = pd.read_csv(args.source_csv)
    src["home_norm"] = src["home_norm"].astype(str)
    src["away_norm"] = src["away_norm"].astype(str)

    rows = []
    matched = 0

    # normaliza e tenta casar
    for ev in events:
        sport_key = ev.get("sport_key","")
        if "soccer" not in sport_key:
            continue
        home = ev.get("home_team","") or ""
        away = ev.get("away_team","") or ""
        hn, an = norm_name(home), norm_name(away)
        oh, od, oa = best_h2h(ev.get("bookmakers", []))
        if not oh or not oa:
            continue
        # fuzzy match contra src
        candidates = src[(src["home_norm"]==hn) & (src["away_norm"]==an)]
        if candidates.empty:
            # Tenta fuzzy
            src["score"] = src.apply(lambda r: (fuzz.token_set_ratio(hn, r["home_norm"]) + 
                                                fuzz.token_set_ratio(an, r["away_norm"])) / 2.0, axis=1)
            mx = src["score"].max()
            if mx >= 90:
                candidates = src[src["score"]==mx]
            src.drop(columns=["score"], inplace=True, errors="ignore")
        if not candidates.empty:
            r0 = candidates.iloc[0]
            rows.append([r0["home"], r0["away"], oh, od, oa, ev.get("commence_time","")])
            matched += 1

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["team_home","team_away","odds_home","odds_draw","odds_away","commence_time"])
        w.writerows(rows)

    print(f"[theoddsapi]Arquivo odds_theoddsapi.csv gerado com {matched} jogos pareados.")

if __name__ == "__main__":
    main()