#!/usr/bin/env python
from __future__ import annotations

# HOTFIX de import p/ end2end
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse, csv
from typing import Dict, Any, List
from utils.apifootball import resolve_league_id, resolve_current_season, find_fixture_id, fetch_odds_by_fixture, ApiFootballError
from utils.match_normalize import canonical

LEAGUES = ["Serie A","Serie B","Serie C","Serie D"]

def read_matches(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print(f"[ERRO] Arquivo não encontrado: {path}"); raise SystemExit(2)
    rows = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        needed = {"match_id","home","away"}
        if not needed.issubset(reader.fieldnames or []):
            print("Error: matches_source.csv precisa de colunas: match_id,home,away[,date].")
            raise SystemExit(2)
        rows.extend(reader)
    return rows

def infer_date_iso(m: Dict[str,str]) -> str:
    from datetime import datetime
    return (m.get("date") or datetime.utcnow().date().isoformat())[:10]

def flatten_apifootball_odds(resp: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat = []
    for entry in resp:
        teams = entry.get("teams", {}) or {}
        fixture = entry.get("fixture", {}) or {}
        home = teams.get("home", {}).get("name") or fixture.get("teams", {}).get("home", {}).get("name")
        away = teams.get("away", {}).get("name") or fixture.get("teams", {}).get("away", {}).get("name")
        for bm in (entry.get("bookmakers") or []):
            bname = bm.get("name")
            for bet in (bm.get("bets") or []):
                market = bet.get("name")
                for val in (bet.get("values") or []):
                    flat.append({
                        "prov_home": home, "prov_away": away,
                        "bookmaker": bname, "market": market,
                        "selection": val.get("value"), "price": val.get("odd")
                    })
    return flat

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    base_in = Path("data/in") / args.rodada
    base_out = Path("data/out") / args.rodada
    base_out.mkdir(parents=True, exist_ok=True)

    matches = read_matches(base_in / "matches_source.csv")

    leagues: Dict[str,int] = {}; seasons: Dict[str,int] = {}
    for lname in LEAGUES:
        try:
            lid = resolve_league_id(country="Brazil", league_name=lname)
            leagues[lname] = lid; seasons[lname] = resolve_current_season(lid)
        except Exception as e:
            print(f"[apifootball] AVISO: não consegui resolver {lname}: {e}")

    collected: List[Dict[str, Any]] = []; unmatched: List[Dict[str, Any]] = []

    for m in matches:
        date_iso = infer_date_iso(m)
        fixture_id = None
        for lname, lid in leagues.items():
            season = seasons.get(lname); 
            if not season: continue
            try:
                fixture_id = find_fixture_id(date_iso, m["home"], m["away"], lid, season)
                if fixture_id:
                    if args.debug:
                        print(f"[apifootball] {m['match_id']} → fixture={fixture_id} ({lname} {season})")
                    break
            except ApiFootballError as e:
                print(f"[apifootball] AVISO find_fixture_id {lname}: {e}")
        if not fixture_id:
            unmatched.append({"match_id": m["match_id"], "home": m["home"], "away": m["away"], "motivo": "fixture_nao_encontrado"})
            continue

        try:
            resp = fetch_odds_by_fixture(fixture_id)
            flat = flatten_apifootball_odds(resp)
            if not flat:
                print(f"[apifootball] sem odds p/ {m['match_id']} '{m['home']}' vs '{m['away']}'")
                unmatched.append({"match_id": m["match_id"], "home": m["home"], "away": m["away"], "motivo": "sem_odds_no_fixture"})
                continue
            mh, ma = canonical(m["home"]), canonical(m["away"])
            for r in flat:
                ph, pa = canonical(r["prov_home"] or ""), canonical(r["prov_away"] or "")
                if (mh == ph and ma == pa):
                    collected.append({
                        "match_id": m["match_id"], "home": m["home"], "away": m["away"],
                        "bookmaker": r["bookmaker"], "market": r["market"],
                        "selection": r["selection"], "price": r["price"]
                    })
        except ApiFootballError as e:
            print(f"[apifootball] ERRO odds fixture={fixture_id}: {e}")

    out_csv = base_out / "odds_apifootball.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","bookmaker","market","selection","price"])
        wr.writeheader(); wr.writerows(collected)

    if unmatched:
        um_csv = base_out / "unmatched_apifootball.csv"
        with um_csv.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=["match_id","home","away","motivo"])
            wr.writeheader(); wr.writerows(unmatched)
        print(f"[apifootball] AVISO: {len(unmatched)} sem odds/fixture → {um_csv}")

    print(f"[apifootball] OK -> {out_csv} ({len(collected)} linhas)")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
