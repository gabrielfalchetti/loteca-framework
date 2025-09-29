#!/usr/bin/env python
# scripts/ingest_odds_apifootball_rapidapi.py
# Resolve league/season/fixture, coleta odds via /odds?fixture= e grava CSV.

from __future__ import annotations
import argparse, csv, os, sys
from pathlib import Path
from typing import Dict, Any, List, Tuple
from utils.apifootball import resolve_league_id, resolve_current_season, find_fixture_id, fetch_odds_by_fixture, ApiFootballError
from utils.match_normalize import canonical, ALIASES

LEAGUES = ["Serie A", "Serie B", "Serie C", "Serie D"]  # buscamos nas quatro automaticamente

def read_matches(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print(f"[ERRO] Arquivo não encontrado: {path}", file=sys.stderr)
        sys.exit(2)
    rows = []
    import csv
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        needed = {"match_id", "home", "away"}
        if not needed.issubset(reader.fieldnames or []):
            print("Error: matches_source.csv precisa de colunas: match_id,home,away[,date].", file=sys.stderr)
            sys.exit(2)
        for r in reader:
            rows.append(r)
    return rows

def infer_date_iso(m: Dict[str,str]) -> str:
    # Se houver coluna date, use; caso não, assuma hoje (não ideal, mas evita crash).
    from datetime import datetime
    if m.get("date"):
        return m["date"][:10]
    return datetime.utcnow().date().isoformat()

def flatten_apifootball_odds(resp: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat = []
    # Estrutura comum: response: [ { league, fixture, bookmakers: [ {name, bets: [ {name, values: [..]} ] } ] } ]
    for entry in resp:
        fixture = entry.get("fixture", {})
        teams = entry.get("teams", {})
        bms = entry.get("bookmakers", []) or entry.get("bookmakers", [])
        home = teams.get("home", {}).get("name")
        away = teams.get("away", {}).get("name")
        if not home or not away:
            # alguns formatos retornam odds sem teams; tente dentro de fixture
            home = fixture.get("teams", {}).get("home", {}).get("name", home)
            away = fixture.get("teams", {}).get("away", {}).get("name", away)
        for bm in (entry.get("bookmakers") or []):
            bname = bm.get("name")
            for bet in (bm.get("bets") or []):
                market = bet.get("name")  # ex.: "Match Winner"
                for val in (bet.get("values") or []):
                    flat.append({
                        "prov_home": home, "prov_away": away,
                        "bookmaker": bname,
                        "market": market,
                        "selection": val.get("value"),
                        "price": val.get("odd")
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

    ms_path = base_in / "matches_source.csv"
    matches = read_matches(ms_path)

    # Pré-resolver ids de ligas BR
    leagues: Dict[str, int] = {}
    seasons: Dict[str, int] = {}
    for lname in LEAGUES:
        try:
            lid = resolve_league_id(country="Brazil", league_name=lname)
            leagues[lname] = lid
            seasons[lname] = resolve_current_season(lid)
        except Exception as e:
            print(f"[apifootball] AVISO: não consegui resolver {lname}: {e}")

    collected_rows: List[Dict[str, Any]] = []
    unmatched_rows: List[Dict[str, Any]] = []

    for m in matches:
        date_iso = infer_date_iso(m)
        fixture_id = None
        # tentar em todas as ligas BR resolvidas
        for lname, lid in leagues.items():
            season = seasons.get(lname)
            if not season:
                continue
            try:
                fixture_id = find_fixture_id(date_iso, m["home"], m["away"], lid, season)
                if fixture_id:
                    if args.debug:
                        print(f"[apifootball] match_id={m['match_id']} → fixture={fixture_id} ({lname} {season})")
                    break
            except ApiFootballError as e:
                print(f"[apifootball] AVISO: find_fixture_id falhou {lname}: {e}")
        if not fixture_id:
            unmatched_rows.append({"match_id": m["match_id"], "home": m["home"], "away": m["away"], "motivo": "fixture_nao_encontrado"})
            continue

        try:
            odds_resp = fetch_odds_by_fixture(fixture_id)
            flat = flatten_apifootball_odds(odds_resp)
            if not flat:
                print(f"[apifootball] sem odds p/ match_id={m['match_id']} '{m['home']}' vs '{m['away']}'")
                # mesmo sem odds, registre no unmatched pra auditoria
                unmatched_rows.append({"match_id": m["match_id"], "home": m["home"], "away": m["away"], "motivo": "sem_odds_no_fixture"})
                continue

            # Filtrar linhas do provedor que casam com esse jogo (normalização por garantia)
            from utils.match_normalize import canonical, fuzzy_match
            mh, ma = canonical(m["home"]), canonical(m["away"])
            for r in flat:
                ph, pa = canonical(r["prov_home"] or ""), canonical(r["prov_away"] or "")
                okay = (mh == ph and ma == pa)
                if not okay:
                    # tentar fuzzy leve
                    from utils.match_normalize import fuzzy_match
                    if fuzzy_match(m["home"], [ph]) and fuzzy_match(m["away"], [pa]):
                        okay = True
                if okay:
                    collected_rows.append({
                        "match_id": m["match_id"],
                        "home": m["home"],
                        "away": m["away"],
                        "bookmaker": r["bookmaker"],
                        "market": r["market"],
                        "selection": r["selection"],
                        "price": r["price"]
                    })
        except ApiFootballError as e:
            print(f"[apifootball] ERRO ao buscar odds fixture={fixture_id}: {e}")

    out_csv = base_out / "odds_apifootball.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","bookmaker","market","selection","price"])
        wr.writeheader()
        wr.writerows(collected_rows)

    if unmatched_rows:
        um_csv = base_out / "unmatched_apifootball.csv"
        with um_csv.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=["match_id","home","away","motivo"])
            wr.writeheader()
            wr.writerows(unmatched_rows)
        print(f"[apifootball] AVISO: {len(unmatched_rows)} casos sem odds/fixture. Veja {um_csv}")

    print(f"[apifootball] OK -> {out_csv} ({len(collected_rows)} linhas)")
    # Não falhe o job se 0 linhas: deixe o consenso decidir
    sys.exit(0)

if __name__ == "__main__":
    main()
