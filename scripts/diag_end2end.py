#!/usr/bin/env python
from __future__ import annotations

# HOTFIX de import
import sys, os, json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse, csv, datetime as dt
from utils.oddsapi import fetch_sports, resolve_brazil_soccer_sport_keys, OddsApiError
from utils.apifootball import resolve_league_id, resolve_current_season, find_fixture_id, ApiFootballError
from utils.match_normalize import canonical

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    base_in = Path("data/in")/args.rodada
    ms = base_in/"matches_source.csv"
    if not ms.exists():
        print(f"[diag] ERRO: {ms} inexistente"); raise SystemExit(2)

    # 1) Variáveis de ambiente
    print("[diag] Checando chaves…")
    the = os.environ.get("THEODDSAPI_KEY"); rap = os.environ.get("RAPIDAPI_KEY")
    print(f"  THEODDSAPI_KEY={'OK' if the else 'FALTA'}  RAPIDAPI_KEY={'OK' if rap else 'FALTA'}")

    # 2) Validar CSV (tem data?)
    rows = list(csv.DictReader(ms.open("r", encoding="utf-8")))
    has_date = "date" in (rows[0].keys() if rows else [])
    print(f"[diag] matches_source.csv: {len(rows)} linhas  | coluna 'date': {'SIM' if has_date else 'NÃO'}")
    if not rows:
        print("[diag] ERRO: CSV vazio"); raise SystemExit(2)

    # 3) TheOddsAPI: lista de esportes e keys BR
    try:
        sports = fetch_sports(active_only=False)
        br_keys = resolve_brazil_soccer_sport_keys()
        print(f"[diag] TheOddsAPI: esportes={len(sports)} | keys(BR) resolvidas={br_keys}")
    except OddsApiError as e:
        print(f"[diag] TheOddsAPI: {e}")

    # 4) API-Football: ligas e season
    for lname in ["Serie A","Serie B","Serie C","Serie D"]:
        try:
            lid = resolve_league_id("Brazil", lname)
            season = resolve_current_season(lid)
            print(f"[diag] API-Football: {lname} → league_id={lid}, season={season}")
        except ApiFootballError as e:
            print(f"[diag] API-Football {lname}: {e}")

    # 5) Fixture mapping (primeiras 6 linhas)
    from datetime import datetime
    sample = rows[:6]
    for m in sample:
        date_iso = (m.get("date") or datetime.utcnow().date().isoformat())[:10]
        ok = False
        for lname in ["Serie A","Serie B","Serie C","Serie D"]:
            try:
                lid = resolve_league_id("Brazil", lname)
                season = resolve_current_season(lid)
                fx = find_fixture_id(date_iso, m["home"], m["away"], lid, season)
                if fx:
                    print(f"[diag] {m['match_id']} {m['home']} vs {m['away']} @{date_iso} → fixture_id={fx} ({lname})")
                    ok = True; break
            except ApiFootballError:
                pass
        if not ok:
            print(f"[diag] {m['match_id']} {m['home']} vs {m['away']} @{date_iso} → NÃO MAPEADO (verifique NOME/DATA)")

    print("[diag] FIM")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
