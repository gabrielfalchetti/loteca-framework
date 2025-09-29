#!/usr/bin/env python
from __future__ import annotations

# HOTFIX de import p/ end2end (executando a partir de scripts/)
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse, csv
from typing import Dict, Any, List, Tuple
from utils.oddsapi import resolve_brazil_soccer_sport_keys, fetch_odds_for_sport, OddsApiError
from utils.match_normalize import canonical, fuzzy_match

def read_matches(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print(f"[ERRO] Arquivo não encontrado: {path}")
        raise SystemExit(2)
    rows = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        needed = {"match_id","home","away"}
        if not needed.issubset(reader.fieldnames or []):
            print("Error: matches_source.csv precisa de colunas: match_id,home,away[,date].")
            raise SystemExit(2)
        rows.extend(reader)
    return rows

def flatten_odds(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat = []
    for ev in events:
        home = ev.get("home_team") or ev.get("teams", {}).get("home")
        away = ev.get("away_team") or ev.get("teams", {}).get("away")
        if not home or not away:
            continue
        for bm in ev.get("bookmakers", []):
            bname = bm.get("title")
            for m in bm.get("markets", []):
                mkey = m.get("key")
                for o in m.get("outcomes", []):
                    flat.append({
                        "prov_home": home, "prov_away": away,
                        "bookmaker": bname, "market": mkey,
                        "selection": o.get("name"), "price": o.get("price")
                    })
    return flat

def match_provider_events(matches: List[Dict[str,str]], prov_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    out, unmatched = [], []
    prov_index: Dict[Tuple[str,str], List[Dict[str, Any]]] = {}
    for r in prov_rows:
        h = canonical(r["prov_home"]); a = canonical(r["prov_away"])
        prov_index.setdefault((h,a), []).append(r)

    for m in matches:
        mh, ma = canonical(m["home"]), canonical(m["away"])
        rows = prov_index.get((mh,ma))
        if not rows:
            cand_keys = list(prov_index.keys())
            cand_home = [k[0] for k in cand_keys]; cand_away = [k[1] for k in cand_keys]
            h_best = fuzzy_match(m["home"], cand_home); a_best = fuzzy_match(m["away"], cand_away)
            if h_best and a_best and (h_best, a_best) in prov_index:
                rows = prov_index[(h_best, a_best)]
        if rows:
            for r in rows:
                out.append({
                    "match_id": m["match_id"], "home": m["home"], "away": m["away"],
                    "bookmaker": r["bookmaker"], "market": r["market"],
                    "selection": r["selection"], "price": r["price"]
                })
        else:
            unmatched.append({"match_id": m["match_id"], "home": m["home"], "away": m["away"], "motivo": "no_match_theoddsapi"})
    return out, unmatched

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    base_in = Path("data/in") / args.rodada
    base_out = Path("data/out") / args.rodada
    base_out.mkdir(parents=True, exist_ok=True)

    rows = read_matches(base_in / "matches_source.csv")

    print("[theoddsapi] Resolvendo sport_keys BR…")
    try:
        keys = resolve_brazil_soccer_sport_keys()
    except OddsApiError as e:
        print(f"[theoddsapi] ERRO: {e}")
        keys = []
    if args.debug:
        print(f"[theoddsapi] candidatos: {keys}")

    all_events = []
    for k in keys:
        ev = fetch_odds_for_sport(k, regions=args.regions.split(","))
        if not ev:
            print(f"[theoddsapi] AVISO {k}: vazio/indisponível")
        else:
            print(f"[theoddsapi] {k}: {len(ev)} eventos")
            all_events.extend(ev)

    flat = flatten_odds(all_events)
    matched, unmatched = match_provider_events(rows, flat)

    out_csv = base_out / "odds_theoddsapi.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","bookmaker","market","selection","price"])
        wr.writeheader(); wr.writerows(matched)

    if unmatched:
        um_csv = base_out / "unmatched_theoddsapi.csv"
        with um_csv.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=["match_id","home","away","motivo"])
            wr.writeheader(); wr.writerows(unmatched)
        print(f"[theoddsapi] AVISO: {len(unmatched)} sem casamento → {um_csv}")

    print(f"[theoddsapi] OK -> {out_csv} ({len(matched)} linhas)")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
