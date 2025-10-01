#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Safe wrapper para ingestão do TheOddsAPI, com modo estrito opcional.

- Sempre escreve CSVs com cabeçalho.
- Se THEODDS_API_KEY estiver inválida ou a API falhar, continua gerando 0 linhas.
- **Se REQUIRE_ODDS=true**, sai com código != 0 quando não houver nenhuma odd coletada.
"""

from __future__ import annotations

import os
import sys
import csv
import json
import argparse
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd

from utils.oddsapi import fetch_odds_for_sport, OddsApiError

OUT_COLS = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]
UNMATCHED_COLS = ["raw_home","raw_away","reason"]

SPORT_KEYS_BR = ["soccer_brazil_campeonato", "soccer_brazil_serie_b"]

def _load_aliases(path: str) -> Dict[str, List[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return {k.lower(): [x.lower() for x in (v if isinstance(v, list) else [v])] for k, v in data.items()}
    except Exception as e:
        print(f"[theoddsapi] AVISO: falha ao ler aliases {path}: {e}")
        return {}

def _norm_team(x: str) -> str:
    if x is None:
        return ""
    return str(x).strip().lower()

def _mk_join_key(h: str, a: str) -> str:
    return f"{_norm_team(h)}__vs__{_norm_team(a)}"

def _extract_h2h_best_prices(event: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    best = {"home": None, "draw": None, "away": None}
    for bk in event.get("bookmakers") or []:
        for m in bk.get("markets") or []:
            key = (m.get("key") or "").lower()
            if key not in ("h2h", "1x2", "match_odds", "full_time_result"):
                continue
            for o in m.get("outcomes") or []:
                name = (o.get("name") or "").strip().lower()
                price = o.get("price") or o.get("decimal") or o.get("odd") or o.get("odds")
                try:
                    price = float(price)
                except Exception:
                    price = None
                if not price or price <= 1:
                    continue
                if name in ("home", "1", "h", "mandante"):
                    best["home"] = price if best["home"] is None else max(best["home"], price)
                elif name in ("draw", "x", "empate"):
                    best["draw"] = price if best["draw"] is None else max(best["draw"], price)
                elif name in ("away", "2", "a", "visitante"):
                    best["away"] = price if best["away"] is None else max(best["away"], price)
    return best["home"], best["draw"], best["away"]

def _safe_write_csv(path: str, rows: List[Dict[str, Any]], cols: List[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    strict = os.environ.get("REQUIRE_ODDS", "").strip().lower() in ("1","true","yes")

    out_dir = os.path.join("data","out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    aliases = _load_aliases(args.aliases)

    odds_rows: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []

    regions = [r.strip() for r in args.regions.split(",") if r.strip()]

    for sk in SPORT_KEYS_BR:
        try:
            events = fetch_odds_for_sport(sk, regions=regions)
        except OddsApiError as e:
            print(f"[theoddsapi] AVISO: {e}")
            continue
        except Exception as e:
            print(f"[theoddsapi] AVISO: erro inesperado em {sk}: {e}")
            continue

        for ev in events:
            home = (ev.get("home_team") or ev.get("homeTeam") or "").strip()
            away = (ev.get("away_team") or ev.get("awayTeam") or "").strip()
            if not home or not away:
                unmatched.append({"raw_home": ev.get("home_team"), "raw_away": ev.get("away_team"), "reason": "missing_teams"})
                continue

            oh, od, oa = _extract_h2h_best_prices(ev)
            if not any([oh, od, oa]):
                unmatched.append({"raw_home": home, "raw_away": away, "reason": "no_h2h"})
                continue

            def canon(name: str) -> str:
                n = _norm_team(name)
                for k, alist in aliases.items():
                    if n == k or n in alist:
                        return k
                return name

            ch = canon(home)
            ca = canon(away)

            odds_rows.append({
                "match_key": f"{_mk_join_key(ch, ca)}",
                "team_home": ch,
                "team_away": ca,
                "odds_home": oh,
                "odds_draw": od,
                "odds_away": oa,
            })

    p_odds = os.path.join(out_dir, "odds_theoddsapi.csv")
    p_unm  = os.path.join(out_dir, "unmatched_theoddsapi.csv")
    _safe_write_csv(p_odds, odds_rows, OUT_COLS)
    _safe_write_csv(p_unm, unmatched, UNMATCHED_COLS)

    print(f"9:Marcador requerido pelo workflow: \"theoddsapi-safe\"")
    print(f"[theoddsapi-safe] linhas -> {{\"odds_theoddsapi.csv\": {len(odds_rows)}, \"unmatched_theoddsapi.csv\": {len(unmatched)}}}")

    if strict and len(odds_rows) == 0:
        print("[theoddsapi-safe] ERRO: modo estrito ativo e nenhuma odd foi coletada.")
        sys.exit(20)

if __name__ == "__main__":
    main()
