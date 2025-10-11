#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coleta odds via TheOddsAPI.
Saída: <rodada>/odds_theoddsapi.csv
Colunas: match_id,home,away,odds_home,odds_draw,odds_away
"""

import os
import re
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from unicodedata import normalize as _ucnorm

import requests
import pandas as pd

CSV_COLS = ["match_id", "home", "away", "odds_home", "odds_draw", "odds_away"]

def log(level, msg):
    print(f"[theoddsapi][{level}] {msg}", flush=True)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--aliases", default="data/aliases.json")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

# ---------- Normalização / Aliases ----------
def _deaccent(s: str) -> str:
    return _ucnorm("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")

def norm_key(s: str) -> str:
    s = _deaccent(s).lower().strip()
    s = re.sub(r"/[a-z]{2}($|[^a-z])", " ", s)   # remove "/SP" etc
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def load_aliases_lenient(path: str) -> dict[str, set[str]]:
    if not os.path.isfile(path):
        return {}
    try:
        txt = open(path, "r", encoding="utf-8").read()
        txt = re.sub(r"//.*", "", txt)
        txt = re.sub(r"/\*.*?\*/", "", txt, flags=re.S)
        txt = re.sub(r",\s*([\]}])", r"\1", txt)
        data = json.loads(txt)
        norm = {}
        for k, vals in (data or {}).items():
            base = norm_key(k)
            lst = vals if isinstance(vals, list) else []
            allv = {norm_key(k)} | {norm_key(v) for v in lst}
            norm[base] = allv
        log("INFO", f"{len(norm)} aliases carregados (lenient).")
        return norm
    except Exception as e:
        log("WARN", f"Falha lendo aliases.json: {e}")
        return {}

# ---------- TheOddsAPI ----------
class TheOdds:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base = "https://api.the-odds-api.com/v4"

    def get(self, path, params=None, retries=3, delay=1.5):
        url = f"{self.base}/{path.lstrip('/')}"
        for i in range(retries):
            try:
                r = requests.get(url, params={**(params or {}), "apiKey": self.api_key}, timeout=25)
                if r.status_code == 429:
                    time.sleep(delay*(i+1))
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if i == retries-1:
                    log("WARN", f"GET {path} falhou: {e}")
                time.sleep(delay)
        return None

    def list_active_soccer_sports(self):
        sports = self.get("sports", {"all": "true"}) or []
        keys = [s["key"] for s in sports if s.get("active") and str(s.get("group","")).lower().startswith("soccer")]
        return keys

    def fetch_all_soccer_events(self, regions: str):
        events = []
        for key in self.list_active_soccer_sports():
            data = self.get(f"sports/{key}/odds", {
                "regions": regions,
                "markets": "h2h",
                "oddsFormat": "decimal",
                "dateFormat": "iso"
            }) or []
            for g in data:
                events.append((key, g))
        return events

def extract_odds_from_game(game: dict):
    home = game.get("home_team")
    away = game.get("away_team")
    for bk in game.get("bookmakers", []):
        for mk in bk.get("markets", []):
            if mk.get("key") == "h2h":
                # outcomes: name == home_team | away_team | Draw
                oh = od = oa = None
                for o in mk.get("outcomes", []):
                    nm = (o.get("name") or "").lower()
                    price = o.get("price")
                    if nm == "draw":
                        od = price
                    elif nm == (home or "").lower():
                        oh = price
                    elif nm == (away or "").lower():
                        oa = price
                if oh and od and oa:
                    return oh, od, oa
    return None

# ---------- Main ----------
def main():
    args = parse_args()
    rodada = args.rodada
    regions = args.regions

    api_key = (os.getenv("THEODDS_API_KEY") or "").strip()
    if not api_key:
        log("ERROR", "THEODDS_API_KEY ausente nos secrets.")
        sys.exit(5)

    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        log("ERROR", f"Whitelist não encontrada: {wl_path}")
        pd.DataFrame(columns=CSV_COLS).to_csv(os.path.join(rodada, "odds_theoddsapi.csv"), index=False)
        sys.exit(5)

    # carregar whitelist
    df = pd.read_csv(wl_path)

    # carregar aliases (tolerante)
    aliases = load_aliases_lenient(args.aliases)

    # baixa todos os eventos de SOCCER (uma vez por run) e indexa
    cli = TheOdds(api_key)
    all_events = cli.fetch_all_soccer_events(regions)
    index = {}
    for key, g in all_events:
        h = norm_key(g.get("home_team", ""))
        a = norm_key(g.get("away_team", ""))
        ok = extract_odds_from_game(g)
        if not ok:
            continue
        idx1 = f"{h}|{a}"
        idx2 = f"{a}|{h}"
        index[idx1] = ok
        index[idx2] = (ok[2], ok[1], ok[0])  # invertido

    results, missing = [], []

    for _, r in df.iterrows():
        mid = str(r["match_id"])
        home = str(r["home"])
        away = str(r["away"])
        log("INFO", f"{mid}: {home} x {away}")

        # tentar com aliases
        homes = {norm_key(home)}
        aways = {norm_key(away)}
        if aliases:
            homes |= (aliases.get(norm_key(home), set()))
            aways |= (aliases.get(norm_key(away), set()))

        found = None
        for h in homes:
            for a in aways:
                key = f"{h}|{a}"
                if key in index:
                    found = index[key]
                    break
            if found:
                break

        if not found:
            log("WARN", f"Nenhum evento mapeado para {home} x {away}")
            missing.append(mid)
            continue

        oh, od, oa = found
        results.append(dict(match_id=mid, home=home, away=away,
                            odds_home=oh, odds_draw=od, odds_away=oa))

    out_path = os.path.join(rodada, "odds_theoddsapi.csv")
    pd.DataFrame(results, columns=CSV_COLS).to_csv(out_path, index=False)

    if not results:
        log("ERROR", "Nenhuma odd válida encontrada para gerar consenso.")
        sys.exit(5)

    if missing:
        log("ERROR", f"Alguns jogos sem odds ({len(missing)}): {missing}")
        sys.exit(5)

    log("INFO", f"Odds coletadas: {len(results)} salvas em {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())