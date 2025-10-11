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
from unicodedata import normalize as _ucnorm

import requests
import pandas as pd

CSV_COLS = ["match_id", "home", "away", "odds_home", "odds_draw", "odds_away"]

STOPWORD_TOKENS = {
    "aa","ec","ac","sc","fc","afc","cf","ca","cd","ud",
    "sp","pr","rj","rs","mg","go","mt","ms","pa","pe","pb","rn","ce","ba","al","se","pi","ma","df","es","sc",
}

def log(level, msg):
    print(f"[theoddsapi][{level}] {msg}", flush=True)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--aliases", default="data/aliases.json")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

def _deaccent(s: str) -> str:
    return _ucnorm("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")

def norm_key(s: str) -> str:
    s = _deaccent(s).lower()
    s = s.replace("&", " e ")
    s = re.sub(r"[()/\-_.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_key_tokens(s: str) -> str:
    toks = [t for t in re.split(r"\s+", norm_key(s)) if t and t not in STOPWORD_TOKENS]
    return " ".join(toks)

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
            base = norm_key_tokens(k)
            lst = vals if isinstance(vals, list) else []
            allv = {norm_key_tokens(k)} | {norm_key_tokens(v) for v in lst}
            norm[base] = allv
        log("INFO", f"{len(norm)} aliases carregados (lenient).")
        return norm
    except Exception as e:
        log("WARN", f"Falha lendo aliases.json: {e}")
        return {}

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
        return [s["key"] for s in sports if s.get("active") and str(s.get("group","")).lower().startswith("soccer")]

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
                oh = od = oa = None
                for o in mk.get("outcomes", []):
                    nm = (o.get("name") or "")
                    price = o.get("price")
                    if not isinstance(price, (int, float)):
                        continue
                    if nm.lower() == "draw":
                        od = float(price)
                    elif norm_key(nm) == norm_key(home):
                        oh = float(price)
                    elif norm_key(nm) == norm_key(away):
                        oa = float(price)
                if oh and od and oa:
                    return oh, od, oa
    return None

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

    df_wl = pd.read_csv(wl_path)
    aliases = load_aliases_lenient(args.aliases)

    # Baixa todos os eventos de soccer e indexa por tokens
    cli = TheOdds(api_key)
    all_events = cli.fetch_all_soccer_events(regions)

    idx = {}
    for key, g in all_events:
        h_raw = g.get("home_team", "") or ""
        a_raw = g.get("away_team", "") or ""
        odds = extract_odds_from_game(g)
        if not odds:
            continue
        # índices “fortes” por tokens e também por normalização simples
        keys = set()
        keys.add(f"{norm_key_tokens(h_raw)}|{norm_key_tokens(a_raw)}")
        keys.add(f"{norm_key(h_raw)}|{norm_key(a_raw)}")
        # exemplo: inverter
        keys.add(f"{norm_key_tokens(a_raw)}|{norm_key_tokens(h_raw)}")
        keys.add(f"{norm_key(a_raw)}|{norm_key(h_raw)}")

        for k in keys:
            if k not in idx:
                idx[k] = odds

    results = []
    missing = []

    for _, r in df_wl.iterrows():
        mid = str(r["match_id"])
        home = str(r["home"])
        away = str(r["away"])

        log("INFO", f"{mid}: {home} x {away}")

        cands = []

        # chaves primárias por tokens
        cands_keys = {
            f"{norm_key_tokens(home)}|{norm_key_tokens(away)}",
            f"{norm_key(home)}|{norm_key(away)}",
        }

        # expande com aliases (se houver)
        hset = {norm_key_tokens(home), norm_key(home)}
        aset = {norm_key_tokens(away), norm_key(away)}
        if aliases:
            hset |= aliases.get(norm_key_tokens(home), set())
            aset |= aliases.get(norm_key_tokens(away), set())
        for hh in hset:
            for aa in aset:
                cands_keys.add(f"{hh}|{aa}")

        found = None
        for k in cands_keys:
            if k in idx:
                found = idx[k]
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
        log("ERROR", "Nenhuma odd válida encontrada para gerar arquivo.")
        sys.exit(5)

    if missing:
        log("ERROR", f"Alguns jogos sem odds ({len(missing)}): {missing}")
        sys.exit(5)

    log("INFO", f"Odds coletadas: {len(results)} salvas em {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())