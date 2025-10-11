#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestor de odds via TheOddsAPI.
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
    s = re.sub(r"\s+", " ", s)
    return s


def load_aliases_lenient(path: str) -> dict[str, set[str]]:
    if not os.path.isfile(path):
        log("INFO", f"aliases.json não encontrado em {path} — seguindo sem.")
        return {}
    try:
        txt = open(path, "r", encoding="utf-8").read()
        # remove comentários e vírgulas finais
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


def alias_variants(name: str, aliases: dict[str, set[str]]) -> list[str]:
    out = []
    base = norm_key(name)
    if base in aliases:
        out.extend(list(aliases[base]))
    else:
        for _, group in aliases.items():
            if base in group:
                out.extend(list(group))
                break
    out.append(name)
    seen, uniq = set(), []
    for v in out:
        if v not in seen:
            seen.add(v)
            uniq.append(v)
    return uniq if uniq else [name]


# ---------- API ----------
def api_get(api_key, endpoint, params=None, retries=3, delay=2):
    base = "https://api.the-odds-api.com/v4"
    url = f"{base}/{endpoint}"
    for i in range(retries):
        try:
            r = requests.get(url, params={**(params or {}), "apiKey": api_key}, timeout=20)
            if r.status_code == 429:
                time.sleep(delay * (i + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log("WARN", f"Tentativa {i+1} falhou: {e}")
            time.sleep(delay)
    return None


def extract_odds(game: dict):
    try:
        home = game.get("home_team")
        away = game.get("away_team")
        markets = game.get("bookmakers", [])
        for bk in markets:
            for mk in bk.get("markets", []):
                key = mk.get("key")
                if key == "h2h":
                    outcomes = mk.get("outcomes", [])
                    odds_map = {o["name"].lower(): o["price"] for o in outcomes if "name" in o}
                    # tenta mapear padrões
                    oh = odds_map.get(home.lower())
                    oa = odds_map.get(away.lower())
                    od = odds_map.get("draw")
                    if all([oh, od, oa]):
                        return oh, od, oa
        return None
    except Exception:
        return None


# ---------- Main ----------
def main():
    args = parse_args()
    rodada = args.rodada
    regions = args.regions
    aliases = load_aliases_lenient(args.aliases)

    api_key = (os.getenv("THEODDS_API_KEY") or "").strip()
    if not api_key:
        log("ERROR", "THEODDS_API_KEY ausente nos secrets.")
        sys.exit(5)

    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        log("ERROR", f"Whitelist não encontrada: {wl_path}")
        pd.DataFrame(columns=CSV_COLS).to_csv(os.path.join(rodada, "odds_theoddsapi.csv"), index=False)
        sys.exit(5)

    df = pd.read_csv(wl_path)
    results, missing = [], []

    for _, r in df.iterrows():
        mid = str(r["match_id"])
        home = str(r["home"])
        away = str(r["away"])
        log("INFO", f"{mid}: {home} x {away}")

        # TheOdds API: não tem search por nome, usa /sports/<sport>/odds
        # Vamos tentar buscar no endpoint global soccer
        data = api_get(api_key, "sports/soccer/odds", {"regions": regions, "markets": "h2h"})
        if not data:
            missing.append(mid)
            continue

        # procura jogo que bata com os nomes (normalizados)
        found = None
        hkey, akey = norm_key(home), norm_key(away)
        for g in data:
            ghome, gaway = norm_key(g.get("home_team", "")), norm_key(g.get("away_team", ""))
            if (hkey in ghome or ghome in hkey) and (akey in gaway or gaway in akey):
                found = g
                break
            # tenta invertido
            if (hkey in gaway or gaway in hkey) and (akey in ghome or ghome in akey):
                found = g
                break

        if not found:
            log("WARN", f"Nenhum evento mapeado para {home} x {away}")
            missing.append(mid)
            continue

        odds = extract_odds(found)
        if not odds:
            log("WARN", f"Odds não encontradas em TheOddsAPI para {home} x {away}")
            missing.append(mid)
            continue

        oh, od, oa = odds
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