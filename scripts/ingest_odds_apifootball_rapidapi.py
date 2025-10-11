#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json
import time
import math
import logging
import datetime as dt
from typing import Dict, List, Tuple, Optional

import requests
from rapidfuzz import fuzz, process
from unidecode import unidecode

logging.basicConfig(level=logging.INFO, format='[apifootball][%(levelname)s] %(message)s')
LOG = logging.getLogger("apifootball")

API_HOST = "api-football-v1.p.rapidapi.com"
BASE = f"https://{API_HOST}/v3"

XKEY = os.getenv("X_RAPIDAPI_KEY", "").strip()
if not XKEY:
    LOG.error("X_RAPIDAPI_KEY ausente nos secrets.")
    sys.exit(5)

ALIASES_PATH = os.getenv("ALIASES_JSON", "data/aliases.json")
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "3"))
if LOOKAHEAD_DAYS <= 0:
    LOOKAHEAD_DAYS = 3

HEADERS = {"X-RapidAPI-Key": XKEY, "X-RapidAPI-Host": API_HOST}

def load_aliases(path: str) -> Dict[str, List[str]]:
    if not os.path.isfile(path):
        LOG.warning("aliases.json não encontrado em %s — prosseguindo sem aliases", path)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        j = json.load(f)
    teams = j.get("teams", {})
    return {normalize_name(k): list(set([normalize_name(x) for x in v+[k]])) for k, v in teams.items()}

def normalize_name(s: str) -> str:
    s = s or ""
    s = s.strip()
    # remove /UF no final
    s = unidecode(s)
    s = s.replace("\u00a0", " ")
    s = " ".join(s.split())
    # tira sufixos de UF comuns
    for suf in ["/BR","/PR","/SP","/RS","/SC","/MG"]:
        if s.lower().endswith(suf.lower()):
            s = s[: -len(suf)]
            s = s.strip()
    return s.lower()

def alt_names(name: str, aliases: Dict[str, List[str]]) -> List[str]:
    n = normalize_name(name)
    out = [n]
    # inclui variantes do dicionário
    for canon, arr in aliases.items():
        if n == canon or n in arr:
            out.extend(arr)
            out.append(canon)
    # alguns hard-fixes
    hard = {
        "gremio novorizontino": ["novorizontino"],
        "america mineiro": ["america mineiro", "america mg", "america-mg"],
        "operario pr": ["operario ferroviario", "operario"],
        "republic of ireland": ["ireland"]
    }
    if n in hard:
        out.extend(hard[n])
    return list(dict.fromkeys(out))  # unique keep order

def http_get(url: str, params: Dict) -> dict:
    for i in range(3):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        time.sleep(1 + i)
    r.raise_for_status()
    return {}

def pick_fixture_by_names(candidates: List[dict], home: str, away: str) -> Optional[dict]:
    # usa fuzzy tanto no time home quanto away
    home_l = normalize_name(home)
    away_l = normalize_name(away)
    best = None
    best_score = -1.0
    for fx in candidates:
        try:
            t1 = normalize_name(fx["teams"]["home"]["name"])
            t2 = normalize_name(fx["teams"]["away"]["name"])
        except Exception:
            continue
        s1 = max(fuzz.token_sort_ratio(home_l, t1), fuzz.token_sort_ratio(home_l, t2))
        s2 = max(fuzz.token_sort_ratio(away_l, t1), fuzz.token_sort_ratio(away_l, t2))
        # exige que sejam times distintos; pontuação média
        if t1 == t2:
            continue
        score = (s1 + s2) / 2.0
        if score > best_score:
            best_score = score
            best = fx
    if best and best_score >= 80:
        return best
    return None

def search_fixtures(home: str, away: str, date_from: str, date_to: str) -> List[dict]:
    # Estratégia 1: search={home}
    out = []
    for q in [home, away, f"{home} {away}"]:
        js = http_get(f"{BASE}/fixtures", {"search": q, "from": date_from, "to": date_to})
        out.extend(js.get("response", []))
    # remove duplicados
    seen = set()
    uniq = []
    for fx in out:
        fid = fx.get("fixture", {}).get("id")
        if fid and fid not in seen:
            seen.add(fid)
            uniq.append(fx)
    return uniq

def get_odds_for_fixture(fixture_id: int) -> Optional[Tuple[float, float, float]]:
    js = http_get(f"{BASE}/odds", {"fixture": fixture_id})
    res = js.get("response", [])
    # retorna primeira casa de apostas com 1X2
    for item in res:
        for bk in item.get("bookmakers", []):
            for mk in bk.get("bets", []):
                if mk.get("name", "").lower() in ["match winner", "1x2", "winner"]:
                    vals = {o.get("value", "").upper(): float(o.get("odd", "nan")) for o in mk.get("values", [])}
                    # algumas APIs usam "Home/Draw/Away" ou "1/X/2"
                    h = vals.get("HOME") or vals.get("1")
                    d = vals.get("DRAW") or vals.get("X")
                    a = vals.get("AWAY") or vals.get("2")
                    if all(x is not None for x in [h, d, a]):
                        return float(h), float(d), float(a)
    return None

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", required=False, default=os.getenv("SEASON", "2025"))
    ap.add_argument("--aliases", required=False, default=ALIASES_PATH)
    ap.add_argument("--debug", action="store_true", default=(os.getenv("DEBUG", "false").lower() in ["1","true","yes"]))
    args = ap.parse_args()
    if args.debug:
        LOG.setLevel(logging.DEBUG)

    rodada_dir = args.rodada
    wl_path = os.path.join(rodada_dir, "matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        LOG.error("Whitelist não encontrada em %s", wl_path)
        sys.exit(5)

    aliases = load_aliases(args.aliases)

    # janela de datas (hoje .. hoje + LOOKAHEAD  |  fallback +7)
    today = dt.date.today()
    date_to = today + dt.timedelta(days=LOOKAHEAD_DAYS)
    date_from = today - dt.timedelta(days=1)  # permite jogos “hoje” que já tenham fixture criado
    f_date_from = date_from.isoformat()
    f_date_to = date_to.isoformat()

    out_rows = []
    missing = []

    with open(wl_path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        rows = list(rd)

    LOG.info("whitelist: %s  linhas=%d  mapeamento=%s", wl_path, len(rows), rd.fieldnames)
    idx = 0
    for r in rows:
        idx += 1
        mid = str(r["match_id"]).strip()
        home = r["home"].strip()
        away = r["away"].strip()

        LOG.info("%d: %s x %s", idx, home, away)

        # nomes alternativos
        home_alts = alt_names(home, aliases)
        away_alts = alt_names(away, aliases)

        # busca fixtures
        candidates = []
        for h in home_alts[:3]:  # limita para não explodir chamadas
            for a in away_alts[:3]:
                cand = search_fixtures(h, a, f_date_from, f_date_to)
                candidates.extend(cand)
                time.sleep(0.2)

        if not candidates:
            LOG.warning("Fixture não localizado para %s x %s", home, away)
            missing.append(mid)
            continue

        chosen = pick_fixture_by_names(candidates, home, away)
        if not chosen:
            LOG.warning("Fixture não casado por fuzzy: %s x %s", home, away)
            missing.append(mid)
            continue

        fixture_id = chosen["fixture"]["id"]
        odds = get_odds_for_fixture(fixture_id)
        if not odds:
            LOG.warning("Odds não encontradas para fixture %s (%s x %s)", fixture_id, home, away)
            missing.append(mid)
            continue

        oH, oD, oA = odds
        out_rows.append({
            "match_id": mid,
            "home": home,
            "away": away,
            "odds_home": oH,
            "odds_draw": oD,
            "odds_away": oA
        })

    # saída
    out_csv = os.path.join(rodada_dir, "odds_apifootball.csv")
    if not out_rows:
        LOG.error("Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).")
        for r in rows:
            LOG.info("%s: %s x %s", r["match_id"], r["home"], r["away"])
        LOG.debug("[DEBUG] coletadas: 0  faltantes: %d -> %s", len(rows), [r["match_id"] for r in rows])
        sys.exit(5)

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","odds_home","odds_draw","odds_away"])
        wr.writeheader()
        for row in out_rows:
            wr.writerow(row)

    if missing:
        LOG.error("Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).")
        for m in missing:
            rr = next(x for x in rows if x["match_id"] == m)
            LOG.info("%s: %s x %s", rr["match_id"], rr["home"], rr["away"])
        LOG.debug("[DEBUG] coletadas: %d  faltantes: %d -> %s", len(out_rows), len(missing), missing)
        sys.exit(5)

    LOG.info("Coletadas %d linhas em %s", len(out_rows), out_csv)

if __name__ == "__main__":
    main()