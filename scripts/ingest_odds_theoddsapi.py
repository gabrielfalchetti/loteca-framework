#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json
import time
import datetime as dt
import logging
from typing import Dict, List, Optional, Tuple

import requests
from rapidfuzz import fuzz, process
from unidecode import unidecode

logging.basicConfig(level=logging.INFO, format='[theoddsapi] %(message)s')
LOG = logging.getLogger("theoddsapi")

API_KEY = os.getenv("THEODDS_API_KEY", "").strip()
if not API_KEY:
    LOG.error("THEODDS_API_KEY ausente nos secrets.")
    sys.exit(5)

REGIONS = os.getenv("REGIONS", "uk,eu,us,au")
ALIASES_PATH = os.getenv("ALIASES_JSON", "data/aliases.json")
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "3"))
if LOOKAHEAD_DAYS <= 0:
    LOOKAHEAD_DAYS = 3

BASE = "https://api.the-odds-api.com/v4"

SPORT_KEYS = [
    # internacionais e genéricos
    "soccer_international_friendly",
    "soccer_uefa_nations_league",
    "soccer_uefa_euro_qualification",
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    # Brasil
    "soccer_brazil_campeonato",
    "soccer_brazil_serie_b",
    # fallback genérico
    "soccer"
]

def normalize_name(s: str) -> str:
    s = s or ""
    s = unidecode(s).strip().lower()
    s = " ".join(s.split())
    # remove /UF
    for suf in ["/br","/pr","/sp","/rs","/sc","/mg"]:
        if s.endswith(suf):
            s = s[: -len(suf)]
            s = s.strip()
    return s

def load_aliases(path: str) -> Dict[str, List[str]]:
    if not os.path.isfile(path):
        return {}
    j = json.load(open(path, "r", encoding="utf-8"))
    teams = j.get("teams", {})
    return {normalize_name(k): list(set([normalize_name(x) for x in v+[k]])) for k, v in teams.items()}

def alt_names(name: str, aliases: Dict[str, List[str]]) -> List[str]:
    n = normalize_name(name)
    out = [n]
    for canon, arr in aliases.items():
        if n == canon or n in arr:
            out.extend(arr)
            out.append(canon)
    hard = {
        "gremio novorizontino": ["novorizontino"],
        "america mineiro": ["america mineiro", "america mg", "america-mg"],
        "operario pr": ["operario ferroviario", "operario"],
        "republic of ireland": ["ireland"]
    }
    if n in hard:
        out.extend(hard[n])
    return list(dict.fromkeys(out))

def fetch_events(sport_key: str, date_to_iso: str) -> List[dict]:
    # markets= h2h (equivale ao 1X2 sem o X onde não há empate; no futebol há empate, então vem 3 outcomes)
    params = {
        "apiKey": API_KEY,
        "regions": REGIONS,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
        "commenceTimeTo": date_to_iso
    }
    url = f"{BASE}/sports/{sport_key}/odds"
    r = requests.get(url, params=params, timeout=30)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json()

def outcomes_to_1x2(teams: List[str], outcomes: List[dict]) -> Optional[Tuple[float,float,float]]:
    # outcomes: [{"name":"Home Team", "price":1.8}, {"name":"Draw","price":3.2}, {"name":"Away Team","price":4.1}]
    # Alguns books usam nomes exatos dos times
    prices = {"home": None, "draw": None, "away": None}
    # tenta mapear por label comum
    for o in outcomes:
        nm = o.get("name","").lower()
        pr = o.get("price")
        if pr is None: 
            continue
        if "draw" in nm or nm == "x":
            prices["draw"] = float(pr)
        elif "home" in nm or (teams and nm == teams[0].lower()):
            prices["home"] = float(pr)
        elif "away" in nm or (len(teams)>1 and nm == teams[1].lower()):
            prices["away"] = float(pr)
    if all(prices[k] is not None for k in ["home","draw","away"]):
        return prices["home"], prices["draw"], prices["away"]
    return None

def match_event(home: str, away: str, events: List[dict]) -> Optional[dict]:
    home_l = normalize_name(home)
    away_l = normalize_name(away)
    best = None
    best_score = -1
    for ev in events:
        try:
            t1 = normalize_name(ev["home_team"])
            t2 = normalize_name(ev["away_team"])
        except Exception:
            continue
        s1 = max(fuzz.token_sort_ratio(home_l, t1), fuzz.token_sort_ratio(home_l, t2))
        s2 = max(fuzz.token_sort_ratio(away_l, t1), fuzz.token_sort_ratio(away_l, t2))
        score = (s1 + s2)/2.0
        if score > best_score:
            best_score = score
            best = ev
    if best and best_score >= 80:
        return best
    return None

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", required=False, default=REGIONS)
    ap.add_argument("--aliases", required=False, default=ALIASES_PATH)
    ap.add_argument("--debug", action="store_true", default=(os.getenv("DEBUG", "false").lower() in ["1","true","yes"]))
    args = ap.parse_args()
    if args.debug:
        LOG.setLevel(logging.DEBUG)

    rodada_dir = args.rodada
    wl = os.path.join(rodada_dir, "matches_whitelist.csv")
    if not os.path.isfile(wl):
        LOG.error("Whitelist %s não encontrada", wl)
        sys.exit(5)

    with open(wl, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    aliases = load_aliases(args.aliases)

    # janela até (início agora, limite no futuro)
    now = dt.datetime.utcnow()
    end = now + dt.timedelta(days=LOOKAHEAD_DAYS)
    commence_to = end.isoformat() + "Z"

    # carrega eventos de todos os sport keys (cache em memória)
    all_events: Dict[str, List[dict]] = {}
    for sk in SPORT_KEYS:
        try:
            evs = fetch_events(sk, commence_to)
            all_events[sk] = evs or []
            LOG.info("sport=%s  eventos=%d", sk, len(all_events[sk]))
            time.sleep(0.2)
        except requests.HTTPError as e:
            LOG.info("sport=%s  HTTP %s (ignorado se 404)", sk, getattr(e.response, "status_code", "?"))
            continue
        except Exception:
            continue

    out_rows = []
    missing = []

    for r in rows:
        mid = str(r["match_id"]).strip()
        home = r["home"].strip()
        away = r["away"].strip()

        # gera variantes (aliases)
        home_alts = alt_names(home, aliases)[:3]
        away_alts = alt_names(away, aliases)[:3]

        found = None
        for sk, evs in all_events.items():
            if not evs:
                continue
            # tenta direto
            ev = match_event(home, away, evs)
            if not ev:
                # tenta com aliases combinados
                for h in home_alts:
                    for a in away_alts:
                        ev = match_event(h, a, evs)
                        if ev:
                            break
                    if ev:
                        break
            if ev:
                found = (sk, ev)
                break

        if not found:
            LOG.warning("Evento não encontrado no TheOddsAPI: %s x %s", home, away)
            missing.append(mid)
            continue

        sk, ev = found
        # extrai odds (h2h)
        odds_home = odds_draw = odds_away = None
        for bk in ev.get("bookmakers", []):
            for mk in bk.get("markets", []):
                if mk.get("key") == "h2h":
                    conv = outcomes_to_1x2([ev.get("home_team",""), ev.get("away_team","")], mk.get("outcomes", []))
                    if conv:
                        odds_home, odds_draw, odds_away = conv
                        break
            if odds_home:
                break

        if not all([odds_home, odds_draw, odds_away]):
            LOG.warning("Odds não disponíveis (h2h) para %s x %s (sport=%s)", home, away, sk)
            missing.append(mid)
            continue

        out_rows.append({
            "match_id": mid,
            "home": home,
            "away": away,
            "odds_home": odds_home,
            "odds_draw": odds_draw,
            "odds_away": odds_away
        })

    out_csv = os.path.join(rodada_dir, "odds_theoddsapi.csv")
    if not out_rows:
        LOG.error("ERRO Nenhum evento mapeado à whitelist (odds vazias).")
        sys.exit(5)

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","odds_home","odds_draw","odds_away"])
        wr.writeheader()
        for row in out_rows:
            wr.writerow(row)

    if missing:
        LOG.warning("Alguns jogos ficaram sem odds TheOddsAPI: %s", missing)
        # não damos exit aqui; deixamos o consenso decidir (STRICT exige cobertura)
    LOG.info("Gerado %s com %d linhas", out_csv, len(out_rows))

if __name__ == "__main__":
    main()