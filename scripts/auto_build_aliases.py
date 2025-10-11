#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera/atualiza data/aliases.json automaticamente a partir de NOMES REAIS retornados pelas APIs.

- Lê OUT_DIR/matches_whitelist.csv.
- Para cada time (home/away):
  * API-Football: /v3/teams?search=<nome>  -> coleta 'name' retornado.
  * TheOddsAPI: carrega eventos de vários sport keys de futebol -> coleta home_team/away_team
    quando bater (fuzzy) com o time da whitelist.
- Agrega variações como aliases. Não inventa nomes. Só registra o que veio das APIs.

Requer:
  X_RAPIDAPI_KEY (RapidAPI da API-Football)
  THEODDS_API_KEY (TheOddsAPI)
Variáveis:
  ALIASES_JSON (padrão: data/aliases.json)
  OUT_DIR (diretório da rodada com matches_whitelist.csv)
  LOOKAHEAD_DAYS, REGIONS (usadas só para TheOddsAPI)
"""

import os
import sys
import csv
import json
import time
import logging
import datetime as dt
from typing import Dict, List, Set

import requests
from unidecode import unidecode
from rapidfuzz import fuzz

logging.basicConfig(level=logging.INFO, format='[aliases-auto] %(levelname)s %(message)s')
LOG = logging.getLogger("aliases-auto")

# ==== ENV ====
XKEY = os.getenv("X_RAPIDAPI_KEY", "").strip()
THEODDS_KEY = os.getenv("THEODDS_API_KEY", "").strip()
ALIASES_PATH = os.getenv("ALIASES_JSON", "data/aliases.json")
OUT_DIR = os.getenv("OUT_DIR", "").strip()
REGIONS = os.getenv("REGIONS", "uk,eu,us,au")
LOOKAHEAD_DAYS = max(int(os.getenv("LOOKAHEAD_DAYS", "3")), 1)

if not OUT_DIR:
    LOG.error("OUT_DIR não definido.")
    sys.exit(2)

WL_PATH = os.path.join(OUT_DIR, "matches_whitelist.csv")
if not os.path.isfile(WL_PATH):
    LOG.error("Whitelist não encontrada: %s", WL_PATH)
    sys.exit(2)

# ==== CONSTS ====
APIF_HOST = "api-football-v1.p.rapidapi.com"
APIF_BASE = f"https://{APIF_HOST}/v3"
APIF_HEADERS = {"X-RapidAPI-Key": XKEY, "X-RapidAPI-Host": APIF_HOST} if XKEY else None

ODDS_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEYS = [
    "soccer_international_friendly",
    "soccer_uefa_nations_league",
    "soccer_uefa_euro_qualification",
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    "soccer_brazil_campeonato",
    "soccer_brazil_serie_b",
    "soccer"
]

UF_SUFFIXES = ["/br","/pr","/sp","/rs","/sc","/mg"]

# ==== Normalização ====
def normalize(s: str) -> str:
    s = unidecode((s or "").strip()).lower()
    s = " ".join(s.split())
    for suf in UF_SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    return s

def canon_key(s: str) -> str:
    # chave canônica no dicionário (normalizada)
    return normalize(s)

# ==== IO aliases.json ====
def load_aliases(path: str) -> Dict[str, List[str]]:
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        j = json.load(f)
    teams = j.get("teams", {})
    # normaliza todas as entradas
    out = {}
    for k, arr in teams.items():
        ck = canon_key(k)
        vals = {canon_key(v) for v in (arr or []) + [k]}
        # guarda a forma "bonita" capitalizada (best-effort) na hora de salvar
        out[ck] = sorted(vals)
    return out

def save_aliases(path: str, canon_map: Dict[str, Set[str]]):
    # reconstrói estrutura { "teams": { "<bonito>": [ "Var 1", "Var 2", ... ] }, "normalize": {...} }
    def pretty(s: str) -> str:
        # tentativa simples de capitalizar decentemente nomes curtos
        parts = s.split()
        if len(parts) <= 3:
            return " ".join(p.capitalize() for p in parts)
        return s  # evita estragar nomes longos
    teams = {}
    for ck, values in sorted(canon_map.items()):
        pkey = pretty(ck)
        teams[pkey] = sorted({pretty(v) for v in values if v})
    data = {
        "teams": teams,
        "normalize": {
            "remove_suffixes": ["/BR","/PR","/SP","/RS","/SC","/MG"],
            "ascii_fold": True,
            "squash_spaces": True,
            "lower": True
        }
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    LOG.info("aliases.json atualizado: %s (times=%d)", path, len(teams))

# ==== API helpers ====
def http_get(url: str, params=None, headers=None, ok_404=False):
    params = params or {}
    for i in range(3):
        r = requests.get(url, params=params, headers=headers, timeout=30)
        if ok_404 and r.status_code == 404:
            return None
        if r.status_code == 200:
            return r.json()
        time.sleep(0.5 + i)
    r.raise_for_status()
    return r.json()

# API-Football: coleta nomes via /teams?search=
def apif_collect_variants(name: str) -> Set[str]:
    out: Set[str] = set()
    if not APIF_HEADERS:
        return out
    q = normalize(name)
    if not q:
        return out
    js = http_get(f"{APIF_BASE}/teams", {"search": q}, headers=APIF_HEADERS)
    if not js:
        return out
    for item in js.get("response", []):
        nm = item.get("team", {}).get("name")
        if nm:
            out.add(normalize(nm))
    return out

# TheOddsAPI: lista eventos e coleta equipes que batem no fuzzy
def odds_collect_variants(target: str, events_cache: Dict[str, List[dict]]) -> Set[str]:
    out: Set[str] = set()
    trg = normalize(target)
    for sk, events in events_cache.items():
        for ev in events:
            h = normalize(ev.get("home_team", ""))
            a = normalize(ev.get("away_team", ""))
            # score simétrico: target perto de h OU a
            sc = max(fuzz.token_sort_ratio(trg, h), fuzz.token_sort_ratio(trg, a))
            if sc >= 85:
                if h: out.add(h)
                if a: out.add(a)
    return out

def fetch_odds_events() -> Dict[str, List[dict]]:
    cache: Dict[str, List[dict]] = {}
    if not THEODDS_KEY:
        return cache
    now = dt.datetime.utcnow()
    end = now + dt.timedelta(days=LOOKAHEAD_DAYS)
    commence_to = end.isoformat() + "Z"
    for sk in SPORT_KEYS:
        params = {
            "apiKey": THEODDS_KEY,
            "regions": REGIONS,
            "markets": "h2h",
            "oddsFormat": "decimal",
            "dateFormat": "iso",
            "commenceTimeTo": commence_to
        }
        url = f"{ODDS_BASE}/sports/{sk}/odds"
        try:
            js = http_get(url, params=params, ok_404=True)
            cache[sk] = js or []
            time.sleep(0.2)
        except Exception as e:
            cache[sk] = []
    return cache

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", default=OUT_DIR)
    ap.add_argument("--aliases_out", default=ALIASES_PATH)
    ap.add_argument("--debug", action="store_true", default=False)
    args = ap.parse_args()
    if args.debug:
        LOG.setLevel(logging.DEBUG)

    # lê whitelist
    with open(WL_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    teams_from_wl: List[str] = []
    for r in rows:
        teams_from_wl.extend([r["home"], r["away"]])

    # carrega aliases existentes
    existing = load_aliases(args.aliases_out)  # {canon: [canon variants]}
    canon_map: Dict[str, Set[str]] = {k: set(v) for k, v in existing.items()}

    # prepara cache de eventos do TheOddsAPI (uma ida só)
    events_cache = fetch_odds_events()

    # para cada time da whitelist, coleta variantes
    for raw_name in teams_from_wl:
        ck = canon_key(raw_name)
        if ck not in canon_map:
            canon_map[ck] = set()
        canon_map[ck].add(ck)

        # API-Football (se chave existir)
        apif_vars = apif_collect_variants(raw_name)
        # aceita apenas variantes com similaridade alta ao canônico
        for v in apif_vars:
            if fuzz.token_sort_ratio(ck, v) >= 85:
                canon_map[ck].add(v)

        # TheOddsAPI
        odds_vars = odds_collect_variants(raw_name, events_cache)
        for v in odds_vars:
            if fuzz.token_sort_ratio(ck, v) >= 85:
                canon_map[ck].add(v)

    # salva
    save_aliases(args.aliases_out, canon_map)

if __name__ == "__main__":
    main()