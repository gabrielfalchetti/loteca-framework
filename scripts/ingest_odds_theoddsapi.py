#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor REAL — TheOddsAPI -> competições nacionais do Brasil
Saída: data/out/<RODADA>/odds_theoddsapi.csv

Melhorias:
- regions padrão = "uk,eu,us" (não existe "br").
- Detecta chaves BR via /sports (title ou key contendo "brazil").
- Fallback explícito para: serie_a, serie_b, serie_c, serie_d, cup.
- Consulta /events antes de /odds (para saber se há jogos listados).
- Logs detalhados (--debug).
- Sempre grava CSV com colunas fixas.
"""

from __future__ import annotations
import argparse, os, sys, time, unicodedata, math, difflib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any
import requests
import numpy as np
import pandas as pd

API = "https://api.the-odds-api.com/v4"
TIMEOUT = 25
RETRY = 3
SLEEP = 0.6
BR_TZ = timezone(timedelta(hours=-3))

OUT_COLS = ["home","away","book","k1","kx","k2","total_line","over","under","ts"]

FALLBACK_BR_SPORT_KEYS = [
    "soccer_brazil_serie_a",
    "soccer_brazil_serie_b",
    "soccer_brazil_serie_c",
    "soccer_brazil_serie_d",
    "soccer_brazil_cup",
    "soccer_brazil_campeonato",
]

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip().replace(".", " ")
    for suf in [" fc"," afc"," ac"," sc","-sp","-rj"]:
        s = s.replace(suf, "")
    return " ".join(s.split())

def _get(url: str, params: Dict[str, Any], debug=False) -> Any:
    last = None
    for i in range(RETRY):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(1.0 + i)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.8 + 0.4*i)
    if debug:
        print(f"[theoddsapi] ERRO {url} -> {last}", file=sys.stderr)
    return []

def _list_brazil_sports(key: str, debug=False) -> List[str]:
    js = _get(f"{API}/sports", {"apiKey": key}, debug=debug)
    sports_keys: List[str] = []
    for it in js or []:
        title = (it.get("title") or "").lower()
        skey  = it.get("key") or ""
        if ("brazil" in title) or ("copa do brasil" in title) or ("brazil" in skey.lower()):
            if skey:
                sports_keys.append(skey)
    sports_keys = list(dict.fromkeys(sports_keys))
    if debug:
        print(f"[theoddsapi] sports detectados (BR): {sports_keys}")
    if not sports_keys:
        sports_keys = FALLBACK_BR_SPORT_KEYS.copy()
        if debug:
            print(f"[theoddsapi] fallback aplicado: {sports_keys}")
    return sports_keys

def _pull_odds_for_sport(key: str, sport_key: str, regions: str, debug=False) -> List[Dict[str, Any]]:
    # 1) Verifica se há eventos listados
    evs = _get(f"{API}/sports/{sport_key}/events", {"apiKey": key}, debug=debug)
    if debug:
        print(f"[theoddsapi] {sport_key}: eventos listados={len(evs) if evs else 0}")
    if not evs:
        return []

    # 2) Puxa odds
    params = {"apiKey": key, "regions": regions, "markets": "h2h", "oddsFormat": "decimal"}
    js = _get(f"{API}/sports/{sport_key}/odds", params, debug=debug)
    out = []
    total_ev = 0
    with_odds = 0
    for ev in js or []:
        total_ev += 1
        home = (ev.get("home_team") or "").strip()
        away = (ev.get("away_team") or "").strip()
        h_list, d_list, a_list = [], [], []
        for bk in ev.get("bookmakers", []) or []:
            for mkt in bk.get("markets", []) or []:
                if (mkt.get("key") or "").lower() == "h2h":
                    for outc in mkt.get("outcomes", []) or []:
