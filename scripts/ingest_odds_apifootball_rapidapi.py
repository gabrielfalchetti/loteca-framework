#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/ingest_odds_apifootball_rapidapi.py

Coleta odds 1X2 (home/draw/away) da API-Football via RapidAPI
para todos os jogos listados em data/out/<RODADA>/matches_whitelist.csv.

Saída: <OUT_DIR>/odds_apifootball.csv com colunas:
match_id,home,away,odds_home,odds_draw,odds_away,source

Regras:
- NÃO cria dados fictícios.
- Se não achar odds para algum jogo, registra no log e retorna exit code 1.
- Robustez a rate limit (HTTP 429) e instabilidades (retries com backoff).

Dependências: requests, pandas, python-dateutil, rapidfuzz (opcional mas recomendado)
"""

from __future__ import annotations
import os
import sys
import csv
import json
import time
import math
import argparse
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd
from dateutil import tz
from datetime import datetime, timedelta

# Similaridade para casar nomes (evita falhas em acentos / abreviações)
try:
    from rapidfuzz import fuzz
    def sim(a: str, b: str) -> int:
        return fuzz.token_set_ratio(a, b)
except Exception:
    def sim(a: str, b: str) -> int:
        a = (a or "").lower().strip()
        b = (b or "").lower().strip()
        return 100 if a == b else 0


API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"
HDRS = lambda key: {"x-rapidapi-key": key, "x-rapidapi-host": API_HOST}

# --------- util ---------
def jprint(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def sleep_backoff(attempt: int):
    time.sleep(min(30, 2 + attempt * 2))


def require_env(name: str) -> str:
    val = os.getenv(name, "")
    if not val:
        print(f"::error::{name} não definido no ambiente", file=sys.stderr)
        sys.exit(5)
    return val


def safe_read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def http_get(url: str, key: str, params: Dict[str, Any], timeout: int = 25, max_retry: int = 5) -> Optional[dict]:
    for a in range(max_retry):
        try:
            r = requests.get(url, headers=HDRS(key), params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 524, 520, 502, 503):
                print(f"[apifootball] HTTP {r.status_code} em {url} params={params} — retry {a+1}/{max_retry}")
                sleep_backoff(a+1)
                continue
            print(f"[apifootball] HTTP {r.status_code}: {r.text[:200]}")
            return None
        except requests.RequestException as e:
            print(f"[apifootball] EXC: {e} — retry {a+1}/{max_retry}")
            sleep_backoff(a+1)
    return None


# --------- resolução de times/fixtures ---------
def search_team_id(name: str, api_key: str, cache: Dict[str, int]) -> Optional[int]:
    key = name.lower().strip()
    if key in cache:
        return cache[key]
    js = http_get(f"{BASE_URL}/teams", api_key, {"search": name})
    best_id, best_sc = None, -1
    if js and js.get("response"):
        for it in js["response"]:
            t = it.get("team", {})
            tname = t.get("name") or ""
            tid = t.get("id")
            score = sim(name, tname)
            if score > best_sc:
                best_sc, best_id = score, tid
    if best_id:
        cache[key] = best_id
    return best_id


def next_fixture_between(home_id: int, away_name: str, api_key: str) -> Optional[int]:
    # Busca próximos jogos do mandante e filtra pelo adversário por similaridade de nome
    js = http_get(f"{BASE_URL}/fixtures", api_key, {"team": home_id, "next": 50})
    if not js or not js.get("response"):
        return None
    best_fx, best_sc = None, -1
    for fx in js["response"]:
        teams = fx.get("teams", {})
        home = teams.get("home", {}).get("name") or ""
        away = teams.get("away", {}).get("name") or ""
        score = sim(away_name, away)
        if score > best_sc:
            best_sc = score
            best_fx = fx.get("fixture", {}).get("id")
    # heurística: só aceita se similaridade razoável
    if best_sc >= 70:
        return best_fx
    return None


def odds_1x2_for_fixture(fixture_id: int, api_key: str) -> Optional[Tuple[float, float, float]]:
    js = http_get(f"{BASE_URL}/odds", api_key, {"fixture": fixture_id})
    if not js or not js.get("response"):
        return None
    # Varre bookmakers e mercados até achar 1x2
    # Estrutura esperada: response[0].bookmakers[].bets[].name == "Match Winner" ou "1x2"
    for item in js["response"]:
        books = item.get("bookmakers") or []
        for bk in books:
            bets = bk.get("bets") or []
            for bet in bets:
                name = (bet.get("name") or "").lower()
                if "match winner" in name or "1x2" in name or name == "winner":
                    values = bet.get("values") or []
                    odd_h = odd_d = odd_a = None
                    for v in values:
                        valnm = (v.get("value") or "").upper().strip()
                        odd  = v.get("odd")
                        try:
                            fodd = float(str(odd).replace(",", "."))
                        except Exception:
                            continue
                        if valnm in ("HOME", "1"):
                            odd_h = fodd
                        elif valnm in ("DRAW", "X"):
                            odd_d = fodd
                        elif valnm in ("AWAY", "2"):
                            odd_a = fodd
                    if all(x is not None for x in (odd_h, odd_d, odd_a)):
                        return (odd_h, odd_d, odd_a)
    return None


# --------- pipeline principal ---------
def run(rodada_dir: str, season: str, api_key: str, debug: bool = False) -> int:
    wl_path = os.path.join(rodada_dir, "matches_whitelist.csv")
    if not os.path.exists(wl_path):
        print(f"::error::Whitelist não encontrado: {wl_path}", file=sys.stderr)
        return 1

    wl = pd.read_csv(wl_path)
    # normaliza nomes
    for col in ("home", "away"):
        if col in wl.columns:
            wl[col] = wl[col].astype(str).str.strip()
    if not {"match_id","home","away"}.issubset(set(wl.columns)):
        print("::error::matches_whitelist.csv precisa das colunas match_id,home,away", file=sys.stderr)
        return 1

    out_csv = os.path.join(rodada_dir, "odds_apifootball.csv")
    rows: List[Dict[str, Any]] = []
    miss: List[str] = []

    # cache de team ids (disco)
    cache_file = os.path.join(rodada_dir, "apifoot_team_cache.json")
    team_cache: Dict[str, int] = {}
    if os.path.exists(cache_file):
        try:
            team_cache = json.load(open(cache_file, "r", encoding="utf-8"))
        except Exception:
            team_cache = {}

    for _, r in wl.iterrows():
        match_id = str(r["match_id"])
        home = str(r["home"])
        away = str(r["away"])

        print(f"[apifootball] match_id={match_id}  {home} x {away}")

        # 1) resolver IDs
        hid = search_team_id(home, api_key, team_cache)
        if not hid:
            print(f"[apifootball][WARN] time mandante não encontrado: {home}")
            miss.append(match_id)
            continue

        # 2) localizar fixture futuro entre home e away
        fix_id = next_fixture_between(hid, away, api_key)
        if not fix_id:
            print(f"[apifootball][WARN] fixture futuro {home} x {away} não encontrado")
            miss.append(match_id)
            continue

        # 3) coletar odds 1x2
        odds = odds_1x2_for_fixture(fix_id, api_key)
        if not odds:
            print(f"[apifootball][WARN] odds 1X2 ausentes para fixture={fix_id} ({home} x {away})")
            miss.append(match_id)
            continue

        oh, od, oa = odds
        rows.append({
            "match_id": match_id,
            "home": home,
            "away": away,
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
            "source": "apifootball"
        })

    # salva cache
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(team_cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # escreve saída SEMPRE com cabeçalho
    os.makedirs(rodada_dir, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","odds_home","odds_draw","odds_away","source"])
        wr.writeheader()
        for row in rows:
            wr.writerow(row)

    if debug:
        print(f"[apifootball][DEBUG] linhas coletadas: {len(rows)}")
        if miss:
            print(f"[apifootball][DEBUG] sem odds para: {miss}")

    # Se qualquer jogo da whitelist ficou sem odds → falha (modo obrigatório)
    if miss:
        print("::error::Alguns jogos não tiveram odds da API-Football (modo obrigatório).")
        return 1

    if len(rows) == 0:
        print("::error::Nenhuma odd coletada (arquivo salvo apenas com cabeçalho).", file=sys.stderr)
        return 1

    print(f"[apifootball] OK -> {out_csv}")
    return 0


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    p.add_argument("--season", required=False, default=os.getenv("SEASON", ""), help="Temporada (opcional)")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    api_key = require_env("X_RAPIDAPI_KEY")
    sys.exit(run(args.rodada, args.season, api_key, args.debug))