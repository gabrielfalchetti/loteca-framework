#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/ingest_odds_apifootball_rapidapi.py

Coleta odds 1X2 (home/draw/away) da API-Football via RapidAPI para
os jogos da rodada informados nos arquivos de input.

Saída: <OUT_DIR>/odds_apifootball.csv com colunas:
match_id,home,away,odds_home,odds_draw,odds_away,source

Política:
- Sem dados fictícios.
- APIs são obrigatórias: se algum jogo da whitelist ficar sem odds → falha.
"""

from __future__ import annotations
import os
import sys
import csv
import json
import time
import argparse
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd

# similaridade robusta (opcional)
try:
    from rapidfuzz import fuzz
    def _sim(a: str, b: str) -> int:
        return fuzz.token_set_ratio(a, b)
except Exception:
    def _sim(a: str, b: str) -> int:
        a = (a or "").lower().strip()
        b = (b or "").lower().strip()
        return 100 if a == b else 0

API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"

def _hdrs(key: str) -> Dict[str, str]:
    return {"x-rapidapi-key": key, "x-rapidapi-host": API_HOST}

# ----------------- utils HTTP -----------------
def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(30, 2 + attempt * 2))

def _http_get(url: str, key: str, params: Dict[str, Any], timeout: int = 25, max_retry: int = 5) -> Optional[dict]:
    for a in range(max_retry):
        try:
            r = requests.get(url, headers=_hdrs(key), params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 524, 520, 502, 503):
                print(f"[apifootball] HTTP {r.status_code} {url} params={params} — retry {a+1}/{max_retry}")
                _sleep_backoff(a+1)
                continue
            print(f"[apifootball] HTTP {r.status_code}: {r.text[:240]}")
            return None
        except requests.RequestException as e:
            print(f"[apifootball] EXC: {e} — retry {a+1}/{max_retry}")
            _sleep_backoff(a+1)
    return None

# ----------------- leitura da whitelist -----------------
WL_CANDIDATES = ("matches_whitelist.csv", "matches_source.csv", "matches.csv")

CANON = {"match_id": {"match_id","id","game_id","jogo_id"},
         "home": {"home","mandante","home_team","team_home","casa"},
         "away": {"away","visitante","away_team","team_away","fora"}}

def _load_first_existing_csv(rodada_dir: str) -> Tuple[pd.DataFrame, str]:
    for fname in WL_CANDIDATES:
        path = os.path.join(rodada_dir, fname)
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                # ignora arquivos vazios (só cabeçalho)
                if df.shape[0] > 0:
                    return df, path
            except Exception:
                pass
    return pd.DataFrame(), ""

def _normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str,str]]:
    # mapeia colunas variáveis → canônicas
    lower_map = {c: c.lower().strip() for c in df.columns}
    inv = {}
    for canon, variants in CANON.items():
        for c, lc in lower_map.items():
            if lc in variants:
                inv[canon] = c
                break
    # se não achou alguma, tenta por similaridade
    for canon in ("home","away"):
        if canon not in inv:
            # escolha a coluna com maior similaridade a 'home'/'away'
            best_c, best_s = None, -1
            for c in df.columns:
                s = _sim(c, canon)
                if s > best_s:
                    best_c, best_s = c, s
            if best_s >= 75:
                inv[canon] = best_c
    return df.rename(columns={inv.get("match_id","match_id"): "match_id",
                              inv.get("home","home"): "home",
                              inv.get("away","away"): "away"}), inv

def _load_whitelist(rodada_dir: str) -> pd.DataFrame:
    df, used_path = _load_first_existing_csv(rodada_dir)
    if df.empty:
        print(f"::error::Nenhum arquivo de partidas encontrado em {rodada_dir} "
              f"(procurado: {', '.join(WL_CANDIDATES)})", file=sys.stderr)
        sys.exit(5)

    df_norm, inv = _normalize_columns(df)

    required = {"match_id","home","away"}
    if not required.issubset(set(df_norm.columns)):
        found = list(df.columns)
        print(f"::error::Não foi possível normalizar colunas para 'match_id,home,away'. "
              f"Arquivo usado: {used_path}. Colunas encontradas: {found}. "
              f"Mapeamentos reconhecidos: {inv}", file=sys.stderr)
        sys.exit(5)

    # tipagem e limpeza
    df_norm["match_id"] = df_norm["match_id"].astype(str).str.strip()
    df_norm["home"] = df_norm["home"].astype(str).str.strip()
    df_norm["away"] = df_norm["away"].astype(str).str.strip()

    # remove linhas inválidas
    df_norm = df_norm[(df_norm["home"]!="") & (df_norm["away"]!="") & (df_norm["match_id"]!="")]
    if df_norm.empty:
        print(f"::error::Lista de jogos vazia após normalização ({used_path}).", file=sys.stderr)
        sys.exit(5)

    print(f"[apifootball] whitelist: {used_path}  linhas={len(df_norm)}  mapeamento={inv}")
    return df_norm[["match_id","home","away"]].copy()

# ----------------- resolução de times/fixtures -----------------
def _search_team_id(name: str, api_key: str, cache: Dict[str, int]) -> Optional[int]:
    k = name.lower().strip()
    if k in cache:
        return cache[k]
    js = _http_get(f"{BASE_URL}/teams", api_key, {"search": name})
    best_id, best_sc = None, -1
    if js and js.get("response"):
        for it in js["response"]:
            t = it.get("team", {})
            tname = t.get("name") or ""
            tid = t.get("id")
            score = _sim(name, tname)
            if score > best_sc:
                best_sc, best_id = score, tid
    if best_id:
        cache[k] = best_id
    return best_id

def _next_fixture_between(home_id: int, away_name: str, api_key: str) -> Optional[int]:
    js = _http_get(f"{BASE_URL}/fixtures", api_key, {"team": home_id, "next": 50})
    if not js or not js.get("response"):
        return None
    best_fx, best_sc = None, -1
    for fx in js["response"]:
        teams = fx.get("teams", {}) or {}
        away = (teams.get("away") or {}).get("name") or ""
        score = _sim(away_name, away)
        if score > best_sc:
            best_sc = score
            best_fx = (fx.get("fixture") or {}).get("id")
    return best_fx if best_sc >= 70 else None

def _odds_1x2_for_fixture(fixture_id: int, api_key: str) -> Optional[Tuple[float,float,float]]:
    js = _http_get(f"{BASE_URL}/odds", api_key, {"fixture": fixture_id})
    if not js or not js.get("response"):
        return None
    for item in js["response"]:
        for bk in (item.get("bookmakers") or []):
            for bet in (bk.get("bets") or []):
                name = (bet.get("name") or "").lower()
                if "match winner" in name or "1x2" in name or name == "winner":
                    oh=od=oa=None
                    for v in (bet.get("values") or []):
                        label = (v.get("value") or "").strip().upper()
                        val = v.get("odd")
                        try:
                            odd = float(str(val).replace(",", "."))
                        except Exception:
                            continue
                        if label in ("HOME","1"):
                            oh = odd
                        elif label in ("DRAW","X"):
                            od = odd
                        elif label in ("AWAY","2"):
                            oa = odd
                    if all(x is not None for x in (oh,od,oa)):
                        return oh,od,oa
    return None

# ----------------- run -----------------
def _require_env(name: str) -> str:
    val = os.getenv(name, "")
    if not val:
        print(f"::error::{name} não definido no ambiente", file=sys.stderr)
        sys.exit(5)
    return val

def run(rodada_dir: str, season: str, api_key: str, debug: bool=False) -> int:
    wl = _load_whitelist(rodada_dir)

    out_csv = os.path.join(rodada_dir, "odds_apifootball.csv")
    rows: List[Dict[str, Any]] = []
    misses: List[str] = []

    # cache de team IDs por rodada (evita chamadas repetidas)
    cache_file = os.path.join(rodada_dir, "apifoot_team_cache.json")
    team_cache: Dict[str,int] = {}
    if os.path.exists(cache_file):
        try:
            team_cache = json.load(open(cache_file, "r", encoding="utf-8"))
        except Exception:
            team_cache = {}

    for _, r in wl.iterrows():
        match_id, home, away = r["match_id"], r["home"], r["away"]
        print(f"[apifootball] {match_id}: {home} x {away}")

        hid = _search_team_id(home, api_key, team_cache)
        if not hid:
            print(f"[apifootball][WARN] Mandante não encontrado: {home}")
            misses.append(match_id); continue

        fix_id = _next_fixture_between(hid, away, api_key)
        if not fix_id:
            print(f"[apifootball][WARN] Fixture não localizado para {home} x {away}")
            misses.append(match_id); continue

        odds = _odds_1x2_for_fixture(fix_id, api_key)
        if not odds:
            print(f"[apifootball][WARN] Odds 1X2 ausentes para fixture={fix_id} ({home} x {away})")
            misses.append(match_id); continue

        oh, od, oa = odds
        rows.append({
            "match_id": str(match_id),
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

    # escreve saída com cabeçalho sempre
    os.makedirs(rodada_dir, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","odds_home","odds_draw","odds_away","source"])
        wr.writeheader()
        for row in rows:
            wr.writerow(row)

    if debug:
        print(f"[apifootball][DEBUG] coletadas: {len(rows)}  faltantes: {len(misses)} -> {misses}")

    if misses:
        print("::error::Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).", file=sys.stderr)
        return 1
    if len(rows) == 0:
        print("::error::Nenhuma odd coletada (arquivo contém apenas o cabeçalho).", file=sys.stderr)
        return 1

    print(f"[apifootball] OK -> {out_csv}")
    return 0

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    p.add_argument("--season", required=False, default=os.getenv("SEASON", ""))
    p.add_argument("--debug", action="store_true")
    return p.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    api_key = _require_env("X_RAPIDAPI_KEY")
    sys.exit(run(args.rodada, args.season, api_key, args.debug))