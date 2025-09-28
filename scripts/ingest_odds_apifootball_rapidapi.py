#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball_rapidapi.py — coleta odds via API-Football (RapidAPI) sem 'requests',
com suporte a aliases de clubes BR para melhorar matching.

Uso:
  python scripts/ingest_odds_apifootball_rapidapi.py --rodada 2025-09-27_1213 \
    --season 2025 --window 14 --fuzzy 0.9 --aliases data/aliases_br.json --debug
"""

from __future__ import annotations
import argparse, os, sys, math, time, json, unicodedata
from datetime import datetime, timedelta, date, timezone
from typing import Any, Dict, List, Tuple
import urllib.request, urllib.parse, urllib.error

import numpy as np
import pandas as pd

BR_TZ = timezone(timedelta(hours=-3))
HOST = "api-football-v1.p.rapidapi.com"
BASE = f"https://{HOST}/v3"
DEFAULT_LEAGUES = [71, 72, 73, 128, 776]
OUT_COLS = ["home","away","book","k1","kx","k2","total_line","over","under","ts"]

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip().replace(".", " ")
    for suf in [" fc"," afc"," ac"," sc","-sp","-rj"," ec"," e.c."]:
        s = s.replace(suf, "")
    return " ".join(s.split())

def _apply_alias(name: str, aliases: Dict[str, List[str]]) -> str:
    """Se 'name' bater com alguma variação conhecida, retorna a forma canônica (chave do dict)."""
    n = _norm(name)
    # checa exato na chave
    if n in aliases:
        return n
    # procura em variações
    for canon, vars_ in aliases.items():
        if n == canon:
            return canon
        for v in vars_:
            if _norm(v) == n:
                return canon
    return n  # sem mapeamento

def _http_get(path: str, params: Dict[str, Any], key: str, debug: bool=False, retry: int=3, sleep: float=0.6) -> Dict[str, Any]:
    q = urllib.parse.urlencode(params, doseq=True, safe=":,")
    url = f"{BASE}{path}?{q}" if q else f"{BASE}{path}"
    hdrs = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": HOST,
        "Accept": "application/json",
        "User-Agent": "loteca-framework/4.3 (urllib)"
    }
    last_err = None
    for i in range(retry):
        req = urllib.request.Request(url, headers=hdrs, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
                try:
                    js = json.loads(data.decode("utf-8", errors="ignore"))
                except Exception:
                    js = {}
                return js if isinstance(js, dict) else {}
        except urllib.error.HTTPError as e:
            status = e.code
            body = e.read().decode("utf-8", errors="ignore")
            last_err = f"HTTP {status}: {body[:300]}"
            if debug:
                print(f"[apifootball] HTTPError {url} -> {last_err}", file=sys.stderr)
            if status in (429, 500, 502, 503, 504):
                time.sleep(sleep*(i+1))
                continue
            break
        except Exception as e:
            last_err = str(e)
            if debug:
                print(f"[apifootball] ERRO {url} -> {last_err}", file=sys.stderr)
            time.sleep(sleep*(i+1))
    return {}

def _read_matches(rodada: str, aliases: Dict[str, List[str]]) -> pd.DataFrame:
    path = os.path.join("data","in",rodada,"matches_source.csv")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"[apifootball] arquivo não encontrado: {path}")
    df = pd.read_csv(path)
    if "home" not in df.columns or "away" not in df.columns:
        raise RuntimeError("[apifootball] matches_source precisa de colunas 'home' e 'away'")
    if "match_id" not in df.columns:
        df.insert(0, "match_id", range(1, len(df)+1))
    if "date" not in df.columns:
        df["date"] = ""
    # normaliza + aplica aliases
    df["home_n"] = df["home"].apply(lambda x: _apply_alias(x, aliases))
    df["away_n"] = df["away"].apply(lambda x: _apply_alias(x, aliases))
    return df.reset_index(drop=True)

def _daterange_for_window(window_days: int) -> Tuple[str,str]:
    today = date.today()
    start = today - timedelta(days=window_days)
    end   = today + timedelta(days=window_days)
    return (start.isoformat(), end.isoformat())

def _list_fixtures(leagues: List[int], season: int, date_from: str, date_to: str, key: str, debug=False) -> List[Dict[str,Any]]:
    fixtures: List[Dict[str,Any]] = []
    for lg in leagues:
        params = {"league": lg, "season": season, "from": date_from, "to": date_to}
        js = _http_get("/fixtures", params, key, debug=debug)
        resp = js.get("response") or []
        if debug:
            print(f"[apifootball] liga={lg}: fixtures={len(resp)}")
        fixtures.extend(resp)
        time.sleep(0.25)
    return fixtures

def _odds_for_fixture(fixture_id: int, key: str, debug=False) -> Dict[str, float]:
    params = {"fixture": fixture_id, "bet": 1}  # 1X2
    js = _http_get("/odds", params, key, debug=debug)
    resp = js.get("response") or []
    k1s, kxs, k2s = [], [], []
    for item in resp:
        for bk in (item.get("bookmakers") or []):
            for bet in (bk.get("bets") or []):
                if str(bet.get("id")) != "1":
                    continue
                for val in (bet.get("values") or []):
                    label = (val.get("value") or "").strip().upper()
                    odd = val.get("odd")
                    try:
                        dec = float(odd)
                    except Exception:
                        continue
                    if label in ("HOME","1"):
                        k1s.append(dec)
                    elif label in ("DRAW","X"):
                        kxs.append(dec)
                    elif label in ("AWAY","2"):
                        k2s.append(dec)
    def avg(xs): 
        return float(np.mean(xs)) if xs else np.nan
    return {"k1": avg(k1s), "kx": avg(kxs), "k2": avg(k2s)}

def _ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    a_tokens = a.split()
    b_tokens = b.split()
    inter = len(set(a_tokens) & set(b_tokens))
    denom = max(len(set(a_tokens) | set(b_tokens)), 1)
    return inter / denom

def _match_fixture(mh: str, ma: str, home: str, away: str, fuzzy: float) -> bool:
    if mh == home and ma == away:
        return True
    if mh == away and ma == home:
        return True
    if fuzzy > 0:
        sc1 = 0.5*(_ratio(mh, home) + _ratio(ma, away))
        sc2 = 0.5*(_ratio(mh, away) + _ratio(ma, home))
        if max(sc1, sc2) >= fuzzy:
            return True
    return False

def _load_aliases(path: str) -> Dict[str, List[str]]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # normaliza chaves e valores
        ali: Dict[str, List[str]] = {}
        for k, vals in raw.items():
            canon = _norm(k)
            vec = list({ _norm(v) for v in (vals or []) })
            ali[canon] = vec
        return ali
    except Exception as e:
        print(f"[apifootball] AVISO: falha ao carregar aliases '{path}': {e}", file=sys.stderr)
        return {}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--window", type=int, default=14)
    ap.add_argument("--fuzzy", type=float, default=0.90)
    ap.add_argument("--leagues", default="")
    ap.add_argument("--aliases", default="data/aliases_br.json", help="JSON com mapeamento de aliases")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rapid_key = os.getenv("RAPIDAPI_KEY","").strip()
    if not rapid_key:
        print("[apifootball] ERRO: defina RAPIDAPI_KEY no ambiente.", file=sys.stderr)
        sys.exit(2)

    aliases = _load_aliases(args.aliases)
    rodada = args.rodada
    out_dir = os.path.join("data","out",rodada); os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "odds_apifootball.csv")

    matches = _read_matches(rodada, aliases)
    leagues = [int(x) for x in args.leagues.split(",") if x.strip().isdigit()] if args.leagues.strip() else list(DEFAULT_LEAGUES)
    date_from, date_to = _daterange_for_window(args.window)

    print(f"[apifootball] Janela: {date_from} -> {date_to}; season={args.season}; ligas={leagues}")
    print(f"[apifootball] Jogos no CSV: {len(matches)}")

    fixtures = _list_fixtures(leagues, args.season, date_from, date_to, rapid_key, debug=args.debug)
    print(f"[apifootball] Fixtures coletados: {len(fixtures)}")

    # Indexa fixtures com aliases
    cand = []
    for fx in fixtures:
        teams = fx.get("teams") or {}
        th = (teams.get("home") or {}).get("name") or ""
        ta = (teams.get("away") or {}).get("name") or ""
        nh, na = _apply_alias(th, aliases), _apply_alias(ta, aliases)
        fid = (fx.get("fixture") or {}).get("id")
        if nh and na and fid:
            cand.append({"fixture_id": int(fid), "home": th, "away": ta, "home_n": nh, "away_n": na})
    # remove duplicatas
    seen, uniq = set(), []
    for it in cand:
        key = (it["home_n"], it["away_n"], it["fixture_id"])
        if key in seen: 
            continue
        seen.add(key); uniq.append(it)

    rows = []; miss = 0
    for _, m in matches.iterrows():
        mh, ma = m["home_n"], m["away_n"]
        hit = None
        for fx in uniq:
            if _match_fixture(mh, ma, fx["home_n"], fx["away_n"], args.fuzzy):
                hit = fx; break
        if not hit:
            print(f"[apifootball] sem match p/ '{m['home']}' vs '{m['away']}' (norm: {mh} x {ma})")
            miss += 1
            continue
        odds = _odds_for_fixture(hit["fixture_id"], rapid_key, debug=args.debug)
        if all(np.isnan(odds.get(k, np.nan)) for k in ("k1","kx","k2")):
            continue
        rows.append({
            "home": m["home"], "away": m["away"], "book": "apifootball_avg",
            "k1": odds.get("k1", np.nan), "kx": odds.get("kx", np.nan), "k2": odds.get("k2", np.nan),
            "total_line": np.nan, "over": np.nan, "under": np.nan,
            "ts": datetime.now(BR_TZ).isoformat(timespec="seconds"),
        })
        time.sleep(0.15)

    df = pd.DataFrame(rows, columns=OUT_COLS)
    df.to_csv(out_path, index=False)
    print(f"[apifootball] OK -> {out_path} ({len(df)} linhas)")
    if miss:
        print(f"[apifootball] Aviso: {miss} jogo(s) sem match — ver nomes/ligas/janela/aliases.")

if __name__ == "__main__":
    main()
