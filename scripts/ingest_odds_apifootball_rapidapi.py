#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor REAL — API-Football (RapidAPI) -> competições nacionais do Brasil
Gera: data/out/<RODADA>/odds_apifootball.csv com colunas:
home,away,book,k1,kx,k2,total_line,over,under,ts

Requisitos:
- Secret/ENV: RAPIDAPI_KEY
- Input: data/in/<RODADA>/matches_source.csv (match_id,home,away[,date])
Uso:
  python scripts/ingest_odds_apifootball_rapidapi.py --rodada 2025-09-27_1213 [--season 2025] [--debug]
"""

from __future__ import annotations
import argparse, os, sys, time, json, math, unicodedata, difflib
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, List, Any
import requests
import numpy as np
import pandas as pd

API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"
TIMEOUT = 20
RETRY = 3
SLEEP_BETWEEN = 0.8  # respeitar rate limit free
BR_TZ = timezone(timedelta(hours=-3))

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip().replace(".", " ")
    for suf in [" fc", " afc", " ac", " sc", " - sp", " - rj", "-sp", "-rj"]:
        s = s.replace(suf, "")
    return " ".join(s.split())

def _req(path: str, params: Dict[str, Any], key: str, debug=False) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {"X-RapidAPI-Key": key, "X-RapidAPI-Host": API_HOST}
    last_err = None
    for i in range(RETRY):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(2.0 + i)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1.0 + i * 0.5)
    if debug:
        print(f"[apifootball] ERRO {url} params={params} -> {last_err}", file=sys.stderr)
    return {"errors": [{"message": str(last_err)}], "response": []}

def _map_brazil_team_ids(key: str, names: List[str], debug=False) -> Dict[str, int]:
    """Resolve team_id por nome (apenas Brasil) usando /teams?search= e country=Brazil."""
    out: Dict[str, int] = {}
    cache: Dict[str, List[Tuple[str, int]]] = {}
    for nm in set(names):
        q = nm.strip()
        if not q:
            continue
        qn = _norm(q)
        if qn in out:
            continue
        js = _req("/teams", {"search": q, "country": "Brazil"}, key, debug=debug)
        cand = []
        for it in js.get("response", []):
            t = it.get("team", {})
            tid = t.get("id")
            tname = t.get("name") or ""
            cand.append((_norm(tname), tid))
        cache[qn] = cand
        # match por igualdade ou fuzzy > 0.85
        best_id = None
        best_score = 0.0
        for (nn, tid) in cand:
            if nn == qn:
                best_id, best_score = tid, 1.0
                break
            sc = difflib.SequenceMatcher(a=qn, b=nn).ratio()
            if sc > best_score:
                best_id, best_score = tid, sc
        if best_id:
            out[qn] = best_id
        elif cand:
            out[qn] = cand[0][1]
        time.sleep(SLEEP_BETWEEN)
    if debug:
        print(f"[apifootball] map teams -> {out}")
    return out

def _find_fixture_id(key: str, hid: int, aid: int, date_iso: str | None, season: int | None, debug=False) -> int | None:
    """Tenta achar fixture id pelo par de equipes e (opcionalmente) data/temporada."""
    # 1) H2H direta + season
    params = {"h2h": f"{hid}-{aid}"}
    if season:
        params["season"] = season
    if date_iso:
        # API não filtra por data com h2h; então usamos janela por 'from'/'to' via /fixtures
        pass
    js = _req("/fixtures", params, key, debug=debug)
    resp = js.get("response", [])
    if date_iso:
        try:
            dt = datetime.fromisoformat(date_iso).date()
        except Exception:
            dt = None
        if dt:
            # tolera +/- 2 dias
            rng = {dt + timedelta(days=d) for d in range(-2, 3)}
            for fx in resp:
                try:
                    dfx = fx["fixture"]["date"][:10]
                    dfxd = datetime.fromisoformat(dfx).date()
                    if dfxd in rng:
                        return fx["fixture"]["id"]
                except Exception:
                    continue
    if resp:
        return resp[0]["fixture"]["id"]
    # 2) Busca por ambos teams + janela de datas (se houver)
    if date_iso:
        try:
            dt = datetime.fromisoformat(date_iso).date()
        except Exception:
            dt = None
        if dt:
            params = {
                "team": hid,
                "season": season or datetime.now(BR_TZ).year,
                "from": (dt - timedelta(days=7)).isoformat(),
                "to": (dt + timedelta(days=7)).isoformat(),
            }
            js = _req("/fixtures", params, key, debug=debug)
            cand = []
            for fx in js.get("response", []):
                teams = fx.get("teams", {})
                th = teams.get("home", {}).get("id")
                ta = teams.get("away", {}).get("id")
                if {th, ta} == {hid, aid}:
                    cand.append((fx["fixture"]["id"], fx["fixture"]["date"]))
            if cand:
                cand.sort(key=lambda x: x[1])
                return cand[0][0]
    return None

def _odds_for_fixture(key: str, fixture_id: int, debug=False) -> Tuple[float, float, float]:
    """Retorna (k1,kx,k2) médios (decimal) agregando casas."""
    js = _req("/odds", {"fixture": fixture_id}, key, debug=debug)
    resp = js.get("response", [])
    prices_h, prices_d, prices_a = [], [], []
    for blk in resp:
        for bm in blk.get("bookmakers", []):
            for mkt in bm.get("bets", []):
                # API-Football nomeia 1X2 como "Match Winner" (id 1) ou "Home/Away" dependendo do provider
                if str(mkt.get("id")) in {"1", "12"} or str(mkt.get("name","")).lower().startswith("match"):
                    vals = mkt.get("values", [])
                    for v in vals:
                        val = v.get("value","").strip().upper()
                        odd = v.get("odd")
                        try:
                            o = float(odd)
                        except Exception:
                            continue
                        if val in {"1","HOME"}:
                            prices_h.append(o)
                        elif val in {"X","DRAW"}:
                            prices_d.append(o)
                        elif val in {"2","AWAY"}:
                            prices_a.append(o)
    def avg(x): return float(np.mean(x)) if x else np.nan
    return avg(prices_h), avg(prices_d), avg(prices_a)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", type=int, default=None, help="ex.: 2025 (p/ filtrar fixtures)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    api_key = os.getenv("RAPIDAPI_KEY", "").strip()
    if not api_key:
        print("[apifootball] ERRO: defina RAPIDAPI_KEY no ambiente.", file=sys.stderr)
        sys.exit(2)

    in_path = os.path.join("data","in",args.rodada,"matches_source.csv")
    if not os.path.isfile(in_path):
        print(f"[apifootball] ERRO: arquivo não encontrado: {in_path}", file=sys.stderr)
        sys.exit(2)

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "odds_apifootball.csv")

    matches = pd.read_csv(in_path)
    for c in ["home","away"]:
        if c not in matches.columns:
            print(f"[apifootball] ERRO: coluna ausente em matches_source.csv: {c}", file=sys.stderr)
            sys.exit(2)
    if "date" not in matches.columns:
        matches["date"] = ""

    matches["home_n"] = matches["home"].apply(_norm)
    matches["away_n"] = matches["away"].apply(_norm)

    # Mapeia IDs de times (Brasil) uma única vez
    unique_names = list(set(matches["home"].astype(str).tolist() + matches["away"].astype(str).tolist()))
    name2id = _map_brazil_team_ids(api_key, unique_names, debug=args.debug)

    rows = []
    for _, r in matches.iterrows():
        hn, an = r["home_n"], r["away_n"]
        hid = name2id.get(hn)
        aid = name2id.get(an)
        if not hid or not aid:
            if args.debug:
                print(f"[apifootball] sem team_id para {r['home']} vs {r['away']}")
            continue
        date_iso = None
        if str(r.get("date","")).strip():
            try:
                date_iso = str(r["date"])[:10]
            except Exception:
                date_iso = None

        fx_id = _find_fixture_id(api_key, hid, aid, date_iso, args.season, debug=args.debug)
        time.sleep(SLEEP_BETWEEN)
        if not fx_id:
            if args.debug:
                print(f"[apifootball] fixture não encontrado: {r['home']} vs {r['away']}")
            continue

        k1,kx,k2 = _odds_for_fixture(api_key, fx_id, debug=args.debug)
        time.sleep(SLEEP_BETWEEN)

        rows.append({
            "home": r["home"], "away": r["away"],
            "book": "apifootball_avg", "k1": k1, "kx": kx, "k2": k2,
            "total_line": np.nan, "over": np.nan, "under": np.nan,
            "ts": datetime.now(BR_TZ).isoformat(timespec="seconds")
        })

    df = pd.DataFrame(rows, columns=["home","away","book","k1","kx","k2","total_line","over","under","ts"])
    df.to_csv(out_path, index=False)
    print(f"[apifootball] OK -> {out_path} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
