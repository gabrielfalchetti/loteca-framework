#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor REAL — TheOddsAPI -> competições nacionais do Brasil
Gera: data/out/<RODADA>/odds_theoddsapi.csv

Requisitos:
- Secret/ENV: THEODDSAPI_KEY
- Input: data/in/<RODADA>/matches_source.csv (match_id,home,away[,date])
Uso:
  python scripts/ingest_odds_theoddsapi.py --rodada 2025-09-27_1213 [--regions br,eu,uk] [--debug]
"""

from __future__ import annotations
import argparse, os, sys, time, unicodedata, math, difflib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Tuple
import requests
import numpy as np
import pandas as pd

API = "https://api.the-odds-api.com/v4"
TIMEOUT = 20
RETRY = 3
SLEEP = 0.6
BR_TZ = timezone(timedelta(hours=-3))

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
    sports = []
    for it in js or []:
        title = (it.get("title") or "").lower()
        # títulos típicos: "Soccer - Brazil - Serie A", "Soccer - Brazil - Serie B", "Copa do Brasil"
        if "brazil" in title or "copa do brasil" in title:
            sports.append(it.get("key"))
    return list(dict.fromkeys(sports))

def _pull_odds_for_sport(key: str, sport: str, regions: str, debug=False) -> List[Dict[str, Any]]:
    params = {
        "apiKey": key,
        "regions": regions,         # ex.: "br,eu,uk,us"
        "markets": "h2h",
        "oddsFormat": "decimal"
    }
    js = _get(f"{API}/sports/{sport}/odds", params, debug=debug)
    out = []
    for ev in js or []:
        home = ev.get("home_team") or ""
        # TheOddsAPI não marca explicitamente away_team; inferimos do participants
        # mas o payload usual tem 'away_team' também:
        away = ev.get("away_team") or ""
        # agregação por média simples entre bookmakers
        h_list, d_list, a_list = [], [], []
        for bk in ev.get("bookmakers", []):
            for mkt in bk.get("markets", []):
                if (mkt.get("key") or "").lower() == "h2h":
                    for outc in mkt.get("outcomes", []):
                        name = (outc.get("name") or "").upper()
                        price = outc.get("price")
                        try:
                            o = float(price)
                        except Exception:
                            continue
                        if name in {"HOME","1"} or name == home.upper():
                            h_list.append(o)
                        elif name in {"DRAW","X"}:
                            d_list.append(o)
                        elif name in {"AWAY","2"} or name == away.upper():
                            a_list.append(o)
        def avg(x): return float(np.mean(x)) if x else np.nan
        out.append({
            "home": home, "away": away,
            "k1": avg(h_list), "kx": avg(d_list), "k2": avg(a_list)
        })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="br,eu,uk,us")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    key = os.getenv("THEODDSAPI_KEY", "").strip()
    if not key:
        print("[theoddsapi] ERRO: defina THEODDSAPI_KEY no ambiente.", file=sys.stderr)
        sys.exit(2)

    in_path = os.path.join("data","in",args.rodada,"matches_source.csv")
    if not os.path.isfile(in_path):
        print(f"[theoddsapi] ERRO: arquivo não encontrado: {in_path}", file=sys.stderr)
        sys.exit(2)

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "odds_theoddsapi.csv")

    matches = pd.read_csv(in_path)
    if "home" not in matches.columns or "away" not in matches.columns:
        print("[theoddsapi] ERRO: matches_source.csv precisa de colunas 'home' e 'away'.", file=sys.stderr)
        sys.exit(2)
    matches["home_n"] = matches["home"].apply(_norm)
    matches["away_n"] = matches["away"].apply(_norm)

    sports = _list_brazil_sports(key, debug=args.debug)
    if args.debug:
        print(f"[theoddsapi] sports BR detectados: {sports}")

    pulled = []
    for sp in sports:
        data = _pull_odds_for_sport(key, sp, args.regions, debug=args.debug)
        pulled.extend(data)
        time.sleep(SLEEP)

    # Index por nome normalizado
    rows = []
    for it in pulled:
        hn = _norm(it["home"]); an = _norm(it["away"])
        rows.append({"home": it["home"], "away": it["away"], "home_n": hn, "away_n": an,
                     "book": "theoddsapi_avg", "k1": it["k1"], "kx": it["kx"], "k2": it["k2"]})
    odds = pd.DataFrame(rows)

    # Match com sua lista de jogos (normalizado). Faz join por igualdade; se falhar, tenta fuzzy 0.9
    out_rows = []
    for _, m in matches.iterrows():
        mh, ma = m["home_n"], m["away_n"]
        hit = odds[(odds["home_n"] == mh) & (odds["away_n"] == ma)]
        if hit.empty:
            # tenta fuzzy dentro do mesmo conjunto
            best = None
            best_sc = 0.0
            for _, o in odds.iterrows():
                sc = 0.5 * (difflib.SequenceMatcher(a=mh, b=o["home_n"]).ratio()
                            + difflib.SequenceMatcher(a=ma, b=o["away_n"]).ratio())
                if sc > best_sc:
                    best_sc, best = sc, o
            if best is not None and best_sc >= 0.90:
                hit = pd.DataFrame([best])
        if not hit.empty:
            h = hit.iloc[0]
            out_rows.append({
                "home": m["home"], "away": m["away"],
                "book": "theoddsapi_avg", "k1": h["k1"], "kx": h["kx"], "k2": h["k2"],
                "total_line": np.nan, "over": np.nan, "under": np.nan,
                "ts": datetime.now(BR_TZ).isoformat(timespec="seconds"),
            })
    df = pd.DataFrame(out_rows, columns=["home","away","book","k1","kx","k2","total_line","over","under","ts"])
    df.to_csv(out_path, index=False)
    print(f"[theoddsapi] OK -> {out_path} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
