#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor REAL — TheOddsAPI -> competições nacionais do Brasil
Gera: data/out/<RODADA>/odds_theoddsapi.csv

Requisitos:
- Secret/ENV: THEODDSAPI_KEY
- Input: data/in/<RODADA>/matches_source.csv (match_id,home,away[,date])
Uso:
  python scripts/ingest_odds_theoddsapi.py --rodada 2025-09-27_1213 [--regions br,eu,uk,us] [--debug]
"""

from __future__ import annotations
import argparse, os, sys, time, unicodedata, math, difflib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any
import requests
import numpy as np
import pandas as pd

API = "https://api.the-odds-api.com/v4"
TIMEOUT = 20
RETRY = 3
SLEEP = 0.6
BR_TZ = timezone(timedelta(hours=-3))

OUT_COLS = ["home","away","book","k1","kx","k2","total_line","over","under","ts"]

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
        # Exemplos: "Soccer - Brazil - Serie A", "Soccer - Brazil - Serie B", "Copa do Brasil"
        if "brazil" in title or "copa do brasil" in title:
            k = it.get("key")
            if k:
                sports.append(k)
    # remover duplicatas mantendo ordem
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
        home = (ev.get("home_team") or "").strip()
        away = (ev.get("away_team") or "").strip()
        if not home or not away:
            # fallback via participantes/outcomes (quando payload vem sem away_team)
            teams = set()
            for bk in ev.get("bookmakers", []):
                for mkt in bk.get("markets", []):
                    if (mkt.get("key") or "").lower() == "h2h":
                        for oc in mkt.get("outcomes", []):
                            nm = (oc.get("name") or "").strip()
                            if nm:
                                teams.add(nm)
            if len(teams) >= 2 and not home and not away:
                # pega dois quaisquer (heurística)
                tlist = list(teams)
                home, away = tlist[0], tlist[1]

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
                        if name in {"HOME","1"} or (home and name == home.upper()):
                            h_list.append(o)
                        elif name in {"DRAW","X"}:
                            d_list.append(o)
                        elif name in {"AWAY","2"} or (away and name == away.upper()):
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

    pulled: List[Dict[str, Any]] = []
    for sp in sports:
        data = _pull_odds_for_sport(key, sp, args.regions, debug=args.debug)
        pulled.extend(data)
        time.sleep(SLEEP)

    # --- Monta DF de odds com colunas garantidas, mesmo vazio ---
    rows = []
    for it in pulled:
        hn = _norm(it.get("home","")); an = _norm(it.get("away",""))
        rows.append({
            "home": it.get("home",""),
            "away": it.get("away",""),
            "home_n": hn,
            "away_n": an,
            "book": "theoddsapi_avg",
            "k1": it.get("k1", np.nan),
            "kx": it.get("kx", np.nan),
            "k2": it.get("k2", np.nan),
        })

    odds_cols = ["home","away","home_n","away_n","book","k1","kx","k2"]
    odds = pd.DataFrame(rows, columns=odds_cols)  # << garante colunas mesmo quando vazio

    # --- Match com a lista de jogos (normalizado) ---
    out_rows = []
    if not odds.empty:
        for _, m in matches.iterrows():
            mh, ma = m["home_n"], m["away_n"]
            hit = odds[(odds["home_n"] == mh) & (odds["away_n"] == ma)]
            if hit.empty:
                # fuzzy matching simples
                best = None
                best_sc = 0.0
                for _, o in odds.iterrows():
                    sc = 0.5 * (
                        difflib.SequenceMatcher(a=mh, b=o["home_n"]).ratio()
                        + difflib.SequenceMatcher(a=ma, b=o["away_n"]).ratio()
                    )
                    if sc > best_sc:
                        best_sc, best = sc, o
                if best is not None and best_sc >= 0.90:
                    hit = pd.DataFrame([best])
            if not hit.empty:
                h = hit.iloc[0]
                out_rows.append({
                    "home": m["home"], "away": m["away"],
                    "book": "theoddsapi_avg",
                    "k1": h["k1"], "kx": h["kx"], "k2": h["k2"],
                    "total_line": np.nan, "over": np.nan, "under": np.nan,
                    "ts": datetime.now(BR_TZ).isoformat(timespec="seconds"),
                })
    else:
        if args.debug:
            print("[theoddsapi] Nenhuma partida retornada pela API (pulled vazio).")

    # --- Salva CSV com colunas fixas, mesmo sem linhas ---
    df = pd.DataFrame(out_rows, columns=OUT_COLS)
    df.to_csv(out_path, index=False)
    print(f"[theoddsapi] OK -> {out_path} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
