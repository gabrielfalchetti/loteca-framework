#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor REAL — TheOddsAPI -> competições nacionais do Brasil
Saída: data/out/<RODADA>/odds_theoddsapi.csv

Destaques:
- regions padrão = "uk,eu,us,au" (todas as regiões válidas).
- Detecta chaves BR via /sports (title contém "Brazil"/"Copa do Brasil" ou key contém "brazil").
- Fallback explícito para chaves BR conhecidas.
- Parâmetro --sports para FORÇAR chaves (ex.: --sports soccer_brazil_serie_a,soccer_brazil_cup).
- Checa /events antes de /odds; logs detalhados e arquivo JSON de debug.
- Sempre grava CSV com colunas fixas (mesmo vazio).

Uso:
  python scripts/ingest_odds_theoddsapi.py --rodada 2025-09-27_1213 \
      --regions "uk,eu,us,au" --sports soccer_brazil_serie_a,soccer_brazil_cup --debug
"""

from __future__ import annotations
import argparse, os, sys, time, unicodedata, math, difflib, json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any
import requests
import numpy as np
import pandas as pd

API = "https://api.the-odds-api.com/v4"
TIMEOUT = 25
RETRY = 3
SLEEP = 0.7
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
        skey  = (it.get("key") or "").lower()
        if ("brazil" in title) or ("copa do brasil" in title) or ("brazil" in skey):
            if it.get("key"):
                sports_keys.append(it["key"])
    sports_keys = list(dict.fromkeys(sports_keys))
    if debug:
        print(f"[theoddsapi] sports detectados (BR): {sports_keys}")
    if not sports_keys:
        sports_keys = FALLBACK_BR_SPORT_KEYS.copy()
        if debug:
            print(f"[theoddsapi] fallback aplicado: {sports_keys}")
    return sports_keys

def _pull_for_sport(key: str, sport_key: str, regions: str, debug=False) -> Dict[str, Any]:
    """Retorna dict com diagnóstico e odds para um sport_key."""
    diag = {"sport": sport_key, "events": 0, "odds_events": 0}
    # 1) listar eventos
    evs = _get(f"{API}/sports/{sport_key}/events", {"apiKey": key}, debug=debug) or []
    diag["events"] = len(evs)
    if debug:
        print(f"[theoddsapi] {sport_key}: eventos listados={diag['events']}")
    if not evs:
        return {"diag": diag, "rows": []}

    # 2) odds H2H
    params = {"apiKey": key, "regions": regions, "markets": "h2h", "oddsFormat": "decimal"}
    js = _get(f"{API}/sports/{sport_key}/odds", params, debug=debug) or []

    rows = []
    for ev in js:
        home = (ev.get("home_team") or "").strip()
        away = (ev.get("away_team") or "").strip()
        h_list, d_list, a_list = [], [], []
        for bk in (ev.get("bookmakers") or []):
            for mkt in (bk.get("markets") or []):
                if (mkt.get("key") or "").lower() == "h2h":
                    for outc in (mkt.get("outcomes") or []):
                        name  = (outc.get("name") or "").upper()
                        price = outc.get("price")
                        try:
                            oddf = float(price)
                        except Exception:
                            continue
                        if name in {"HOME","1"} or (home and name == home.upper()):
                            h_list.append(oddf)
                        elif name in {"DRAW","X"}:
                            d_list.append(oddf)
                        elif name in {"AWAY","2"} or (away and name == away.upper()):
                            a_list.append(oddf)
        def avg(x): return float(np.mean(x)) if x else np.nan
        k1, kx, k2 = avg(h_list), avg(d_list), avg(a_list)
        if not (np.isnan(k1) and np.isnan(kx) and np.isnan(k2)):
            rows.append({"home": home, "away": away, "k1": k1, "kx": kx, "k2": k2})

    diag["odds_events"] = len(rows)
    if debug:
        print(f"[theoddsapi] sport={sport_key}: com_odds={diag['odds_events']}/{diag['events']}")
    return {"diag": diag, "rows": rows}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au", help='regions válidas: "us,uk,eu,au" (combine com vírgula)')
    ap.add_argument("--sports", default="", help="lista de sport keys separadas por vírgula (para FORÇAR), ex.: soccer_brazil_serie_a,soccer_brazil_cup")
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
    dbg_path = os.path.join(out_dir, "theoddsapi_debug.json")

    matches = pd.read_csv(in_path)
    if "home" not in matches.columns or "away" not in matches.columns:
        print("[theoddsapi] ERRO: matches_source.csv precisa de colunas 'home' e 'away'.", file=sys.stderr)
        sys.exit(2)
    matches["home_n"] = matches["home"].apply(_norm)
    matches["away_n"] = matches["away"].apply(_norm)

    # Esportes alvo
    if args.sports.strip():
        sports = [s.strip() for s in args.sports.split(",") if s.strip()]
        if args.debug:
            print(f"[theoddsapi] chaves FORÇADAS: {sports}")
    else:
        sports = _list_brazil_sports(key, debug=args.debug)
        if args.debug:
            print(f"[theoddsapi] chaves BR a consultar (auto/fallback): {sports}")

    pulled_rows = []
    diagnostics = []
    for sp in sports:
        res = _pull_for_sport(key, sp, args.regions, debug=args.debug)
        diagnostics.append(res["diag"])
        pulled_rows.extend(res["rows"])
        time.sleep(SLEEP)

    # Salva debug JSON para auditoria
    with open(dbg_path, "w", encoding="utf-8") as f:
        json.dump({"when": datetime.now(BR_TZ).isoformat(), "regions": args.regions, "sports": sports, "diag": diagnostics}, f, ensure_ascii=False, indent=2)

    # Monta DF de odds (com colunas garantidas)
    rows = []
    for it in pulled_rows:
        hn = _norm(it.get("home","")); an = _norm(it.get("away",""))
        rows.append({
            "home": it.get("home",""), "away": it.get("away",""),
            "home_n": hn, "away_n": an,
            "book": "theoddsapi_avg",
            "k1": it.get("k1", np.nan), "kx": it.get("kx", np.nan), "k2": it.get("k2", np.nan),
        })
    odds_cols = ["home","away","home_n","away_n","book","k1","kx","k2"]
    odds = pd.DataFrame(rows, columns=odds_cols)

    # Match com seus jogos (igualdade → fuzzy 0.90)
    out_rows = []
    if not odds.empty:
        for _, m in matches.iterrows():
            mh, ma = m["home_n"], m["away_n"]
            hit = odds[(odds["home_n"] == mh) & (odds["away_n"] == ma)]
            if hit.empty:
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
            print("[theoddsapi] Nenhum evento BR retornado com odds.")

    df = pd.DataFrame(out_rows, columns=OUT_COLS)
    df.to_csv(out_path, index=False)
    print(f"[theoddsapi] OK -> {out_path} ({len(df)} linhas)")
    if args.debug:
        print(f"[theoddsapi] debug salvo em: {dbg_path}")

if __name__ == "__main__":
    main()
