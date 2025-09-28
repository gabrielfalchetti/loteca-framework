#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor REAL — API-Football (RapidAPI) -> Brasil (todas as competições nacionais principais)
Gera: data/out/<RODADA>/odds_apifootball.csv (colunas: home,away,book,k1,kx,k2,total_line,over,under,ts)

Melhorias:
- Busca fixtures por LIGA (IDs do Brasil) + JANELA DE DATA [-3d, +7d] a partir das datas do matches_source.
- Não depende de mapear time->id por 'search'; usa fixture oficial da liga.
- Faz match por nome normalizado com seu matches_source.csv.
- Logs de debug detalhados (--debug).

Requisitos:
- ENV: RAPIDAPI_KEY com permissão para endpoint /odds
- Input: data/in/<RODADA>/matches_source.csv (colunas: match_id,home,away[,date])

Uso:
  python scripts/ingest_odds_apifootball_rapidapi.py --rodada 2025-09-27_1213 --season 2025 [--debug]
"""

from __future__ import annotations
import argparse, os, sys, time, unicodedata, math, difflib
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Tuple
import requests
import numpy as np
import pandas as pd

API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"
TIMEOUT = 25
RETRY = 3
SLEEP = 0.7
BR_TZ = timezone(timedelta(hours=-3))

# Principais ligas nacionais (API-Football league IDs)
# (A lista cobre A, B, C, D e Copa do Brasil; adicione estaduais se quiser.)
BR_LEAGUES = {
    71: "Serie A",
    72: "Serie B",
    73: "Copa do Brasil",
    128: "Serie C",
    776: "Serie D",
}

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

def _req(path: str, params: Dict[str, Any], key: str, debug=False) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {"X-RapidAPI-Key": key, "X-RapidAPI-Host": API_HOST}
    last = None
    for i in range(RETRY):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(1.5 + i)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.8 + 0.4*i)
    if debug:
        print(f"[apifootball] ERRO {url} params={params} -> {last}", file=sys.stderr)
    return {"response": []}

def _collect_fixtures_for_league(key: str, league_id: int, season: int, d_from: str, d_to: str, debug=False) -> List[Dict[str, Any]]:
    js = _req("/fixtures", {"league": league_id, "season": season, "from": d_from, "to": d_to}, key, debug=debug)
    return js.get("response", []) or []

def _collect_odds_for_fixture(key: str, fixture_id: int, debug=False) -> Tuple[float, float, float]:
    js = _req("/odds", {"fixture": fixture_id}, key, debug=debug)
    resp = js.get("response", []) or []
    h, d, a = [], [], []
    for blk in resp:
        for bm in blk.get("bookmakers", []):
            for bet in bm.get("bets", []):
                # 1X2 geralmente id=1 e/ou nome contendo "Match"
                if str(bet.get("id")) in {"1","12"} or str(bet.get("name","")).lower().startswith("match"):
                    for v in bet.get("values", []):
                        tag = (v.get("value","")).strip().upper()
                        odd = v.get("odd")
                        try:
                            o = float(odd)
                        except Exception:
                            continue
                        if tag in {"1","HOME"}:
                            h.append(o)
                        elif tag in {"X","DRAW"}:
                            d.append(o)
                        elif tag in {"2","AWAY"}:
                            a.append(o)
    def avg(x): return float(np.mean(x)) if x else np.nan
    return avg(h), avg(d), avg(a)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", type=int, default=None, help="ex.: 2025")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    key = os.getenv("RAPIDAPI_KEY","").strip()
    if not key:
        print("[apifootball] ERRO: defina RAPIDAPI_KEY no ambiente.", file=sys.stderr)
        sys.exit(2)

    in_path = os.path.join("data","in",args.rodada,"matches_source.csv")
    if not os.path.isfile(in_path):
        print(f"[apifootball] ERRO: arquivo não encontrado: {in_path}", file=sys.stderr)
        sys.exit(2)

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir,"odds_apifootball.csv")

    matches = pd.read_csv(in_path)
    for c in ["home","away"]:
        if c not in matches.columns:
            print(f"[apifootball] ERRO: matches_source.csv precisa da coluna '{c}'.", file=sys.stderr)
            sys.exit(2)
    if "date" not in matches.columns:
        matches["date"] = ""

    # Normalização p/ match final
    matches["home_n"] = matches["home"].apply(_norm)
    matches["away_n"] = matches["away"].apply(_norm)

    # Deduz janela de data
    dates = []
    for v in matches["date"].astype(str).tolist():
        try:
            dates.append(datetime.fromisoformat(v[:10]).date())
        except Exception:
            pass
    if dates:
        d0 = min(dates)
        d1 = max(dates)
    else:
        # fallback: hoje
        today = datetime.now(BR_TZ).date()
        d0 = today
        d1 = today

    # Janela estendida [-3, +7] para cobrir adiantamentos/adiamentos
    d_from = (d0 - timedelta(days=3)).isoformat()
    d_to   = (d1 + timedelta(days=7)).isoformat()

    season = args.season or d0.year

    if args.debug:
        print(f"[apifootball] Janela: {d_from} -> {d_to}; season={season}; ligas={sorted(BR_LEAGUES.keys())}")

    # 1) Coleta fixtures por liga/temporada e janela
    fixtures = []
    for lid in BR_LEAGUES.keys():
        fx = _collect_fixtures_for_league(key, lid, season, d_from, d_to, debug=args.debug)
        fixtures.extend(fx)
        time.sleep(SLEEP)

    if args.debug:
        print(f"[apifootball] Fixtures coletados (total): {len(fixtures)}")

    # 2) Odds por fixture
    rows = []
    for fx in fixtures:
        try:
            fid = fx["fixture"]["id"]
            th  = (fx["teams"]["home"]["name"] or "").strip()
            ta  = (fx["teams"]["away"]["name"] or "").strip()
        except Exception:
            continue
        k1,kx,k2 = _collect_odds_for_fixture(key, fid, debug=args.debug)
        time.sleep(SLEEP)
        rows.append({"home": th, "away": ta, "k1": k1, "kx": kx, "k2": k2})

    odds = pd.DataFrame(rows, columns=["home","away","k1","kx","k2"])
    odds["home_n"] = odds["home"].apply(_norm)
    odds["away_n"] = odds["away"].apply(_norm)

    # 3) Match com sua lista de jogos
    out_rows = []
    for _, m in matches.iterrows():
        mh, ma = m["home_n"], m["away_n"]
        hit = odds[(odds["home_n"] == mh) & (odds["away_n"] == ma)]
        if hit.empty:
            # fuzzy (>= 0.90)
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
                "book": "apifootball_avg",
                "k1": h["k1"], "kx": h["kx"], "k2": h["k2"],
                "total_line": np.nan, "over": np.nan, "under": np.nan,
                "ts": datetime.now(BR_TZ).isoformat(timespec="seconds"),
            })

    df = pd.DataFrame(out_rows, columns=OUT_COLS)
    df.to_csv(out_path, index=False)
    print(f"[apifootball] OK -> {out_path} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
