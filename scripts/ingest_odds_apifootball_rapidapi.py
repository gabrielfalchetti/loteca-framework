#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor REAL — API-Football (RapidAPI) -> Brasil (competições nacionais) com janela, fuzzy e cache

Saída:
  data/out/<RODADA>/odds_apifootball.csv  (colunas: home,away,book,k1,kx,k2,total_line,over,under,ts)

Recursos:
- --window: controla a janela de datas consultada (~[-window//2, +window]); padrão 7 -> [-3,+7].
- --fuzzy:  limiar de similaridade para casar nomes (0–1); padrão 0.88.
- Aliases opcionais em data/aliases_br.json  (mapa "meu_nome" -> "nome_oficial_api").
- Carry-forward: preserva odds da execução anterior para jogos cujo date < hoje e não retornaram agora.
- Logs verbosos com --debug.

Requisitos:
- ENV: RAPIDAPI_KEY com permissão para endpoint /odds.
- Input: data/in/<RODADA>/matches_source.csv  (colunas mín.: home, away [,date])
  - se 'date' não existir, assume hoje para cálculo da janela.

Uso típico:
  python scripts/ingest_odds_apifootball_rapidapi.py --rodada 2025-09-27_1213 --season 2025 --window 14 --fuzzy 0.90 --debug
"""

from __future__ import annotations
import argparse, os, sys, time, json, unicodedata, math, difflib
from datetime import datetime, timedelta, timezone, date
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

# Ligas nacionais principais (IDs na API-Football). Adicione estaduais se desejar.
BR_LEAGUES = {
    71: "Serie A",
    72: "Serie B",
    73: "Copa do Brasil",
    128: "Serie C",
    776: "Serie D",
}

OUT_COLS = ["home","away","book","k1","kx","k2","total_line","over","under","ts"]

# ---------------- Utils ---------------- #

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip().replace(".", " ")
    for suf in [" fc"," afc"," ac"," sc","-sp","-rj"]:
        s = s.replace(suf, "")
    return " ".join(s.split())

def _apply_aliases(name: str, aliases: Dict[str,str]) -> str:
    n = _norm(name)
    return aliases.get(n, name)

def _req(path: str, params: Dict[str, Any], key: str, debug=False) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {"X-RapidAPI-Key": key, "X-RapidAPI-Host": API_HOST}
    last = None
    for i in range(RETRY):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                # rate limit — espera incremental e tenta de novo
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

def _iso_date(s: str) -> date | None:
    try:
        return datetime.fromisoformat(s[:10]).date()
    except Exception:
        return None

def _collect_fixtures_for_league(key: str, league_id: int, season: int, d_from: str, d_to: str, debug=False) -> List[Dict[str, Any]]:
    js = _req("/fixtures", {"league": league_id, "season": season, "from": d_from, "to": d_to}, key, debug=debug)
    return js.get("response", []) or []

def _collect_odds_for_fixture(key: str, fixture_id: int, debug=False) -> Tuple[float, float, float]:
    js = _req("/odds", {"fixture": fixture_id}, key, debug=debug)
    resp = js.get("response", []) or []
    h, d, a = [], [], []
    for blk in resp:
        for bm in blk.get("bookmakers", []) or []:
            for bet in bm.get("bets", []) or []:
                # H2H geralmente id=1 e/ou nome com "Match"
                if str(bet.get("id")) in {"1","12"} or str(bet.get("name","")).lower().startswith("match"):
                    for v in bet.get("values", []) or []:
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

# ---------------- Main ---------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", type=int, default=None, help="ex.: 2025")
    ap.add_argument("--window", type=int, default=7, help="largura ~ [-window//2, +window] (padrão 7)")
    ap.add_argument("--fuzzy", type=float, default=0.88, help="limiar de fuzzy matching (0-1)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    key = os.getenv("RAPIDAPI_KEY","").strip()
    if not key:
        print("[apifootball] ERRO: defina RAPIDAPI_KEY no ambiente.", file=sys.stderr)
        sys.exit(2)

    in_path  = os.path.join("data","in",args.rodada,"matches_source.csv")
    out_dir  = os.path.join("data","out",args.rodada)
    out_path = os.path.join(out_dir,"odds_apifootball.csv")
    if not os.path.isfile(in_path):
        print(f"[apifootball] ERRO: arquivo não encontrado: {in_path}", file=sys.stderr)
        sys.exit(2)
    os.makedirs(out_dir, exist_ok=True)

    # Aliases opcionais
    aliases_path = os.path.join("data","aliases_br.json")
    aliases: Dict[str,str] = {}
    if os.path.isfile(aliases_path):
        try:
            raw = json.load(open(aliases_path, "r", encoding="utf-8"))
            aliases = { _norm(k): v for k,v in raw.items() }
            if args.debug:
                print(f"[apifootball] aliases carregados: {len(aliases)}")
        except Exception as e:
            print(f"[apifootball] aviso: falha ao ler aliases_br.json -> {e}")

    # Carrega matches
    matches = pd.read_csv(in_path)
    for c in ["home","away"]:
        if c not in matches.columns:
            print(f"[apifootball] ERRO: matches_source.csv precisa da coluna '{c}'.", file=sys.stderr)
            sys.exit(2)
    if "date" not in matches.columns:
        matches["date"] = ""

    # Aplica aliases e normaliza
    matches["home"] = matches["home"].apply(lambda s: _apply_aliases(s, aliases))
    matches["away"] = matches["away"].apply(lambda s: _apply_aliases(s, aliases))
    matches["home_n"] = matches["home"].apply(_norm)
    matches["away_n"] = matches["away"].apply(_norm)

    # Datas e janela
    dates = [ _iso_date(v) for v in matches["date"].astype(str).tolist() if _iso_date(v) ]
    if dates:
        d0, d1 = min(dates), max(dates)
    else:
        today = datetime.now(BR_TZ).date()
        d0 = d1 = today

    left = max(1, args.window//2)
    d_from = (d0 - timedelta(days=left)).isoformat()
    d_to   = (d1 + timedelta(days=args.window)).isoformat()
    season = args.season or d0.year

    if args.debug:
        print(f"[apifootball] Janela: {d_from} -> {d_to}; season={season}; ligas={sorted(BR_LEAGUES.keys())}")
        print(f"[apifootball] Jogos no CSV: {len(matches)}")

    # 1) Coleta fixtures por liga/temporada e janela
    fixtures = []
    for lid in BR_LEAGUES.keys():
        js = _req("/fixtures", {"league": lid, "season": season, "from": d_from, "to": d_to}, key, debug=args.debug)
        fixtures.extend(js.get("response", []) or [])
        time.sleep(SLEEP)

    if args.debug:
        print(f"[apifootball] Fixtures coletados: {len(fixtures)}")

    # Monta DF de fixtures normalizado
    fx_rows = []
    for fx in fixtures:
        try:
            fid = fx["fixture"]["id"]
            th  = (fx["teams"]["home"]["name"] or "").strip()
            ta  = (fx["teams"]["away"]["name"] or "").strip()
        except Exception:
            continue
        fx_rows.append({"fixture_id": fid, "home": th, "away": ta, "home_n": _norm(th), "away_n": _norm(ta)})
    fx_df = pd.DataFrame(fx_rows, columns=["fixture_id","home","away","home_n","away_n"])

    # 2) Odds por fixture
    rows = []
    if not fx_df.empty:
        for _, it in fx_df.iterrows():
            k1,kx,k2 = _collect_odds_for_fixture(key, int(it["fixture_id"]), debug=args.debug)
            time.sleep(SLEEP)
            rows.append({
                "home": it["home"], "away": it["away"], "home_n": it["home_n"], "away_n": it["away_n"],
                "k1": k1, "kx": kx, "k2": k2
            })
    odds = pd.DataFrame(rows, columns=["home","away","home_n","away_n","k1","kx","k2"])

    # 3) Match com sua lista (igualdade -> fuzzy >= limiar)
    out_rows = []
    for _, m in matches.iterrows():
        mh, ma = m["home_n"], m["away_n"]
        hit = odds[(odds["home_n"] == mh) & (odds["away_n"] == ma)]
        if hit.empty and not odds.empty:
            best, best_sc = None, 0.0
            for _, o in odds.iterrows():
                sc = 0.5 * (
                    difflib.SequenceMatcher(a=mh, b=o["home_n"]).ratio()
                    + difflib.SequenceMatcher(a=ma, b=o["away_n"]).ratio()
                )
                if sc > best_sc:
                    best_sc, best = sc, o
            if best is not None and best_sc >= float(args.fuzzy):
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
        elif args.debug:
            print(f"[apifootball] sem match p/ '{m['home']}' vs '{m['away']}' (norm: {mh} x {ma})")

    new_df = pd.DataFrame(out_rows, columns=OUT_COLS)

    # ---------------- Carry-forward p/ jogos passados ----------------
    # Se já existe um odds_apifootball.csv anterior, preserva odds de jogos cuja data < hoje
    if os.path.isfile(out_path):
        try:
            prev = pd.read_csv(out_path)
        except Exception:
            prev = pd.DataFrame(columns=OUT_COLS)
        # normaliza chaves p/ juntar
        prev["home_n"] = prev["home"].apply(_norm)
        prev["away_n"] = prev["away"].apply(_norm)
        new_df["home_n"] = new_df["home"].apply(_norm)
        new_df["away_n"] = new_df["away"].apply(_norm)

        # quais jogos já aconteceram?
        now_d = datetime.now(BR_TZ).date()
        matches["date_d"] = matches["date"].astype(str).apply(_iso_date)
        past_keys = set()
        for _, r in matches.iterrows():
            dd = r.get("date_d")
            if dd and dd < now_d:
                past_keys.add((_norm(r["home"]), _norm(r["away"])))

        # para cada jogo passado ausente na coleta atual, recupere do prev
        carry_rows = []
        for (hn, an) in past_keys:
            has_now = ((new_df["home_n"] == hn) & (new_df["away_n"] == an)).any()
            if not has_now:
                hit_prev = prev[(prev["home_n"] == hn) & (prev["away_n"] == an)]
                if not hit_prev.empty:
                    h = hit_prev.iloc[0]
                    carry_rows.append({
                        "home": h["home"], "away": h["away"], "book": "apifootball_cache",
                        "k1": h["k1"], "kx": h["kx"], "k2": h["k2"],
                        "total_line": np.nan, "over": np.nan, "under": np.nan,
                        "ts": h.get("ts", ""),
                    })
        if carry_rows:
            carry_df = pd.DataFrame(carry_rows, columns=OUT_COLS)
            new_df = pd.concat([new_df.drop(columns=["home_n","away_n"], errors="ignore"), carry_df], ignore_index=True, sort=False)
        else:
            new_df = new_df.drop(columns=["home_n","away_n"], errors="ignore")
    # ----------------------------------------------------------------

    new_df.to_csv(out_path, index=False)
    print(f"[apifootball] OK -> {out_path} ({len(new_df)} linhas)")

if __name__ == "__main__":
    main()
