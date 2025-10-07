#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coleta de odds H2H na TheOddsAPI com “modo seguro”.

- Lê o arquivo de jogos reais (CSV) para casar os eventos e evitar times/ligas errados:
  * Por padrão usa MATCHES_PATH (env) ou --matches (override)
  * Formato mínimo: home,away[,date]
    - home/away em texto; date opcional (YYYY-MM-DD)
- Aceita lista de esportes via --sports (ex.: soccer_argentina_primera_division,soccer_brazil_campeonato).
  Se omitida, usa um conjunto razoável de soccer.

Saídas em OUT_DIR (argumento --rodada):
  - odds_theoddsapi.csv         (casados com o matches_source)
  - unmatched_theoddsapi.csv    (coletados mas não casados)
"""

import os
import sys
import csv
import json
import time
import argparse
from typing import Dict, Any, List
import requests
import pandas as pd

DEFAULT_SPORTS = [
    "soccer_brazil_campeonato",
    "soccer_brazil_serie_b",
    "soccer_argentina_primera_division",
    "soccer_argentina_primera_nacional"
]

def die(msg: str, code: int = 2):
    print(f"[theoddsapi-safe] ERRO: {msg}", file=sys.stderr)
    sys.exit(code)

def info(msg: str):
    print(f"[theoddsapi-safe] {msg}")

def get_api_key() -> str:
    k = os.getenv("THEODDS_API_KEY", "").strip()
    if not k:
        die("THEODDS_API_KEY ausente (defina nos Secrets).")
    return k

def read_matches(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        die(f"{path} não encontrado")
    df = pd.read_csv(path)
    for c in ("home", "away"):
        if c not in df.columns:
            die(f"coluna ausente em {path}: {c}")
    # normalização
    for c in ("home", "away"):
        df[c] = df[c].astype(str).str.strip().str.lower()
    return df

def norm(s: str) -> str:
    return str(s or "").strip().lower()

def match_key(home: str, away: str) -> str:
    return f"{norm(home)}__vs__{norm(away)}"

def fetch_odds_for_sport(api_key: str, sport_key: str, regions: str, debug: bool=False) -> List[Dict[str, Any]]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {"apiKey": api_key, "regions": regions, "markets": "h2h"}
    if debug:
        print(f"[theoddsapi-safe][DEBUG] GET {url} {params}")
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        print(f"[theoddsapi-safe] AVISO: {sport_key} -> HTTP {r.status_code} {r.text[:200]}")
        return []
    try:
        return r.json()
    except Exception:
        return []

def flatten_event(ev: Dict[str, Any]) -> Dict[str, Any]:
    """Retorna dict simples com home/away e odds médias (h2h)."""
    # cada bookmaker tem markets -> outcomes [{name: 'Home/Away/Draw', price: float}]
    # outcomes de h2h: HOME, DRAW (se houver), AWAY
    home_name, away_name = "", ""
    odds_home, odds_draw, odds_away = None, None, None

    # participantes
    try:
        comps = ev.get("bookmakers", [])
        # alguns payloads trazem "home_team"/"away_team" direto:
        home_name = ev.get("home_team") or ""
        away_name = ev.get("away_team") or ""
        # se vazio, tenta derived de outcomes do primeiro bookmaker:
        if not home_name or not away_name:
            for b in comps:
                for m in b.get("markets", []):
                    if m.get("key") == "h2h":
                        names = [o.get("name","") for o in m.get("outcomes", [])]
                        if len(names) >= 2:
                            # heurística
                            home_name = names[0]
                            away_name = names[-1]
                        break
                if home_name and away_name:
                    break
    except Exception:
        pass

    # odds (média simples entre bookmakers)
    oh, od, oa, nh, nd, na = 0.0, 0.0, 0.0, 0, 0, 0
    for b in ev.get("bookmakers", []):
        for m in b.get("markets", []):
            if m.get("key") == "h2h":
                vals = {o.get("name","").strip().upper(): o.get("price") for o in m.get("outcomes", [])}
                # tentar mapear nomes comumente usados
                for lbl, price in vals.items():
                    if price is None:
                        continue
                    if lbl in ("HOME", home_name.upper()):
                        oh += float(price); nh += 1
                    elif lbl in ("DRAW","EMPATE","X"):
                        od += float(price); nd += 1
                    elif lbl in ("AWAY", away_name.upper()):
                        oa += float(price); na += 1
    if nh > 0: odds_home = oh / nh
    if nd > 0: odds_draw = od / nd
    if na > 0: odds_away = oa / na

    return {
        "team_home": home_name,
        "team_away": away_name,
        "odds_home": odds_home,
        "odds_draw": odds_draw,
        "odds_away": odds_away
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="OUT_DIR (ex: data/out/123456)")
    ap.add_argument("--regions", default=os.getenv("REGIONS","uk,eu,us,au"))
    ap.add_argument("--sports", default=os.getenv("ODDS_SPORTS",""))
    ap.add_argument("--matches", default=os.getenv("MATCHES_PATH","data/in/matches_source.csv"))
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    api_key = get_api_key()
    # jogos reais
    dfm = read_matches(args.matches)

    sports = [s.strip() for s in (args.sports or "").split(",") if s.strip()]
    if not sports:
        sports = DEFAULT_SPORTS

    collected: List[Dict[str, Any]] = []
    for sk in sports:
        evs = fetch_odds_for_sport(api_key, sk, args.regions, debug=args.debug)
        for e in evs:
            flat = flatten_event(e)
            if flat["team_home"] and flat["team_away"]:
                flat["match_key"] = match_key(flat["team_home"], flat["team_away"])
                collected.append(flat)
        time.sleep(0.2)

    # agora casamos com dfm (home/away) — tolerância básica de contains
    valid_rows, unmatched_rows = [], []
    if collected:
        # normaliza um mapa possível de chaves do CSV
        dfm["_mk"] = dfm.apply(lambda r: match_key(r["home"], r["away"]), axis=1)
        want_keys = set(dfm["_mk"].unique())

        for row in collected:
            mk = match_key(row["team_home"], row["team_away"])
            if mk in want_keys and all(row.get(k) for k in ("odds_home","odds_away")):
                valid_rows.append({
                    "match_key": mk,
                    "team_home": row["team_home"],
                    "team_away": row["team_away"],
                    "odds_home": row["odds_home"],
                    "odds_draw": row["odds_draw"],
                    "odds_away": row["odds_away"]
                })
            else:
                unmatched_rows.append(row)

    # escreve arquivos
    def write_csv(path: str, rows: List[Dict[str, Any]]):
        cols = sorted({k for r in rows for k in r.keys()}) if rows else []
        with open(path,"w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow(r)

    out_ok = os.path.join(out_dir, "odds_theoddsapi.csv")
    out_unm = os.path.join(out_dir, "unmatched_theoddsapi.csv")
    write_csv(out_ok, valid_rows)
    write_csv(out_unm, unmatched_rows)

    print(f"[theoddsapi-safe] linhas -> {json.dumps({os.path.basename(out_ok): len(valid_rows), os.path.basename(out_unm): len(unmatched_rows)})}")

    # se nada válido, ainda assim saímos com erro 2 para o consenso saber que só terá API-Football
    if not valid_rows:
        die("nenhuma linha válida (odds_theoddsapi.csv vazio).", code=2)

if __name__ == "__main__":
    main()