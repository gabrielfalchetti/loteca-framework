#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, json, time
import argparse
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

API_BASE = "https://api.the-odds-api.com/v4"

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--markets", default="h2h")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

def load_matches(rodada):
    fn = f"data/in/{rodada}/matches_source.csv"
    df = pd.read_csv(fn)
    # campos obrigatórios
    for col in ["match_id","home_team","away_team","sport_key_theoddsapi"]:
        if col not in df.columns:
            raise ValueError(f"Campo obrigatório ausente: {col}")
    return df

def fetch_odds_for_match(apikey, sport_key, home, away, regions, markets, debug=False):
    # TheOddsAPI trabalha por sport_key, e retorna lista de jogos com "home_team"/"away_team".
    params = {
        "apiKey": apikey,
        "regions": regions,
        "markets": markets,
        "oddsFormat": "decimal",
        "dateFormat": "iso"
    }
    url = f"{API_BASE}/sports/{sport_key}/odds"
    r = requests.get(url, params=params, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"TheOddsAPI error {r.status_code}: {r.text}")
    data = r.json()
    # match exato por nome (case-insensitive), normalizando
    def norm(x): return x.strip().lower()
    H, A = norm(home), norm(away)
    rows = []
    for ev in data:
        if norm(ev.get("home_team","")) == H and norm(ev.get("away_team","")) == A:
            # extrai h2h
            for bk in ev.get("bookmakers", []):
                for mk in bk.get("markets", []):
                    if mk.get("key") != "h2h": 
                        continue
                    outcomes = mk.get("outcomes", [])
                    rec = {"bookmaker": bk.get("title",""), "last_update": mk.get("last_update","")}
                    # outcomes podem vir como [home,draw,away] variando
                    for out in outcomes:
                        k = out.get("name","").strip().lower()
                        if k == norm(home):
                            rec["home_price"] = out.get("price")
                        elif k == "draw":
                            rec["draw_price"] = out.get("price")
                        elif k == norm(away):
                            rec["away_price"] = out.get("price")
                    rows.append(rec)
    return rows

def main():
    args = parse_args()
    rodada = args.rodada
    outdir = f"data/out/{rodada}"
    os.makedirs(outdir, exist_ok=True)
    apikey = os.getenv("THEODDS_API_KEY", "").strip()
    if not apikey:
        print("[theoddsapi] ERRO: THEODDS_API_KEY não definido.", file=sys.stderr)
        sys.exit(2)

    dfm = load_matches(rodada)
    all_rows = []
    for _, m in dfm.iterrows():
        mid = m["match_id"]
        home = m["home_team"]
        away = m["away_team"]
        sport_key = m["sport_key_theoddsapi"]
        try:
            rows = fetch_odds_for_match(apikey, sport_key, home, away, args.regions, args.markets, args.debug)
            for r in rows:
                r["match_id"] = mid
                r["home_team"] = home
                r["away_team"] = away
                r["sport_key"] = sport_key
                all_rows.append(r)
            if args.debug:
                print(f"[theoddsapi] {mid}: {len(rows)} linhas")
        except Exception as e:
            print(f"[theoddsapi] AVISO {mid}: {e}", file=sys.stderr)

    out_path = f"{outdir}/odds_theoddsapi.csv"
    if not all_rows:
        # grava vazio mas sinaliza “sem odds”
        pd.DataFrame(columns=["match_id","home_team","away_team","sport_key","bookmaker","last_update","home_price","draw_price","away_price"]).to_csv(out_path, index=False)
        print(f"[theoddsapi] OK -> {out_path} (0 linhas)")
        sys.exit(3)  # para o safe acusar “sem odds”
    pd.DataFrame(all_rows).to_csv(out_path, index=False)
    print(f"[theoddsapi] OK -> {out_path} ({len(all_rows)} linhas)")

if __name__ == "__main__":
    main()
