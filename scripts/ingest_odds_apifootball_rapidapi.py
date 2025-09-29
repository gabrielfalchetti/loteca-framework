#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, argparse
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from rapidfuzz import fuzz, process

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--window", type=int, default=21, help="dias para trás e para frente a partir do kickoff_utc")
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

def load_matches(rodada):
    fn = f"data/in/{rodada}/matches_source.csv"
    df = pd.read_csv(fn)
    for col in ["match_id","home_team","away_team","league_id_apifootball","kickoff_utc","season"]:
        if col not in df.columns:
            raise ValueError(f"Campo obrigatório ausente: {col}")
    # normaliza datas
    df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce")
    return df

def load_aliases(path):
    if not os.path.exists(path):
        return {"teams":{}, "leagues":{}}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def canon(name, aliases):
    name = str(name).strip()
    if name in aliases.get("teams", {}):
        return name
    # tenta achar chave cujo aliases contenham o nome informado
    for k, arr in aliases.get("teams", {}).items():
        if name == k or name in arr:
            return k
    return name

def rf_ratio(a,b): return fuzz.ratio(a.lower(), b.lower())

def api_get(path, params, key):
    headers = {
        "x-rapidapi-host": API_HOST,
        "x-rapidapi-key": key
    }
    r = requests.get(API_BASE + path, headers=headers, params=params, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"RapidAPI {r.status_code}: {r.text}")
    return r.json()

def search_fixture_id(key, league_id, season, hteam, ateam, dt_from, dt_to, debug=False):
    res = api_get("/fixtures", {
        "league": league_id,
        "season": season,
        "from": dt_from.strftime("%Y-%m-%d"),
        "to": dt_to.strftime("%Y-%m-%d")
    }, key)
    best = None
    for it in res.get("response", []):
        th = it["teams"]["home"]["name"]
        ta = it["teams"]["away"]["name"]
        score = (rf_ratio(hteam, th) + rf_ratio(ateam, ta)) / 2.0
        cand = (score, it["fixture"]["id"])
        if best is None or cand[0] > best[0]:
            best = cand
        if debug:
            print(f"[apifootball][cand] {th} vs {ta} -> {score:.1f}")
    if best and best[0] >= 80:  # threshold razoável
        return int(best[1])
    return None

def fetch_odds_fixture(key, fixture_id):
    out = api_get("/odds", {"fixture": fixture_id}, key)
    rows = []
    for resp in out.get("response", []):
        for bk in resp.get("bookmakers", []):
            # mercado 1x2 costuma vir como "Match Winner" em alguns provedores
            for md in bk.get("bets", []):
                if md.get("name","").lower() in ["match winner","winner","1x2","fulltime result","match result"]:
                    rec = {
                        "bookmaker": bk.get("name",""),
                        "market": md.get("name",""),
                        "home_price": None,
                        "draw_price": None,
                        "away_price": None
                    }
                    for val in md.get("values", []):
                        nm = val.get("value","").lower()
                        odd = val.get("odd")
                        if nm in ["home","1","home team"]:
                            rec["home_price"] = odd
                        elif nm in ["draw","x"]:
                            rec["draw_price"] = odd
                        elif nm in ["away","2","away team"]:
                            rec["away_price"] = odd
                    rows.append(rec)
    return rows

def main():
    args = parse_args()
    rodada = args.rodada
    outdir = f"data/out/{rodada}"
    os.makedirs(outdir, exist_ok=True)
    key = os.getenv("RAPIDAPI_KEY", "").strip()
    if not key:
        print("[apifootball] ERRO: RAPIDAPI_KEY não definido.", file=sys.stderr)
        sys.exit(2)

    aliases = load_aliases(args.aliases)
    dfm = load_matches(rodada)

    all_rows = []
    for _, m in dfm.iterrows():
        mid = m["match_id"]
        season = int(m["season"])
        league_id = int(m["league_id_apifootball"])
        ko = m["kickoff_utc"]
        hteam = canon(m["home_team"], aliases)
        ateam = canon(m["away_team"], aliases)

        dt_from = (ko - timedelta(days=args.window)).to_pydatetime()
        dt_to   = (ko + timedelta(days=args.window)).to_pydatetime()

        try:
            fx = search_fixture_id(key, league_id, season, hteam, ateam, dt_from, dt_to, args.debug)
            if not fx:
                print(f"[apifootball] sem fixture p/ {mid} '{hteam}' vs '{ateam}'", file=sys.stderr)
                continue
            if args.debug:
                print(f"[apifootball] {mid} fixture_id={fx}")
            rows = fetch_odds_fixture(key, fx)
            for r in rows:
                r["match_id"] = mid
                r["home_team"] = hteam
                r["away_team"] = ateam
                r["fixture_id"] = fx
                r["league_id"] = league_id
                all_rows.append(r)
        except Exception as e:
            print(f"[apifootball] AVISO {mid}: {e}", file=sys.stderr)

    out_path = f"{outdir}/odds_apifootball.csv"
    if not all_rows:
        pd.DataFrame(columns=["match_id","home_team","away_team","fixture_id","league_id","bookmaker","market","home_price","draw_price","away_price"]).to_csv(out_path, index=False)
        print(f"[apifootball] OK -> {out_path} (0 linhas)")
        # não sai com erro: o consenso decide
        sys.exit(0)

    pd.DataFrame(all_rows).to_csv(out_path, index=False)
    print(f"[apifootball] OK -> {out_path} ({len(all_rows)} linhas)")

if __name__ == "__main__":
    main()
