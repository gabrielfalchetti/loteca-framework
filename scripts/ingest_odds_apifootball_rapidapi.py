#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, argparse, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from rapidfuzz import fuzz
from unidecode import unidecode

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--window", type=int, default=28, help="dias p/ busca do fixture")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

def norm(s): return unidecode(str(s).strip().lower())

def load_aliases(path):
    if not os.path.exists(path):
        return {"teams":{}, "leagues":{}}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def canon_team(name, aliases):
    n = str(name).strip()
    # volta chave canônica se estiver nas aliases
    for k, arr in aliases.get("teams", {}).items():
        if n == k or n in arr:
            return k
    return n

def api_get(path, params, key):
    headers = { "x-rapidapi-host": API_HOST, "x-rapidapi-key": key }
    r = requests.get(API_BASE + path, headers=headers, params=params, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"RapidAPI {r.status_code}: {r.text}")
    return r.json()

def load_matches(rodada):
    fn = f"data/in/{rodada}/matches_source.csv"
    df = pd.read_csv(fn)
    for c in ["match_id","home_team","away_team","league_id_apifootball","season","kickoff_utc"]:
        if c not in df.columns:
            raise ValueError(f"Campo obrigatório ausente em {fn}: {c}")
    df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce")
    return df

def search_fixture_id(key, league_id, season, home, away, ko, window, debug=False):
    frm = (ko - timedelta(days=window)).strftime("%Y-%m-%d")
    to  = (ko + timedelta(days=window)).strftime("%Y-%m-%d")
    js = api_get("/fixtures", {"league": league_id, "season": season, "from": frm, "to": to}, key)
    best = (0, None)
    for it in js.get("response", []):
        th = it["teams"]["home"]["name"]; ta = it["teams"]["away"]["name"]
        sc = (fuzz.ratio(norm(home), norm(th)) + fuzz.ratio(norm(away), norm(ta)))/2.0
        if sc > best[0]:
            best = (sc, it["fixture"]["id"])
        if debug:
            print(f"[apif][cand] {th} vs {ta} -> {sc:.1f}")
    return best[1] if best[0] >= 80 else None

def fetch_odds_by_fixture(key, fixture_id):
    js = api_get("/odds", {"fixture": fixture_id}, key)
    rows = []
    for resp in js.get("response", []):
        for bk in resp.get("bookmakers", []):
            for bet in bk.get("bets", []):
                if bet.get("name","").lower() in ["match winner","winner","1x2","fulltime result","match result"]:
                    rec = {"bookmaker": bk.get("name",""), "market": bet.get("name",""),
                           "home_price": None, "draw_price": None, "away_price": None}
                    for v in bet.get("values", []):
                        nm = v.get("value","").strip().lower()
                        if nm in ["home","1","home team"]:
                            rec["home_price"] = v.get("odd")
                        elif nm in ["draw","x"]:
                            rec["draw_price"] = v.get("odd")
                        elif nm in ["away","2","away team"]:
                            rec["away_price"] = v.get("odd")
                    rows.append(rec)
    return rows

def fetch_odds_by_date(key, league_id, season, date_iso, home, away, debug=False):
    # fallback: odds por data + liga
    js = api_get("/odds", {"league": league_id, "season": season, "date": date_iso}, key)
    rows = []
    H, A = norm(home), norm(away)
    for resp in js.get("response", []):
        th = resp["teams"]["home"]["name"]; ta = resp["teams"]["away"]["name"]
        sc = (fuzz.ratio(H, norm(th)) + fuzz.ratio(A, norm(ta)))/2.0
        if sc < 80:
            continue
        for bk in resp.get("bookmakers", []):
            for bet in bk.get("bets", []):
                if bet.get("name","").lower() in ["match winner","winner","1x2","fulltime result","match result"]:
                    rec = {"bookmaker": bk.get("name",""), "market": bet.get("name",""),
                           "home_price": None, "draw_price": None, "away_price": None}
                    for v in bet.get("values", []):
                        nm = v.get("value","").strip().lower()
                        if nm in ["home","1","home team"]:
                            rec["home_price"] = v.get("odd")
                        elif nm in ["draw","x"]:
                            rec["draw_price"] = v.get("odd")
                        elif nm in ["away","2","away team"]:
                            rec["away_price"] = v.get("odd")
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
        league_id = int(m["league_id_apifootball"])
        season = int(m["season"])
        ko = m["kickoff_utc"]
        home = canon_team(m["home_team"], aliases)
        away = canon_team(m["away_team"], aliases)

        try:
            fixture_id = search_fixture_id(key, league_id, season, home, away, ko, args.window, args.debug)
            rows = []
            if fixture_id:
                if args.debug:
                    print(f"[apif] {mid} fixture={fixture_id}")
                rows = fetch_odds_by_fixture(key, fixture_id)

            if not rows:
                # Fallback por data no dia do jogo + dia anterior + seguinte (casas às vezes publicam com defasagem)
                for delta in (-1, 0, 1):
                    date_iso = (ko + timedelta(days=delta)).strftime("%Y-%m-%d")
                    tmp = fetch_odds_by_date(key, league_id, season, date_iso, home, away, args.debug)
                    rows.extend(tmp)
                    if tmp and args.debug:
                        print(f"[apif] {mid} fallback date={date_iso} -> {len(tmp)} linhas")
                    if rows:
                        break

            for r in rows:
                r.update({"match_id": mid, "home_team": home, "away_team": away, "league_id": league_id})
                all_rows.append(r)

        except Exception as e:
            print(f"[apifootball] AVISO {mid}: {e}", file=sys.stderr)

    out = f"{outdir}/odds_apifootball.csv"
    if not all_rows:
        pd.DataFrame(columns=["match_id","home_team","away_team","league_id","bookmaker","market","home_price","draw_price","away_price"]).to_csv(out, index=False)
        print(f"[apifootball] OK -> {out} (0 linhas)")
        sys.exit(0)

    # normaliza para float quando possível
    df = pd.DataFrame(all_rows)
    for c in ["home_price","draw_price","away_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.to_csv(out, index=False)
    print(f"[apifootball] OK -> {out} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
