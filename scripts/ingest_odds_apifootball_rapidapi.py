#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, argparse, time
import pandas as pd
import requests

def log(m): print(f"[apifootball] {m}", flush=True)
def err(m): print(f"[apifootball] ERRO: {m}", file=sys.stderr, flush=True)

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"
API_KEY  = os.getenv("RAPIDAPI_KEY", "").strip()

def read_matches(in_path):
    if not os.path.exists(in_path):
        err(f"Arquivo não encontrado: {in_path}")
        return pd.DataFrame(columns=["match_id","home_team","away_team","league","kickoff_utc","fixture_id"])
    df = pd.read_csv(in_path)
    for c in ["match_id","home_team","away_team","league","kickoff_utc","fixture_id"]:
        if c not in df.columns: df[c] = None
    df["match_id"] = df["match_id"].astype(str)
    return df

def fetch_fixture_odds(fixture_id):
    url = f"{API_BASE}/odds"
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": API_HOST
    }
    params = {"fixture": str(fixture_id)}
    r = requests.get(url, headers=headers, params=params, timeout=20)
    status = r.status_code
    try:
        js = r.json()
    except Exception:
        js = None
    return status, js, r.text

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if not API_KEY:
        err("RAPIDAPI_KEY não definido.")
        sys.exit(2)

    in_path  = f"data/in/{args.rodada}/matches_source.csv"
    out_path = f"data/out/{args.rodada}/odds_apifootball.csv"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    dfm = read_matches(in_path)
    log(f"Jogos no CSV: {len(dfm)}")

    rows = []
    misses = 0
    for _, row in dfm.iterrows():
        fix = row.get("fixture_id")
        if not fix or str(fix) == "nan":
            # sem fixture_id — você pode preencher manualmente se quiser odds por fixture exato
            continue
        status, js, raw = fetch_fixture_odds(fix)
        if status != 200 or js is None:
            err(f"Falha odds fixture={fix} (HTTP {status}). Resumo: {raw[:200]}")
            continue
        resp = js.get("response") if isinstance(js, dict) else None
        if not resp:
            log(f"sem odds p/ fixture={fix} '{row['home_team']}' vs '{row['away_team']}'")
            misses += 1
            continue

        # percorre bookmakers -> bets -> values (1, X, 2)
        for e in resp:
            for bk in e.get("bookmakers", []):
                bname = bk.get("name")
                for bet in bk.get("bets", []):
                    if str(bet.get("name","")).lower() not in ("match winner","1x2","win-draw-win"):
                        continue
                    price_home = price_draw = price_away = None
                    for v in bet.get("values", []):
                        val = (v.get("value") or "").strip().upper()
                        odd = v.get("odd")
                        if val in ("1","HOME"):
                            price_home = odd
                        elif val in ("X","DRAW"):
                            price_draw = odd
                        elif val in ("2","AWAY"):
                            price_away = odd
                    if all([price_home, price_draw, price_away]):
                        rows.append({
                            "match_id": str(row["match_id"]),
                            "home_odds": price_home,
                            "draw_odds": price_draw,
                            "away_odds": price_away,
                            "bookmaker": bname,
                            "provider": "rapidapi"
                        })
        time.sleep(0.15)

    if not rows:
        log("Nenhuma odd encontrada no API-Football para os fixtures informados.")
        pd.DataFrame(columns=["match_id","home_odds","draw_odds","away_odds","bookmaker","provider"]).to_csv(out_path, index=False)
        sys.exit(0)

    pd.DataFrame(rows).to_csv(out_path, index=False)
    log(f"OK -> {out_path} ({len(rows)} linhas)")

if __name__ == "__main__":
    main()
