#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, argparse, time
import pandas as pd
import requests

def log(m): print(f"[theoddsapi] {m}", flush=True)
def err(m): print(f"[theoddsapi] ERRO: {m}", file=sys.stderr, flush=True)

API_URL = "https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
API_KEY = os.getenv("THEODDS_API_KEY", "").strip()

def load_league_map(path="data/league_map.json"):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def read_matches(in_path):
    if not os.path.exists(in_path):
        err(f"Arquivo não encontrado: {in_path}")
        return pd.DataFrame(columns=["match_id","home_team","away_team","league","kickoff_utc"])
    df = pd.read_csv(in_path)
    for c in ["match_id","home_team","away_team","league","kickoff_utc"]:
        if c not in df.columns: df[c] = None
    df["match_id"] = df["match_id"].astype(str)
    return df

def fetch_odds_for_league(sport_key, regions, markets="h2h", odds_format="decimal"):
    params = {
        "apiKey": API_KEY,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format
    }
    url = API_URL.format(sport_key=sport_key)
    r = requests.get(url, params=params, timeout=20)
    status = r.status_code
    try:
        data = r.json()
    except Exception:
        data = None
    return status, data, r.text

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if not API_KEY:
        err("THEODDS_API_KEY não definido.")
        sys.exit(2)

    in_path  = f"data/in/{args.rodada}/matches_source.csv"
    out_path = f"data/out/{args.rodada}/odds_theoddsapi.csv"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    log(f"Entrada: {in_path}")
    log(f"Saída:   {out_path}")

    league_map = load_league_map()
    dfm = read_matches(in_path)
    log(f"Partidas lidas: {len(dfm)}")

    # ligas presentes
    leagues = sorted([str(x) for x in dfm["league"].dropna().unique()])
    if not leagues:
        err("Nenhuma 'league' informada nos jogos. Preencha a coluna 'league' no CSV.")
        pd.DataFrame(columns=["match_id","home_odds","draw_odds","away_odds","bookmaker","provider"]).to_csv(out_path, index=False)
        sys.exit(0)

    # consulta por liga (sport_key)
    rows = []
    for lg in leagues:
        skey = league_map.get(lg)
        if not skey:
            err(f"Sem sport_key para liga='{lg}'. Ajuste data/league_map.json.")
            continue

        status, data, raw = fetch_odds_for_league(skey, regions=args.regions)
        if status != 200 or data is None:
            err(f"Falha ao consultar {skey} (HTTP {status}). Resp curta: {raw[:200]}")
            continue

        # data = lista de jogos com bookmakers
        # tentamos casar por (home_team, away_team) normalizados
        subset = dfm[dfm["league"] == lg].copy()
        for item in data:
            home = (item.get("home_team") or "").strip()
            away = (item.get("away_team") or "").strip()
            bks  = item.get("bookmakers") or []
            # casa simples: mesmo nome
            m = subset[(subset["home_team"].str.strip()==home) & (subset["away_team"].str.strip()==away)]
            if m.empty:
                continue
            match_id = m["match_id"].iloc[0]
            for bk in bks:
                bname = bk.get("title")
                for mk in (bk.get("markets") or []):
                    if (mk.get("key") or "") != "h2h":  # 1X2
                        continue
                    outcomes = mk.get("outcomes") or []
                    # outcomes podem vir como [home, away, draw] em ordens diferentes
                    price_home = price_draw = price_away = None
                    for oc in outcomes:
                        name = (oc.get("name") or "").lower()
                        price = oc.get("price")
                        if name in ("home","home team", home.lower()):
                            price_home = price
                        elif name in ("draw","empate","x"):
                            price_draw = price
                        elif name in ("away","away team", away.lower()):
                            price_away = price
                    if all([price_home, price_draw, price_away]):
                        rows.append({
                            "match_id": str(match_id),
                            "home_odds": price_home,
                            "draw_odds": price_draw,
                            "away_odds": price_away,
                            "bookmaker": bname,
                            "provider": "theoddsapi"
                        })
        time.sleep(0.2)  # gentileza com a API

    if not rows:
        log("Nenhuma odd encontrada na TheOddsAPI para as ligas/jogos informados.")
        pd.DataFrame(columns=["match_id","home_odds","draw_odds","away_odds","bookmaker","provider"]).to_csv(out_path, index=False)
        sys.exit(0)

    pd.DataFrame(rows).to_csv(out_path, index=False)
    log(f"OK -> {out_path} ({len(rows)} linhas)")

if __name__ == "__main__":
    main()
