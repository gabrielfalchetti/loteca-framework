# -*- coding: utf-8 -*-
"""
Ingest API-FOOTBALL via RapidAPI (SAFE)
- Usa variáveis:
  X_RAPIDAPI_KEY (obrigatória p/ usar), APIFOOT_LEAGUE_IDS (ex "71,72")
- Data é inferida de RODADA (AAAA-MM-DD_#### -> AAAA-MM-DD)
- Tenta coletar odds (endpoint /v3/odds) por liga/data; se indisponível, tenta fixtures e ignora.
- Casa com matches_source.csv via match_key.
- Saídas:
  data/out/<rodada>/odds_apifootball.csv
  data/out/<rodada>/unmatched_apifootball.csv
"""
from __future__ import annotations
import argparse, os, sys, json
from pathlib import Path
import requests
import pandas as pd
from scripts.text_normalizer import load_aliases, canonicalize_team, make_match_key

BASE = "https://api-football-v1.p.rapidapi.com/v3"

def parse_date_from_rodada(rodada: str) -> str:
    # "2025-09-27_1213" -> "2025-09-27"
    return rodada.split("_")[0]

def headers():
    key = os.getenv("X_RAPIDAPI_KEY")
    return {
        "x-rapidapi-key": key,
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }

def get_odds(date_str: str, league_id: int, season: int, debug=False):
    # /odds?date=YYYY-MM-DD&league=<id>&season=<year>&bookmaker=...
    url = f"{BASE}/odds"
    params = {"date": date_str, "league": league_id, "season": season}
    if debug: print(f"[apifootball] GET {url} params={params}")
    r = requests.get(url, headers=headers(), params=params, timeout=30)
    if r.status_code == 403:
        print(f"[apifootball] HTTP 403 em {url} params={params} body={r.json()}")
        r.raise_for_status()
    r.raise_for_status()
    return r.json().get("response", [])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if not os.getenv("X_RAPIDAPI_KEY"):
        print("[apifootball-safe] SKIP: X_RAPIDAPI_KEY ausente.")
        print("[apifootball-safe] linhas -> " + json.dumps({
            "odds_apifootball.csv": 0,
            "unmatched_apifootball.csv": 0
        }))
        return

    leagues_env = os.getenv("APIFOOT_LEAGUE_IDS", "71,72")
    leagues = [int(x.strip()) for x in leagues_env.split(",") if x.strip().isdigit()]
    date_str = parse_date_from_rodada(args.rodada)

    in_dir  = Path(f"data/in/{args.rodada}")
    out_dir = Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)

    src_path = in_dir/"matches_source.csv"
    if not src_path.exists():
        print(f"[apifootball-safe] ERRO: {src_path} não encontrado")
        sys.exit(2)
    src_df = pd.read_csv(src_path)
    for col in ("team_home","team_away","match_key"):
        if col not in src_df.columns:
            print(f"[apifootball-safe] ERRO: coluna ausente em matches_source.csv: {col}")
            sys.exit(2)

    aliases = load_aliases("data/aliases_br.json")

    rows = []
    for lid in leagues:
        try:
            data = get_odds(date_str, lid, args.season, debug=args.debug)
        except requests.HTTPError:
            # segue fluxo; odds podem não estar no seu plano Rapid
            continue

        for item in data:
            # estrutura: fixture (teams/home/name, teams/away/name), bookmakers -> bets
            fix = item.get("fixture", {})
            teams = item.get("teams", {})
            home = teams.get("home", {}).get("name","") or ""
            away = teams.get("away", {}).get("name","") or ""

            # buscar mercado "Match Winner" (1X2)
            oh = od = oa = None
            for bk in item.get("bookmakers", []):
                for bet in bk.get("bets", []):
                    if bet.get("name","").lower() in ("match winner","1x2","winner"):
                        for v in bet.get("values", []):
                            label = (v.get("value","") or "").lower()
                            odd = v.get("odd")
                            if label in ("home","1"): oh = oh or float(odd)
                            elif label in ("draw","x"): od = od or float(odd)
                            elif label in ("away","2"): oa = oa or float(odd)

            rows.append({"team_home": home, "team_away": away, "odds_home": oh, "odds_draw": od, "odds_away": oa})

    if not rows:
        # nada coletado / plano sem odds — gera arquivos vazios para não quebrar pipeline
        (out_dir/"odds_apifootball.csv").write_text("", encoding="utf-8")
        (out_dir/"unmatched_apifootball.csv").write_text("", encoding="utf-8")
        print("[apifootball-safe] linhas -> " + json.dumps({"odds_apifootball.csv": 0, "unmatched_apifootball.csv": 0}))
        return

    ev_df = pd.DataFrame(rows)
    ev_df["team_home_c"] = ev_df["team_home"].map(lambda x: canonicalize_team(str(x), aliases))
    ev_df["team_away_c"] = ev_df["team_away"].map(lambda x: canonicalize_team(str(x), aliases))
    ev_df["match_key"]   = [
        make_match_key(h, a, aliases=None)
        for h, a in zip(ev_df["team_home_c"], ev_df["team_away_c"])
    ]

    merged = ev_df.merge(
        src_df[["match_key","team_home","team_away"]],
        on="match_key",
        how="inner",
        suffixes=("_ev","")
    )

    def _ok(row):
        vals = [row["odds_home"], row["odds_draw"], row["odds_away"]]
        try:
            return sum(float(x) > 1.0 for x in vals if pd.notna(x)) >= 2
        except Exception:
            return False
    merged["__valid"] = merged.apply(_ok, axis=1)
    valid = merged[merged["__valid"]].copy()

    out_cols = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]
    valid[out_cols].to_csv(out_dir/"odds_apifootball.csv", index=False)
    matched = set(valid["match_key"])
    unmatched = src_df[~src_df["match_key"].isin(matched)].copy()
    unmatched.to_csv(out_dir/"unmatched_apifootball.csv", index=False)

    print("[apifootball-safe] linhas -> " + json.dumps({
        "odds_apifootball.csv": int(len(valid)),
        "unmatched_apifootball.csv": int(len(unmatched))
    }))

if __name__ == "__main__":
    main()