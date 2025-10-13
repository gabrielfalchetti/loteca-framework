# -*- coding: utf-8 -*-
import argparse
import os
import sys
from typing import Dict, List

import pandas as pd
import requests
from unidecode import unidecode

THEODDS_API_KEY = os.getenv("THEODDS_API_KEY", "")

def _norm(s: str) -> str:
    return " ".join(unidecode(str(s or "")).lower().split())

def fetch_all_odds(regions: str) -> List[Dict]:
    base = "https://api.the-odds-api.com/v4/sports/soccer/odds"
    params = {
        "apiKey": THEODDS_API_KEY,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    rows = []
    for g in data:
        home = g.get("home_team","")
        away = g.get("away_team","")
        ct = g.get("commence_time","")
        for bk in g.get("bookmakers", []):
            for m in bk.get("markets", []):
                if m.get("key") != "h2h":
                    continue
                prices = m.get("outcomes", [])
                odds_home = odds_draw = odds_away = None
                for p in prices:
                    nm = p.get("name","").lower()
                    price = p.get("price", None)
                    if price is None:
                        continue
                    try:
                        price = float(price)
                    except Exception:
                        continue
                    if nm in ("home","home team", _norm(home)):
                        odds_home = price
                    elif nm in ("draw","empate"):
                        odds_draw = price
                    elif nm in ("away","away team", _norm(away)):
                        odds_away = price
                rows.append({
                    "home": home, "away": away, "commence_time": ct,
                    "odds_home": odds_home, "odds_draw": odds_draw, "odds_away": odds_away
                })
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório OUT da rodada")
    ap.add_argument("--regions", default="eu,uk,us,au")
    ap.add_argument("--source_csv", required=True, help="matches_norm.csv (com nomes já normalizados)")
    args = ap.parse_args()

    if not THEODDS_API_KEY:
        print("[theoddsapi][WARN] THEODDS_API_KEY não configurada.", file=sys.stderr)
        sys.exit(0)

    if not os.path.exists(args.source_csv):
        print(f"[theoddsapi][CRITICAL] source_csv não encontrado: {args.source_csv}", file=sys.stderr)
        sys.exit(5)

    src = pd.read_csv(args.source_csv)
    src["home_norm"] = src["home"].map(_norm)
    src["away_norm"] = src["away"].map(_norm)

    rows = fetch_all_odds(args.regions)
    df = pd.DataFrame(rows)
    if df.empty:
        print("[theoddsapi][WARN] Nenhuma odd retornada.")
        out_path = os.path.join(args.rodada, "odds_theoddsapi.csv")
        pd.DataFrame([], columns=["match_id","team_home","team_away","odds_home","odds_draw","odds_away"]).to_csv(out_path, index=False)
        return

    df["home_norm"] = df["home"].map(_norm)
    df["away_norm"] = df["away"].map(_norm)

    # join por nomes normalizados (já canônicos) + (opcional) commence_time quando existir nos dois lados
    merged = src.merge(df, how="left", left_on=["home_norm","away_norm"], right_on=["home_norm","away_norm"])

    # manter somente linhas com odds completas
    merged = merged.dropna(subset=["odds_home","odds_draw","odds_away"], how="any")

    out = merged[["match_id","home","away","odds_home","odds_draw","odds_away"]].rename(
        columns={"home":"team_home","away":"team_away"}
    )

    os.makedirs(args.rodada, exist_ok=True)
    out_path = os.path.join(args.rodada, "odds_theoddsapi.csv")
    out.to_csv(out_path, index=False)
    print(f"[theoddsapi]Arquivo odds_theoddsapi.csv gerado com {len(out)} jogos da sua lista.")

if __name__ == "__main__":
    sys.exit(main())