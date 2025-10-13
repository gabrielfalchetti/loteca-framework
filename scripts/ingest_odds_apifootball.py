# -*- coding: utf-8 -*-
import argparse
import csv
import os
import sys
from typing import Dict, List

import pandas as pd
import requests

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")

def fetch_odds_by_fixture_id(fixture_id: int) -> Dict:
    base = "https://v3.football.api-sports.io/odds"
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    params = {"fixture": fixture_id, "bookmaker": 8}  # 8=Bet365 (ajuste se quiser)
    r = requests.get(base, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    js = r.json().get("response", [])
    # Extrair 1X2 se houver
    odds_home = odds_draw = odds_away = None
    for item in js:
        for b in item.get("bookmakers", []):
            for mkt in b.get("bets", []):
                if mkt.get("name","").lower() in ("match winner","1x2","3way result"):
                    vals = mkt.get("values", [])
                    for v in vals:
                        nm = v.get("value","").lower()
                        odd = v.get("odd", None)
                        if odd is None:
                            continue
                        try:
                            odd = float(odd)
                        except Exception:
                            continue
                        if nm in ("home","1","home team"):
                            odds_home = odd
                        elif nm in ("draw","x"):
                            odds_draw = odd
                        elif nm in ("away","2","away team"):
                            odds_away = odd
    return {"odds_home": odds_home, "odds_draw": odds_draw, "odds_away": odds_away}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório OUT da rodada")
    ap.add_argument("--source_csv", required=True, help="matches_norm.csv (com fixture_id/home/away)")
    args = ap.parse_args()

    if not API_FOOTBALL_KEY:
        print("[apifootball][WARN] API_FOOTBALL_KEY não configurada.", file=sys.stderr)
        sys.exit(0)

    if not os.path.exists(args.source_csv):
        print(f"[apifootball][CRITICAL] source_csv não encontrado: {args.source_csv}", file=sys.stderr)
        sys.exit(5)

    df = pd.read_csv(args.source_csv)
    out_rows: List[Dict] = []
    print(f"[apifootball]Iniciando busca direcionada para {len(df)} jogos do arquivo de origem.")

    for _, r in df.iterrows():
        match_id = r.get("match_id")
        team_home = r.get("home")
        team_away = r.get("away")
        fixture_id = r.get("fixture_id")
        if pd.isna(fixture_id) or str(fixture_id).strip() == "":
            print(f"[apifootball][WARN] Sem fixture_id para: {team_home} vs {team_away}")
            continue
        try:
            odds = fetch_odds_by_fixture_id(int(fixture_id))
        except Exception as e:
            print(f"[apifootball][WARN] Falha consultando fixture={fixture_id}: {e}")
            continue
        if any(v is None for v in odds.values()):
            # odds incompletas -> ignorar
            continue
        out_rows.append({
            "match_id": match_id,
            "team_home": team_home,
            "team_away": team_away,
            **odds
        })

    os.makedirs(args.rodada, exist_ok=True)
    out_path = os.path.join(args.rodada, "odds_apifootball.csv")
    pd.DataFrame(out_rows).to_csv(out_path, index=False)
    print(f"[apifootball]Arquivo odds_apifootball.csv gerado com {len(out_rows)} jogos encontrados.")

if __name__ == "__main__":
    sys.exit(main())