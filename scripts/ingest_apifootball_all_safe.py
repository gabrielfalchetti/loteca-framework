#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ingestão 'tudo-em-um' do API-Football: fixtures, odds, lineups, injuries, h2h, standings, stats.
- Casa com 'data/in/matches_source.csv' (home,away[,league_id,date])
- Escreve CSVs em OUT_DIR (obrigatório) e falha se faltar dado essencial.

Saídas:
  - apifoot_fixtures.csv
  - apifoot_odds.csv
  - apifoot_lineups.csv
  - apifoot_injuries.csv
  - apifoot_h2h.csv
  - apifoot_standings.csv
  - apifoot_teamstats.csv
  - apifoot_fixturestats.csv
"""

import os
import sys
import csv
import json
import argparse
import pandas as pd
from typing import List, Dict, Any
from scripts.apifoot_client import (
    fixtures_by_date, odds_by_fixture, lineups_by_fixture, injuries_by_date_league,
    h2h, standings, teams_stats, fixture_stats, sleep_rl
)

REQ_COLUMNS = ["home","away"]  # opcional: league_id,date
MATCHES_DEFAULT = "data/in/matches_source.csv"

def die(msg: str):
    print(f"[apifoot] ERRO: {msg}", file=sys.stderr)
    sys.exit(2)

def read_matches(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        die(f"{path} não encontrado")
    df = pd.read_csv(path)
    for c in REQ_COLUMNS:
        if c not in df.columns:
            die(f"coluna obrigatória ausente em {path}: {c}")
    # normaliza strings
    for c in ["home","away"]:
        df[c] = df[c].astype(str).str.strip().str.lower()
    return df

def ensure_outdir(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

def write_csv(path: str, rows: List[Dict[str, Any]]):
    cols = sorted({k for row in rows for k in row.keys()}) if rows else []
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def best_fixture_match(fixtures: List[Dict[str,Any]], home: str, away: str) -> Dict[str,Any]:
    # match “best effort” por nome (semelhante ao que já fazemos)
    def norm(s: str) -> str:
        return str(s or "").strip().lower()
    cand = []
    for fx in fixtures:
        th = norm(fx["teams"]["home"]["name"])
        ta = norm(fx["teams"]["away"]["name"])
        score = 0
        if home in th: score += 1
        if away in ta: score += 1
        if th == home: score += 2
        if ta == away: score += 2
        cand.append((score, fx))
    cand.sort(key=lambda x: x[0], reverse=True)
    return cand[0][1] if cand and cand[0][0] > 0 else {}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", required=True, help="Diretório de saída (ex: data/out/123456)")
    p.add_argument("--matches", default=MATCHES_DEFAULT, help="CSV de entrada com home,away[,league_id,date]")
    p.add_argument("--season", default=os.getenv("SEASON","").strip())
    p.add_argument("--leagues", default=os.getenv("APIFOOT_LEAGUE_IDS","").strip(), help="ex: 71,72")
    p.add_argument("--date", help="YYYY-MM-DD (opcional se o matches tiver date)")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    if not args.season:
        die("SEASON não definida")
    leagues = [int(x) for x in args.leagues.split(",") if x.strip().isdigit()]
    if not leagues:
        die("APIFOOT_LEAGUE_IDS vazio — informe pelo menos 1 liga")
    ensure_outdir(args.out_dir)

    dfm = read_matches(args.matches)

    fixtures_rows, odds_rows, lineups_rows = [], [], []
    inj_rows, h2h_rows, stand_rows, tstats_rows, fstats_rows = [], [], [], [], []

    # para standings por liga (evita repetir)
    seen_stand = set()
    # caching por (league, team_id) em teams_stats
    seen_tstats = set()

    # Fazemos loop por liga → data(s) inferidas
    # Se o CSV tiver coluna date, usamos por linha; se não, opcionalmente --date; se nada: falha
    has_date_col = "date" in dfm.columns
    if not has_date_col and not args.date:
        die("Data dos jogos não informada (adicione coluna 'date' no matches_source.csv ou use --date YYYY-MM-DD)")

    for league_id in leagues:
        # standings uma vez por liga
        if league_id not in seen_stand:
            st = standings(league_id, int(args.season), debug=args.debug)
            table = st.get("response", [])
            for block in table:
                league = block.get("league", {})
                for group in block.get("league", {}).get("standings", []):
                    for row in group:
                        stand_rows.append({
                            "league_id": league.get("id"),
                            "team_id": row["team"]["id"],
                            "team": row["team"]["name"],
                            "rank": row.get("rank"),
                            "points": row.get("points"),
                            "form": row.get("form"),
                            "goals_diff": row.get("goalsDiff"),
                            "played": row.get("all",{}).get("played")
                        })
            seen_stand.add(league_id)

        # Loop nos matches dessa liga
        for _, r in dfm.iterrows():
            home = r["home"]; away = r["away"]
            date_str = r["date"] if has_date_col else args.date
            # busca fixtures do dia naquela liga
            fx_all = fixtures_by_date(date_str, league_id, int(args.season), debug=args.debug).get("response", [])
            sleep_rl()
            if not fx_all:
                continue
            fx = best_fixture_match(fx_all, home, away)
            if not fx:
                continue

            fixture_id = fx["fixture"]["id"]
            team_home_id = fx["teams"]["home"]["id"]
            team_away_id = fx["teams"]["away"]["id"]
            ref = (fx["fixture"].get("referee") or "").strip()

            fixtures_rows.append({
                "fixture_id": fixture_id,
                "league_id": league_id,
                "date": date_str,
                "home": fx["teams"]["home"]["name"],
                "away": fx["teams"]["away"]["name"],
                "referee": ref,
                "venue": fx["fixture"]["venue"]["name"],
                "city": fx["fixture"]["venue"]["city"]
            })

            # odds 1X2
            od = odds_by_fixture(fixture_id, debug=args.debug).get("response", [])
            sleep_rl()
            for book in od:
                # escolhemos o mercado 'Match Winner' (1X2); estrutura varia por book
                for b in book.get("bookmakers", []):
                    for m in b.get("bets", []):
                        if m.get("name","").lower() in ("match winner","1x2","match_winner"):
                            kv = {"fixture_id": fixture_id, "bookmaker": b.get("name")}
                            for v in m.get("values", []):
                                lbl = v.get("value","").upper()
                                odd = v.get("odd")
                                if lbl in ("HOME","1"):
                                    kv["odds_home"] = float(odd)
                                elif lbl in ("DRAW","X"):
                                    kv["odds_draw"] = float(odd)
                                elif lbl in ("AWAY","2"):
                                    kv["odds_away"] = float(odd)
                            if {"odds_home","odds_draw","odds_away"} <= kv.keys():
                                odds_rows.append(kv)

            # lineups
            lu = lineups_by_fixture(fixture_id, debug=args.debug).get("response", [])
            sleep_rl()
            for side in lu:
                lineups_rows.append({
                    "fixture_id": fixture_id,
                    "team": side["team"]["name"],
                    "team_id": side["team"]["id"],
                    "coach": side.get("coach",{}).get("name"),
                    "formation": side.get("formation"),
                    "confirmed": side.get("team",{}).get("update") is not None
                })

            # injuries (por data/league)
            inj = injuries_by_date_league(date_str, league_id, int(args.season), debug=args.debug).get("response", [])
            sleep_rl()
            for it in inj:
                inj_rows.append({
                    "league_id": league_id,
                    "date": date_str,
                    "team_id": it["team"]["id"],
                    "team": it["team"]["name"],
                    "player_id": it["player"]["id"],
                    "player": it["player"]["name"],
                    "type": it["player"].get("type"),
                    "reason": it["player"].get("reason")
                })

            # h2h
            hh = h2h(team_home_id, team_away_id, debug=args.debug).get("response", [])
            sleep_rl()
            for g in hh[:10]:
                h2h_rows.append({
                    "home": g["teams"]["home"]["name"],
                    "away": g["teams"]["away"]["name"],
                    "home_winner": g["teams"]["home"]["winner"],
                    "away_winner": g["teams"]["away"]["winner"],
                    "goals_home": g["goals"]["home"],
                    "goals_away": g["goals"]["away"],
                    "date": g["fixture"]["date"][:10]
                })

            # team stats (forma etc.) para cada time, cacheando
            for tid in (team_home_id, team_away_id):
                key = (league_id, tid)
                if key not in seen_tstats:
                    ts = teams_stats(league_id, int(args.season), tid, debug=args.debug).get("response", {})
                    sleep_rl()
                    if ts:
                        tstats_rows.append({
                            "league_id": league_id,
                            "team_id": tid,
                            "team": ts.get("team",{}).get("name"),
                            "form": ts.get("form"),
                            "played": ts.get("fixtures",{}).get("played",{}).get("total"),
                            "wins": ts.get("fixtures",{}).get("wins",{}).get("total"),
                            "draws": ts.get("fixtures",{}).get("draws",{}).get("total"),
                            "losses": ts.get("fixtures",{}).get("losses",{}).get("total"),
                            "goals_for_avg": ts.get("goals",{}).get("for",{}).get("average",{}).get("total"),
                            "goals_against_avg": ts.get("goals",{}).get("against",{}).get("average",{}).get("total"),
                        })
                    seen_tstats.add(key)

            # fixture stats (se o jogo já ocorreu ou tem prévia)
            fs = fixture_stats(fixture_id, debug=args.debug).get("response", [])
            sleep_rl()
            for side in fs:
                stats = {s["type"]: s["value"] for s in side.get("statistics", [])}
                fstats_rows.append({
                    "fixture_id": fixture_id,
                    "team": side["team"]["name"],
                    "shots_on_target": stats.get("Shots on Target"),
                    "shots_total": stats.get("Total Shots"),
                    "possession": stats.get("Ball Possession"),
                    "passes_accuracy": stats.get("Passes %"),
                    "fouls": stats.get("Fouls"),
                    "corners": stats.get("Corner Kicks"),
                    "offsides": stats.get("Offsides"),
                    "yellow": stats.get("Yellow Cards"),
                    "red": stats.get("Red Cards")
                })

    # Escrita dos CSVs
    if not fixtures_rows:
        die("Nenhuma fixture casada com matches_source.csv (confira nomes, liga e data).")
    write_csv(os.path.join(args.out_dir, "apifoot_fixtures.csv"), fixtures_rows)

    if not odds_rows:
        die("Odds do API-Football vazias para as fixtures encontradas.")
    write_csv(os.path.join(args.out_dir, "apifoot_odds.csv"), odds_rows)

    write_csv(os.path.join(args.out_dir, "apifoot_lineups.csv"), lineups_rows)
    write_csv(os.path.join(args.out_dir, "apifoot_injuries.csv"), inj_rows)
    write_csv(os.path.join(args.out_dir, "apifoot_h2h.csv"), h2h_rows)
    write_csv(os.path.join(args.out_dir, "apifoot_standings.csv"), stand_rows)
    write_csv(os.path.join(args.out_dir, "apifoot_teamstats.csv"), tstats_rows)
    write_csv(os.path.join(args.out_dir, "apifoot_fixturestats.csv"), fstats_rows)

    print("[apifoot] OK ->", args.out_dir)

if __name__ == "__main__":
    main()