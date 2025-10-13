# -*- coding: utf-8 -*-
import argparse
import csv
import datetime as dt
import os
import sys
import unicodedata
from typing import Dict, List, Tuple, Optional

import requests
from rapidfuzz import fuzz, process
from unidecode import unidecode
import pandas as pd

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
THEODDS_API_KEY = os.getenv("THEODDS_API_KEY", "")

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = unidecode(s)  # remove acentos
    s = " ".join(s.split())  # colapsar espaços
    return s.lower()

def fetch_candidate_fixtures_football(lookahead_days: int) -> List[Dict]:
    """Busca fixtures futuros na API-Football (janela lookahead)."""
    if not API_FOOTBALL_KEY:
        return []
    base = "https://v3.football.api-sports.io"
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    # buscar próximos N dias via 'fixtures?date' por dia para robustez
    out = []
    today = dt.datetime.utcnow().date()
    for i in range(int(lookahead_days) + 1):
        d = today + dt.timedelta(days=i)
        url = f"{base}/fixtures"
        params = {"date": d.isoformat(), "timezone": "UTC"}
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            data = r.json().get("response", [])
            for fx in data:
                out.append({
                    "provider": "apifootball",
                    "fixture_id": fx["fixture"]["id"],
                    "commence_time": fx["fixture"]["date"],
                    "league": fx.get("league", {}).get("name", ""),
                    "home_name": fx["teams"]["home"]["name"],
                    "away_name": fx["teams"]["away"]["name"],
                    "home_id": fx["teams"]["home"]["id"],
                    "away_id": fx["teams"]["away"]["id"],
                })
        except Exception:
            continue
    return out

def fetch_candidate_scores_theodds(lookahead_days: int) -> List[Dict]:
    """Busca jogos (scores) na TheOddsAPI dentro da janela."""
    if not THEODDS_API_KEY:
        return []
    # Soccer "all" endpoint de scores: próximos dias
    # Nota: alguns planos não listam todos os campeonatos.
    base = "https://api.the-odds-api.com/v4/sports/soccer/odds"
    # Como alternativa, poderíamos consultar /v4/sports para listar ligas e chamar cada uma.
    # Para simplicidade, usa odds de hoje+N (commence_time incluso).
    out = []
    today = dt.datetime.utcnow()
    end = today + dt.timedelta(days=int(lookahead_days))
    # Como TheOddsAPI organiza por liga, este sample usa a agregação de odds geral;
    # se sua conta permitir listar ligas, refinar aqui.
    params = {
        "apiKey": THEODDS_API_KEY,
        "regions": "eu,uk,us,au",
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
      }
    try:
        r = requests.get(base, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for row in data:
            ct = row.get("commence_time", "")
            try:
                when = dt.datetime.fromisoformat(ct.replace("Z","+00:00"))
            except Exception:
                continue
            if not (today <= when <= end):
                continue
            home = row.get("home_team","")
            away = row.get("away_team","")
            out.append({
                "provider": "theodds",
                "fixture_id": row.get("id", ""),  # TheOdds fixture key (não compatível com APIFootball)
                "commence_time": ct,
                "league": row.get("sport_title",""),
                "home_name": home,
                "away_name": away,
                "home_id": "",  # TheOdds não fornece team_id universal
                "away_id": "",
            })
    except Exception:
        pass
    return out

def build_catalog(lookahead_days: int) -> pd.DataFrame:
    rows = []
    rows += fetch_candidate_fixtures_football(lookahead_days)
    rows += fetch_candidate_scores_theodds(lookahead_days)
    if not rows:
        return pd.DataFrame(columns=["provider","fixture_id","commence_time","league","home_name","away_name","home_id","away_id"])
    df = pd.DataFrame(rows).drop_duplicates()
    # campos normalizados p/ fuzzy
    df["home_norm"] = df["home_name"].map(_norm)
    df["away_norm"] = df["away_name"].map(_norm)
    return df

def fuzzy_resolve_one(home: str, away: str, catalog: pd.DataFrame) -> Optional[Dict]:
    """Resolve um par (home,away) contra o catálogo, tentando casa/fora e swap."""
    if catalog.empty:
        return None
    h = _norm(home)
    a = _norm(away)

    # Ranking por soma de pontuações (home vs home_norm) + (away vs away_norm)
    def score_row(row) -> float:
        s1 = fuzz.WRatio(h, row["home_norm"])
        s2 = fuzz.WRatio(a, row["away_norm"])
        return (s1 + s2) / 2.0

    best_idx = None
    best_score = -1
    for idx, row in catalog.iterrows():
        s = score_row(row)
        if s > best_score:
            best_score = s
            best_idx = idx

    # também tentar swap (se por acaso o mandante veio invertido)
    def score_row_swapped(row) -> float:
        s1 = fuzz.WRatio(h, row["away_norm"])
        s2 = fuzz.WRatio(a, row["home_norm"])
        return (s1 + s2) / 2.0

    best_idx_sw = None
    best_score_sw = -1
    for idx, row in catalog.iterrows():
        s = score_row_swapped(row)
        if s > best_score_sw:
            best_score_sw = s
            best_idx_sw = idx

    # escolher o melhor entre normal e swap, com limiar
    cand = None
    swapped = False
    limiar = 85  # ajuste conforme necessidade
    if best_score >= best_score_sw and best_score >= limiar:
        cand = catalog.loc[best_idx].to_dict()
        swapped = False
    elif best_score_sw >= limiar:
        cand = catalog.loc[best_idx_sw].to_dict()
        swapped = True

    if not cand:
        return None

    return {
        "provider": cand["provider"],
        "fixture_id": cand.get("fixture_id",""),
        "home_id": cand.get("home_id",""),
        "away_id": cand.get("away_id",""),
        "home_resolved": cand["away_name"] if swapped else cand["home_name"],
        "away_resolved": cand["home_name"] if swapped else cand["away_name"],
        "commence_time": cand.get("commence_time",""),
        "league": cand.get("league",""),
        "match_quality": best_score_sw if swapped else best_score,
        "swapped": swapped,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source_csv", required=True, help="CSV com match_id,home,away (PT/EN)")
    ap.add_argument("--out", required=True, help="arquivo de saída (matches_norm.csv)")
    ap.add_argument("--lookahead_days", type=int, default=3)
    args = ap.parse_args()

    if not os.path.exists(args.source_csv):
        print(f"[resolve_matches][CRITICAL] source_csv não encontrado: {args.source_csv}", file=sys.stderr)
        sys.exit(4)

    src = pd.read_csv(args.source_csv)
    for col in ("match_id","home","away"):
        if col not in src.columns:
            print(f"[resolve_matches][CRITICAL] coluna ausente em {args.source_csv}: {col}", file=sys.stderr)
            sys.exit(4)

    catalog = build_catalog(args.lookahead_days)
    if catalog.empty:
        print("[resolve_matches][WARN] catálogo vazio (APIs não retornaram jogos). Saída manterá nomes originais.")
        out = src.copy()
        out["provider"] = ""
        out["fixture_id"] = ""
        out["home_id"] = ""
        out["away_id"] = ""
        out["commence_time"] = ""
        out["league"] = ""
        out.to_csv(args.out, index=False)
        return

    rows = []
    dbg = []
    for _, r in src.iterrows():
        match_id = r["match_id"]
        home = str(r["home"])
        away = str(r["away"])
        res = fuzzy_resolve_one(home, away, catalog)
        if res:
            rows.append({
                "match_id": match_id,
                "home": res["home_resolved"],
                "away": res["away_resolved"],
                "provider": res["provider"],
                "fixture_id": res["fixture_id"],
                "home_id": res["home_id"],
                "away_id": res["away_id"],
                "commence_time": res["commence_time"],
                "league": res["league"],
            })
            dbg.append({
                "match_id": match_id, "home_in": home, "away_in": away,
                **res
            })
        else:
            rows.append({
                "match_id": match_id,
                "home": home,
                "away": away,
                "provider": "",
                "fixture_id": "",
                "home_id": "",
                "away_id": "",
                "commence_time": "",
                "league": "",
            })
            dbg.append({
                "match_id": match_id, "home_in": home, "away_in": away,
                "provider": "", "fixture_id": "", "home_id":"", "away_id":"",
                "home_resolved": home, "away_resolved": away,
                "commence_time":"", "league":"", "match_quality": 0, "swapped": False
            })

    out_df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out_df.to_csv(args.out, index=False)

    dbg_df = pd.DataFrame(dbg)
    dbg_path = os.path.join(os.path.dirname(args.out), "matches_resolved_debug.csv")
    dbg_df.to_csv(dbg_path, index=False)

    print(f"[resolve_matches] OK — gravado {len(out_df)} em {args.out}")

if __name__ == "__main__":
    sys.exit(main())