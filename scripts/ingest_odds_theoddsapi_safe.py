#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor seguro da TheOddsAPI com suporte a --sports.
- Lê data/in/matches_source.csv (colunas obrigatórias: home,away).
- Busca odds h2h nos esportes passados via --sports (se omitido, usa um default).
- Faz matching (normalizado + fuzzy) entre jogos do arquivo e eventos da API.
- Salva:
  <out_dir>/odds_theoddsapi.csv
  <out_dir>/unmatched_theoddsapi.csv
Compatível com o pipeline de consenso que lê data/out/<RODADA_ID>/...
"""

import argparse
import os
import sys
import json
from typing import List, Tuple

import pandas as pd
import requests
from rapidfuzz import fuzz, process
from unidecode import unidecode

# -------- utilidades simples -------- #

def norm(s: str) -> str:
    return " ".join(unidecode(str(s)).lower().strip().split())

def make_key(home: str, away: str) -> str:
    return f"{norm(home)}__vs__{norm(away)}"

def must_cols(df: pd.DataFrame, cols: List[str], path: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"[theoddsapi-safe] ERRO: coluna(s) ausente(s) em {path}: {', '.join(missing)}", file=sys.stderr)
        sys.exit(2)

# -------- coleta TheOddsAPI -------- #

def fetch_odds_for_sport(api_key: str, sport_key: str, regions: str, debug: bool=False) -> list:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {"apiKey": api_key, "regions": regions, "markets": "h2h"}
    if debug:
        print(f"[theoddsapi-safe][DEBUG] GET {url} {params}")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return []

def flatten_events(json_list: list) -> pd.DataFrame:
    rows = []
    for ev in json_list:
        try:
            home = ev["home_team"]
            away = ev["away_team"]
            bookmakers = ev.get("bookmakers") or []
            # procura o primeiro book com odds h2h
            o_home = o_draw = o_away = None
            for bk in bookmakers:
                mkts = bk.get("markets") or []
                for mk in mkts:
                    if mk.get("key") == "h2h":
                        outcomes = mk.get("outcomes") or []
                        # outcomes podem vir em qualquer ordem
                        for oc in outcomes:
                            name = oc.get("name")
                            price = oc.get("price")
                            if name == home:
                                o_home = price
                            elif name == away:
                                o_away = price
                            elif name and name.lower() in ("draw", "empate"):
                                o_draw = price
                        break
                if o_home or o_away or o_draw:
                    break
            rows.append({
                "team_home": home,
                "team_away": away,
                "odds_home": o_home,
                "odds_draw": o_draw,
                "odds_away": o_away
            })
        except Exception:
            continue
    return pd.DataFrame(rows)

def fuzzy_pair_match(src_home: str, src_away: str, cand_df: pd.DataFrame, thresh: int=90) -> Tuple[int, pd.Series]:
    """
    Retorna (score_medio, linha_candidata) ou (0, None)
    """
    if cand_df.empty:
        return 0, None
    h = norm(src_home)
    a = norm(src_away)

    best_idx = None
    best_score = 0
    for idx, row in cand_df.iterrows():
        ch = norm(row["team_home"])
        ca = norm(row["team_away"])
        # checa nas duas ordens (só por segurança)
        s1 = (fuzz.token_sort_ratio(h, ch) + fuzz.token_sort_ratio(a, ca)) // 2
        s2 = (fuzz.token_sort_ratio(h, ca) + fuzz.token_sort_ratio(a, ch)) // 2
        s = max(s1, s2)
        if s > best_score:
            best_score = s
            best_idx = idx
    if best_idx is None or best_score < thresh:
        return 0, None
    return best_score, cand_df.loc[best_idx]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório de saída (OUT_DIR) ou ID (o workflow sincroniza depois).")
    parser.add_argument("--regions", default="uk,eu,us,au")
    parser.add_argument("--sports", default="", help="Lista de esportes TheOddsAPI separados por vírgula. Ex: soccer_argentina_primera_division,soccer_argentina_primera_nacional")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    api_key = os.environ.get("THEODDS_API_KEY", "")
    if not api_key:
        print("[theoddsapi-safe] ERRO: THEODDS_API_KEY não definido.", file=sys.stderr)
        sys.exit(2)

    src_path = "data/in/matches_source.csv"
    if not os.path.isfile(src_path):
        print(f"[theoddsapi-safe] ERRO: {src_path} não encontrado", file=sys.stderr)
        sys.exit(2)

    src_df = pd.read_csv(src_path)
    must_cols(src_df, ["home", "away"], src_path)
    src_df["match_key"] = src_df.apply(lambda r: make_key(r["home"], r["away"]), axis=1)

    # Esportes
    sports = [s.strip() for s in (args.sports or "").split(",") if s.strip()]
    if not sports:
        # default seguro (BR + AR). Ajuste conforme seu uso
        sports = [
            "soccer_brazil_campeonato",
            "soccer_brazil_serie_b",
            "soccer_argentina_primera_division",
            "soccer_argentina_primera_nacional",
        ]

    # Coleta
    all_events = []
    for sp in sports:
        try:
            data = fetch_odds_for_sport(api_key, sp, args.regions, debug=args.debug)
            df = flatten_events(data)
            if not df.empty:
                all_events.append(df)
        except Exception as e:
            if args.debug:
                print(f"[theoddsapi-safe][DEBUG] Falha em {sp}: {e}")

    if not all_events:
        # ainda assim escrevemos arquivos vazios para o pipeline se comportar bem
        pd.DataFrame(columns=["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]).to_csv(
            os.path.join(out_dir, "odds_theoddsapi.csv"), index=False
        )
        pd.DataFrame(columns=["match_key","home","away"]).to_csv(
            os.path.join(out_dir, "unmatched_theoddsapi.csv"), index=False
        )
        print("[theoddsapi-safe] AVISO: nenhum evento retornado pelos esportes solicitados.")
        sys.exit(0)

    events_df = pd.concat(all_events, ignore_index=True).drop_duplicates()

    # Matching
    matched_rows = []
    unmatched_rows = []

    for _, r in src_df.iterrows():
        score, ev = fuzzy_pair_match(r["home"], r["away"], events_df, thresh=90)
        if ev is None:
            unmatched_rows.append({"match_key": r["match_key"], "home": r["home"], "away": r["away"]})
        else:
            matched_rows.append({
                "match_key": r["match_key"],
                "team_home": ev["team_home"],
                "team_away": ev["team_away"],
                "odds_home": ev["odds_home"],
                "odds_draw": ev["odds_draw"],
                "odds_away": ev["odds_away"]
            })

    valid = pd.DataFrame(matched_rows)
    unmatched = pd.DataFrame(unmatched_rows)

    valid.to_csv(os.path.join(out_dir, "odds_theoddsapi.csv"), index=False)
    unmatched.to_csv(os.path.join(out_dir, "unmatched_theoddsapi.csv"), index=False)

    print(f"[theoddsapi-safe] linhas -> {json.dumps({ 'odds_theoddsapi.csv': int(valid.shape[0]), 'unmatched_theoddsapi.csv': int(unmatched.shape[0])})}")

if __name__ == "__main__":
    main()