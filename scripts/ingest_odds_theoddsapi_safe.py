#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingeste odds da TheOddsAPI e casa com os jogos do arquivo de entrada
fixo: data/in/matches_source.csv

Saídas (em OUT_DIR passado por --rodada):
- odds_theoddsapi.csv
- unmatched_theoddsapi.csv

Uso:
  python scripts/ingest_odds_theoddsapi_safe.py \
    --rodada data/out/<id_do_run> \
    --regions "uk,eu,us,au" \
    [--debug]
"""

import argparse
import csv
import json
import os
import sys
from typing import Dict, List, Tuple, Optional

import requests
import pandas as pd

# Imports utilitários
from scripts.text_normalizer import (
    load_aliases,
    canonicalize_team,
    make_match_key,
    equals_team,
    normalize_string,
)

INPUT_FILE = "data/in/matches_source.csv"  # ENTRADA FIXA

# Endpoints TheOddsAPI
THEODDS_ENDPOINT = "https://api.the-odds-api.com/v4/sports/{sport}/odds"

SPORTS = [
    "soccer_brazil_campeonato",  # Série A
    "soccer_brazil_serie_b",     # Série B
]

# Colunas esperadas do matches_source.csv
REQUIRED_COLS = ["home", "away"]  # "league" e "date" podem existir, mas não são exigidas


def debug_print(enabled: bool, *args):
    if enabled:
        print(*args, flush=True)


def read_source_matches(path: str, debug: bool = False) -> pd.DataFrame:
    if not os.path.isfile(path):
        print(f"[theoddsapi-safe] ERRO: {path} não encontrado", flush=True)
        sys.exit(2)

    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        print(f"[theoddsapi-safe] ERRO: coluna(s) ausente(s) em {path}: {', '.join(missing)}", flush=True)
        sys.exit(2)

    # Gera match_key se não existir
    if "match_key" not in df.columns:
        df["match_key"] = df.apply(lambda r: make_match_key(str(r["home"]), str(r["away"])), axis=1)

    # Renomeia para padrão interno
    df = df.rename(columns={"home": "team_home", "away": "team_away"})
    # Colunas ordenadas
    cols = ["match_key", "team_home", "team_away"] + [c for c in df.columns if c not in {"match_key", "team_home", "team_away"}]
    df = df[cols]
    debug_print(debug, f"[theoddsapi-safe][DEBUG] source rows: {len(df)}")
    return df


def fetch_odds_for_sport(api_key: str, sport: str, regions: str, debug: bool = False) -> List[dict]:
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": "h2h",
    }
    url = THEODDS_ENDPOINT.format(sport=sport)
    debug_print(debug, f"[theoddsapi-safe][DEBUG] GET {url} {params}")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def flatten_theodds_events(raw_events: List[dict]) -> List[dict]:
    """
    Converte resposta da TheOddsAPI em linhas simplificadas:
      {
        "team_home": ...,
        "team_away": ...,
        "odds_home": float|None,
        "odds_draw": float|None,
        "odds_away": float|None
      }
    OBS: A TheOddsAPI não marca home/away no objeto "bookmakers". Usaremos "home_team" do evento.
    """
    rows = []
    for ev in raw_events:
        home = ev.get("home_team", "")
        away = ev.get("away_team", "")

        # Pega PRIMEIRO bookmaker disponível com market h2h
        odds_home = odds_draw = odds_away = None
        try:
            books = ev.get("bookmakers", [])
            if books:
                mkts = books[0].get("markets", [])
                for m in mkts:
                    if m.get("key") == "h2h":
                        outcomes = m.get("outcomes", [])
                        # outcomes: [{name: "Draw"/team, price: ...}, ...]
                        for out in outcomes:
                            name = out.get("name", "")
                            price = out.get("price")
                            if not price:
                                continue
                            if name.lower() in {"draw", "empate"}:
                                odds_draw = float(price)
                            elif normalize_string(name) == normalize_string(home):
                                odds_home = float(price)
                            elif normalize_string(name) == normalize_string(away):
                                odds_away = float(price)
        except Exception:
            pass

        rows.append(
            {
                "team_home": home or "",
                "team_away": away or "",
                "odds_home": odds_home,
                "odds_draw": odds_draw,
                "odds_away": odds_away,
            }
        )
    return rows


def match_events_to_source(events_df: pd.DataFrame, src_df: pd.DataFrame, aliases_path: Optional[str], debug: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    aliases = load_aliases(aliases_path)

    # prepara lista de linhas válidas (eventos com ao menos UMA odd numérica)
    valid = events_df[
        (events_df["odds_home"].apply(lambda x: isinstance(x, (int, float)))) |
        (events_df["odds_draw"].apply(lambda x: isinstance(x, (int, float)))) |
        (events_df["odds_away"].apply(lambda x: isinstance(x, (int, float))))
    ].copy()

    if valid.empty:
        debug_print(debug, "[theoddsapi-safe][DEBUG] nenhum evento com odds numéricas -> 0 válidos.")
        matched = src_df.iloc[0:0].copy()
        matched[["odds_home", "odds_draw", "odds_away"]] = None
        return matched, src_df.copy()

    # cria match_key para eventos (normalizando nomes)
    valid["match_key"] = valid.apply(
        lambda r: make_match_key(
            canonicalize_team(str(r["team_home"]), aliases),
            canonicalize_team(str(r["team_away"]), aliases),
        ),
        axis=1,
    )

    # também canonicaliza a fonte para ficar comparável
    src_df = src_df.copy()
    src_df["__canon_home"] = src_df["team_home"].apply(lambda s: canonicalize_team(str(s), aliases))
    src_df["__canon_away"] = src_df["team_away"].apply(lambda s: canonicalize_team(str(s), aliases))
    src_df["__canon_key"] = src_df.apply(lambda r: make_match_key(r["__canon_home"], r["__canon_away"]), axis=1)

    # join por match_key canônica
    merged = src_df.merge(
        valid[["match_key", "odds_home", "odds_draw", "odds_away"]],
        left_on="__canon_key",
        right_on="match_key",
        how="left",
        suffixes=("", "_ev"),
    )

    matched = merged[~merged["odds_home"].isna() | ~merged["odds_draw"].isna() | ~merged["odds_away"].isna()].copy()
    unmatched = merged[merged["odds_home"].isna() & merged["odds_draw"].isna() & merged["odds_away"].isna()].copy()

    # padroniza saída matched
    if not matched.empty:
        matched_out = matched[["team_home", "team_away"]].copy()
        matched_out["match_key"] = matched["match_key"]  # o da direita (eventos) já é canônico
        matched_out["odds_home"] = matched["odds_home"]
        matched_out["odds_draw"] = matched["odds_draw"]
        matched_out["odds_away"] = matched["odds_away"]
    else:
        matched_out = src_df.iloc[0:0][["team_home", "team_away"]].copy()
        matched_out["match_key"] = []
        matched_out["odds_home"] = []
        matched_out["odds_draw"] = []
        matched_out["odds_away"] = []

    # saída unmatched: preserve colunas originais úteis
    if not unmatched.empty:
        unmatched_out = unmatched[["match_key", "team_home", "team_away"]].copy()
    else:
        unmatched_out = src_df.iloc[0:0][["match_key", "team_home", "team_away"]].copy()

    if debug:
        print(f"[theoddsapi-safe][DEBUG] src: {len(src_df)} | válidas(evts): {len(valid)} | matched: {len(matched_out)} | unmatched: {len(unmatched_out)}", flush=True)

    return matched_out, unmatched_out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório de saída (OUT_DIR)")
    parser.add_argument("--regions", default="uk,eu,us,au")
    parser.add_argument("--aliases", default="data/aliases_br.json")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    api_key = os.environ.get("THEODDS_API_KEY", "")
    if not api_key:
        print("[theoddsapi-safe] SKIP: THEODDS_API_KEY ausente.", flush=True)
        sys.exit(0)

    # 1) Carrega a FONTE (sempre fixa)
    src_df = read_source_matches(INPUT_FILE, debug=args.debug)

    # 2) Coleta eventos das ligas desejadas
    raw_all: List[dict] = []
    for sport in SPORTS:
        try:
            chunk = fetch_odds_for_sport(api_key, sport, args.regions, debug=args.debug)
            raw_all.extend(chunk)
        except requests.HTTPError as e:
            print(f"[theoddsapi-safe] AVISO: HTTP {e.response.status_code} em {sport} — seguindo.", flush=True)
        except Exception as e:
            print(f"[theoddsapi-safe] AVISO: erro '{e}' em {sport} — seguindo.", flush=True)

    # 3) Achata e filtra
    flat_rows = flatten_theodds_events(raw_all)
    events_df = pd.DataFrame(flat_rows)
    if events_df.empty:
        print(f"[theoddsapi-safe] AVISO: nenhum evento retornado pela API.", flush=True)

    # 4) Matching com a fonte
    valid_df, unmatched_df = match_events_to_source(events_df, src_df, aliases_path=args.aliases, debug=args.debug)

    # 5) Escritas
    odds_path = os.path.join(out_dir, "odds_theoddsapi.csv")
    unmatched_path = os.path.join(out_dir, "unmatched_theoddsapi.csv")

    # odds
    with open(odds_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["team_home", "team_away", "match_key", "odds_home", "odds_draw", "odds_away"],
        )
        writer.writeheader()
        for _, r in valid_df.iterrows():
            writer.writerow(
                {
                    "team_home": r["team_home"],
                    "team_away": r["team_away"],
                    "match_key": r["match_key"],
                    "odds_home": r.get("odds_home"),
                    "odds_draw": r.get("odds_draw"),
                    "odds_away": r.get("odds_away"),
                }
            )

    # unmatched
    unmatched_df.to_csv(unmatched_path, index=False)

    # 6) Logs finais
    print(f"[theoddsapi-safe] linhas -> {json.dumps({os.path.basename(odds_path): len(valid_df), os.path.basename(unmatched_path): len(unmatched_df)})}", flush=True)


if __name__ == "__main__":
    main()