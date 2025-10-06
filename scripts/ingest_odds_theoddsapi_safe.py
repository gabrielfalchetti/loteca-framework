#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json
import time
import argparse
from typing import List, Dict, Tuple

import requests
import pandas as pd
from rapidfuzz import fuzz, process

# ---------- Utilidades ----------
def dbg(msg: str, enable: bool):
    if enable:
        print(f"[theoddsapi-safe][DEBUG] {msg}", flush=True)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def norm(s: str) -> str:
    return (s or "").strip().lower()

def make_match_key(home: str, away: str) -> str:
    return f"{norm(home)}__vs__{norm(away)}"

def read_matches_source(in_path: str, debug: bool=False) -> pd.DataFrame:
    if not os.path.isfile(in_path):
        print(f"[theoddsapi-safe] ERRO: arquivo não encontrado: {in_path}", flush=True)
        sys.exit(2)

    df = pd.read_csv(in_path)
    # Aceita o formato simples (home,away). Se vier outro, acusa.
    required = {"home", "away"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"[theoddsapi-safe] ERRO: coluna(s) ausente(s) em {in_path}: {', '.join(missing)}", flush=True)
        sys.exit(2)

    # Cria match_key
    df["match_key"] = df.apply(lambda r: make_match_key(r["home"], r["away"]), axis=1)
    # Canoniza auxiliares
    df["home_n"] = df["home"].map(norm)
    df["away_n"] = df["away"].map(norm)
    dbg(f"matches_source lidas: {len(df)}", debug)
    return df[["match_key", "home", "away", "home_n", "away_n"]]

def get_sport_keys_from_env() -> List[str]:
    env_val = os.getenv("ODDS_SPORTS", "").strip()
    if not env_val:
        # Fallback (Brasil – antigo comportamento). Mas recomendamos setar ODDS_SPORTS no workflow.
        return ["soccer_brazil_campeonato", "soccer_brazil_serie_b"]
    return [s.strip() for s in env_val.split(",") if s.strip()]

def fetch_odds_for_sport(api_key: str, sport_key: str, regions: str, debug: bool=False) -> List[Dict]:
    base = "https://api.the-odds-api.com/v4/sports"
    url = f"{base}/{sport_key}/odds"
    params = {"apiKey": api_key, "regions": regions, "markets": "h2h"}
    dbg(f"GET {url} {params}", debug)
    r = requests.get(url, params=params, timeout=30)
    if r.status_code == 404:
        # esporte não disponível (TheOddsAPI retorna 404 para sport_key desconhecida)
        dbg(f"404 para {sport_key} — ignorando", debug)
        return []
    r.raise_for_status()
    return r.json()

def explode_events(raw: List[Dict]) -> pd.DataFrame:
    # Converte a resposta da TheOddsAPI em linhas (home, away e odds)
    records = []
    for ev in raw:
        try:
            teams = ev.get("teams", [])
            home = ev.get("home_team", "")
            # TheOddsAPI nem sempre marca home_team; se faltar, tentamos inferir
            if not home and len(teams) == 2:
                home = teams[0]
            away = ""
            if len(teams) == 2:
                away = teams[1] if teams[0] == home else teams[0]

            # odds H2H: buscamos melhor média entre os books disponíveis
            o_home, o_draw, o_away = None, None, None
            for bk in ev.get("bookmakers", []):
                for mk in bk.get("markets", []):
                    if mk.get("key") == "h2h":
                        sel = mk.get("outcomes", [])
                        # outcomes podem vir em qualquer ordem
                        for s in sel:
                            if norm(s.get("name")) in (norm(home), "home", "1"):
                                o_home = s.get("price", o_home)
                            elif norm(s.get("name")) in (norm(away), "away", "2"):
                                o_away = s.get("price", o_away)
                            elif norm(s.get("name")) in ("draw", "empate", "x"):
                                o_draw = s.get("price", o_draw)
            if home and away and (o_home or o_away or o_draw):
                records.append({
                    "team_home_api": home,
                    "team_away_api": away,
                    "odds_home": o_home,
                    "odds_draw": o_draw,
                    "odds_away": o_away,
                })
        except Exception:
            continue
    return pd.DataFrame.from_records(records)

def fuzzy_match(src: pd.DataFrame, ev: pd.DataFrame, debug: bool=False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if ev.empty:
        return pd.DataFrame(columns=[
            "match_key","team_home","team_away","odds_home","odds_draw","odds_away"
        ]), src.copy()

    # Listas normalizadas do source
    pairs = src[["match_key", "home", "away", "home_n", "away_n"]].to_dict("records")

    matched_rows = []
    used_keys = set()

    for _, row in ev.iterrows():
        h_api = row["team_home_api"]
        a_api = row["team_away_api"]
        h_n, a_n = norm(h_api), norm(a_api)

        # Melhor par source por soma das similaridades
        best = None
        best_score = -1
        for p in pairs:
            # sim entre home_api vs home_src e away_api vs away_src
            s1 = fuzz.token_set_ratio(h_n, p["home_n"])
            s2 = fuzz.token_set_ratio(a_n, p["away_n"])
            score = (s1 + s2) / 2
            if score > best_score:
                best_score = score
                best = p

        if best and best_score >= 80:  # limiar razoável
            mk = best["match_key"]
            if mk not in used_keys:
                matched_rows.append({
                    "match_key": mk,
                    "team_home": best["home"],
                    "team_away": best["away"],
                    "odds_home": row.get("odds_home"),
                    "odds_draw": row.get("odds_draw"),
                    "odds_away": row.get("odds_away"),
                })
                used_keys.add(mk)

    matched_df = pd.DataFrame(matched_rows)
    unmatched_df = src[~src["match_key"].isin(list(used_keys))].copy()
    dbg(f"casados={len(matched_df)}; sem_casar={len(unmatched_df)}", debug)
    return matched_df, unmatched_df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída (OUT_DIR)")
    ap.add_argument("--regions", default=os.getenv("REGIONS", "uk,eu,us,au"))
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    ensure_dir(out_dir)

    # Entrada fixa que combinamos
    src_path = "data/in/matches_source.csv"
    src_df = read_matches_source(src_path, debug=args.debug)

    api_key = os.getenv("THEODDS_API_KEY", "").strip()
    if not api_key:
        print("[theoddsapi-safe] ERRO: THEODDS_API_KEY ausente.", flush=True)
        sys.exit(2)

    sport_keys = get_sport_keys_from_env()
    all_events = []

    for sk in sport_keys:
        try:
            raw = fetch_odds_for_sport(api_key, sk, args.regions, debug=args.debug)
            if raw:
                ev_df = explode_events(raw)
                if not ev_df.empty:
                    all_events.append(ev_df)
        except requests.HTTPError as e:
            print(f"[theoddsapi-safe] AVISO: {sk}: {e}", flush=True)
        except Exception as e:
            print(f"[theoddsapi-safe] AVISO: erro genérico em {sk}: {e}", flush=True)

    if all_events:
        events_df = pd.concat(all_events, ignore_index=True)
    else:
        events_df = pd.DataFrame(columns=["team_home_api","team_away_api","odds_home","odds_draw","odds_away"])

    valid_df, unmatched_df = fuzzy_match(src_df, events_df, debug=args.debug)

    # Garante saída, mesmo vazia (para não quebrar o consensus)
    ensure_dir(out_dir)
    odds_path = os.path.join(out_dir, "odds_theoddsapi.csv")
    unmatched_path = os.path.join(out_dir, "unmatched_theoddsapi.csv")

    valid_cols = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]
    valid_df = valid_df.reindex(columns=valid_cols)
    valid_df.to_csv(odds_path, index=False)

    unmatched_cols = ["match_key","home","away"]
    unmatched_df = unmatched_df.reindex(columns=unmatched_cols)
    unmatched_df.to_csv(unmatched_path, index=False)

    print(f"[theoddsapi-safe] linhas -> {json.dumps({os.path.basename(odds_path): int(valid_df.shape[0]), os.path.basename(unmatched_path): int(unmatched_df.shape[0])})}", flush=True)


if __name__ == "__main__":
    main()