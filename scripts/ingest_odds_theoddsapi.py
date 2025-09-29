#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coleta odds H2H (1X2) do TheOddsAPI por sport_key e cruza com os jogos da rodada.

Saída:
  data/out/{rodada}/odds_theoddsapi.csv

Colunas:
  match_id, utc_kickoff, country_code, league_name,
  home_team, away_team,
  bookmaker, home_odds, draw_odds, away_odds,
  provider, last_update

Requisitos:
  - Secret THEODDS_API_KEY definido no ambiente
  - Arquivo de entrada: data/in/{rodada}/matches_source.csv
  - Mapa de ligas -> sport_key: data/theodds_league_map.json
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
import unicodedata

import pandas as pd
import requests

THEODDS_BASE = "https://api.the-odds-api.com/v4"
DEFAULT_MARKETS = "h2h"
DEFAULT_ODDS_FORMAT = "decimal"
DEFAULT_DATE_FORMAT = "iso"

def log(msg: str):
    print(f"[theoddsapi] {msg}", flush=True)

def fail(code: int, msg: str):
    log(f"ERRO: {msg}")
    sys.exit(code)

def normalize_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return " ".join(s.strip().lower().split())

def read_matches(rodada: str) -> pd.DataFrame:
    inp = f"data/in/{rodada}/matches_source.csv"
    if not os.path.exists(inp):
        fail(2, f"arquivo de entrada nao encontrado: {inp}")
    df = pd.read_csv(inp)
    required = ["match_id", "utc_kickoff", "country_code", "league_name", "home_team", "away_team"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        fail(2, f"colunas ausentes em matches_source.csv: {missing}")

    # Normalizações auxiliares
    for c in ["home_team", "away_team", "country_code", "league_name"]:
        df[c] = df[c].astype(str)

    df["norm_home"] = df["home_team"].map(normalize_name)
    df["norm_away"] = df["away_team"].map(normalize_name)
    df["league_key"] = (df["country_code"].str.upper().str.strip() + ":" + df["league_name"].str.strip())
    return df

def load_league_map() -> dict:
    path = "data/theodds_league_map.json"
    if not os.path.exists(path):
        fail(2, f"mapa de ligas nao encontrado: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def fetch_odds_for_sport(sport_key: str, api_key: str, regions: str) -> list:
    """
    Retorna lista de eventos (JSON) para um sport_key.
    """
    url = (
        f"{THEODDS_BASE}/sports/{sport_key}/odds"
        f"?apiKey={api_key}"
        f"&regions={regions}"
        f"&markets={DEFAULT_MARKETS}"
        f"&oddsFormat={DEFAULT_ODDS_FORMAT}"
        f"&dateFormat={DEFAULT_DATE_FORMAT}"
    )
    r = requests.get(url, timeout=20)
    if r.status_code == 429:
        # rate limit: segura 2s e tenta 1 vez
        time.sleep(2)
        r = requests.get(url, timeout=20)
    if r.status_code != 200:
        log(f"AVISO: status {r.status_code} ao consultar {sport_key}: {r.text[:180]}")
        return []
    try:
        data = r.json()
        if not isinstance(data, list):
            log(f"AVISO: retorno inesperado em {sport_key}: {str(data)[:180]}")
            return []
        return data
    except Exception as e:
        log(f"AVISO: erro parseando JSON de {sport_key}: {e}")
        return []

def event_candidates_to_rows(event: dict) -> list:
    """
    Converte um evento do TheOddsAPI em linhas por bookmaker com odds (home,draw,away).
    """
    rows = []
    try:
        commence_time = event.get("commence_time")  # ISO
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        bookmakers = event.get("bookmakers", []) or []
        for bk in bookmakers:
            bk_name = bk.get("title") or bk.get("key") or ""
            last_update = bk.get("last_update") or event.get("commence_time")
            markets = bk.get("markets", []) or []
            for m in markets:
                if (m.get("key") or "").lower() != "h2h":
                    continue
                outcomes = m.get("outcomes", []) or []
                # outcomes: list of { name: "Home/Draw/Away team name", price: float }
                home_odds = None
                draw_odds = None
                away_odds = None
                for o in outcomes:
                    name = (o.get("name") or "").strip()
                    price = o.get("price")
                    nname = normalize_name(name)
                    # tenta identificar home/away por comparação com nomes do evento
                    if nname == normalize_name(home):
                        home_odds = price
                    elif nname == normalize_name(away):
                        away_odds = price
                    elif nname in ("draw", "empate", "x"):
                        draw_odds = price
                # fallback: se não identificou pelo nome, usa posição (não é o ideal, mas evita perder dado)
                if home_odds is None or away_odds is None:
                    # alguns feeds não repetem o nome do time na outcome
                    if len(outcomes) == 3:
                        # supõe ordem [home, draw, away]
                        home_odds = home_odds or outcomes[0].get("price")
                        draw_odds = draw_odds or outcomes[1].get("price")
                        away_odds = away_odds or outcomes[2].get("price")

                if home_odds and away_odds and draw_odds:
                    rows.append({
                        "utc_kickoff": commence_time,
                        "home_team": home,
                        "away_team": away,
                        "bookmaker": bk_name,
                        "home_odds": home_odds,
                        "draw_odds": draw_odds,
                        "away_odds": away_odds,
                        "provider": "theoddsapi",
                        "last_update": last_update,
                    })
    except Exception as e:
        log(f"AVISO: erro convertendo evento -> rows: {e}")
    return rows

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="ID da rodada (ex: 2025-09-27_1213)")
    parser.add_argument("--regions", default="uk,eu,us,au", help="regioes TheOddsAPI (ex: uk,eu,us,au)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    rodada = args.rodada
    regions = args.regions

    api_key = os.getenv("THEODDS_API_KEY", "").strip()
    if not api_key:
        fail(1, "THEODDS_API_KEY nao definido")

    out_dir = f"data/out/{rodada}"
    os.makedirs(out_dir, exist_ok=True)
    out_csv = f"{out_dir}/odds_theoddsapi.csv"

    matches = read_matches(rodada)
    log(f"Entrada: data/in/{rodada}/matches_source.csv")
    log(f"Saida:   {out_csv}")
    log(f"Partidas lidas: {len(matches)}")

    league_map = load_league_map()

    # Seleciona jogos ainda futuros (TheOdds trabalha com 'upcoming' p/ odds)
    now_utc = datetime.now(timezone.utc)
    def is_future(row):
        try:
            dt = datetime.fromisoformat(str(row["utc_kickoff"]).replace("Z", "+00:00"))
            return dt > now_utc
        except Exception:
            return True  # se não conseguir parsear, não bloqueia
    matches["is_future"] = matches.apply(is_future, axis=1)
    # Mesmo que alguns jogos não sejam futuros, ainda tentamos casar com odds retornadas (não filtramos agressivo)

    # Agrupa por league_key para buscar o sport_key correspondente
    league_keys = sorted(matches["league_key"].unique())
    all_rows = []

    for lk in league_keys:
        sport_key = league_map.get(lk)
        if not sport_key:
            log(f"AVISO: league_key sem sport_key no mapa: '{lk}' (adicionar em data/theodds_league_map.json)")
            continue

        # Busca odds no provedor por sport_key
        events = fetch_odds_for_sport(sport_key, api_key, regions)
        if args.debug:
            log(f"DEBUG: {sport_key} retornou {len(events)} eventos")

        # Converte todos os eventos para linhas por bookmaker
        sport_rows = []
        for ev in events:
            sport_rows.extend(event_candidates_to_rows(ev))
        if not sport_rows:
            continue
        df_sport = pd.DataFrame(sport_rows)
        # Normaliza auxiliar p/ join por nome
        df_sport["norm_home"] = df_sport["home_team"].map(normalize_name)
        df_sport["norm_away"] = df_sport["away_team"].map(normalize_name)

        # Subconjunto de matches da liga
        sub = matches[matches["league_key"] == lk].copy()

        # Join por nome normalizado (home/away + kickoff por data, com tolerância)
        # Primeiro, tenta join por nomes; depois, se necessário, aproxima kickoff por data (mesmo dia)
        merged = sub.merge(
            df_sport,
            how="left",
            left_on=["norm_home", "norm_away"],
            right_on=["norm_home", "norm_away"],
            suffixes=("", "_prov")
        )

        # Filtra por mesmo dia (para reduzir colisões de jogos homônimos)
        def same_day(a, b) -> bool:
            try:
                da = datetime.fromisoformat(str(a).replace("Z", "+00:00")).date()
                db = datetime.fromisoformat(str(b).replace("Z", "+00:00")).date()
                return da == db
            except Exception:
                return True

        if "utc_kickoff_prov" in merged.columns:
            merged = merged[ merged.apply(lambda r: same_day(r.get("utc_kickoff"), r.get("utc_kickoff_prov")), axis=1) ]

        # Mantém apenas linhas com odds coletadas
        merged = merged.dropna(subset=["home_odds", "away_odds", "draw_odds"])

        # Seleciona colunas finais
        keep_cols = [
            "match_id", "utc_kickoff", "country_code", "league_name",
            "home_team", "away_team",
            "bookmaker", "home_odds", "draw_odds", "away_odds",
            "provider", "last_update"
        ]
        merged = merged[keep_cols] if set(keep_cols).issubset(merged.columns) else merged

        all_rows.append(merged)

    if all_rows:
        out = pd.concat(all_rows, ignore_index=True)
    else:
        out = pd.DataFrame(columns=[
            "match_id","utc_kickoff","country_code","league_name",
            "home_team","away_team","bookmaker",
            "home_odds","draw_odds","away_odds","provider","last_update"
        ])

    out.to_csv(out_csv, index=False)
    log(f"OK -> {out_csv} ({len(out)} linhas)")

if __name__ == "__main__":
    main()
