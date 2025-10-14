#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import os
import sys
import time
import re
from typing import Dict, List, Tuple, Optional

import requests
import pandas as pd
from unidecode import unidecode


# ========================= Utils de normalização =========================

def _slug(s: str) -> str:
    if s is None:
        return ""
    s = unidecode(str(s)).lower()
    s = (
        s.replace("(", " ")
         .replace(")", " ")
         .replace("/", " ")
         .replace("-", " ")
         .replace("_", " ")
         .replace(".", " ")
         .replace(",", " ")
         .replace("'", " ")
         .replace("’", " ")
         .replace("&", " and ")
    )
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def pick_home_away_cols(df: pd.DataFrame) -> Tuple[str, str]:
    # prioriza colunas normalizadas
    lc = {c.lower(): c for c in df.columns}
    home = lc.get("home_norm") or lc.get("home") or lc.get("team_home") or lc.get("mandante")
    away = lc.get("away_norm") or lc.get("away") or lc.get("team_away") or lc.get("visitante")
    if not home or not away:
        print("::error::CSV precisa conter colunas home/home_norm e away/away_norm", file=sys.stderr)
        sys.exit(5)
    return home, away


def pick_id_col(df: pd.DataFrame) -> str:
    lc = {c.lower(): c for c in df.columns}
    mid = lc.get("match_id") or lc.get("id") or lc.get("jogo") or lc.get("game_id")
    if not mid:
        print("::error::CSV precisa conter coluna match_id (ou id/jogo/game_id)", file=sys.stderr)
        sys.exit(5)
    return mid


# ========================= TheOddsAPI Client =========================

API_BASE = "https://api.the-odds-api.com/v4"

def fetch_upcoming_odds(api_key: str, regions: str, sport_key: str = "upcoming", markets: str = "h2h",
                        odds_format: str = "decimal", date_format: str = "iso", timeout: int = 20) -> List[dict]:
    """
    Busca odds de jogos vindouros. Para Soccer em geral, o 'sport_key' 'upcoming' funciona e retorna vários esportes.
    Alternativamente, você pode usar 'soccer' (mas manteremos 'upcoming' como nos seus logs).
    """
    url = f"{API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
    }
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 401:
            print(f"[theoddsapi][ERROR] 401 Unauthorized — verifique a THEODDS_API_KEY e o plano/limite de requests.", file=sys.stderr)
            return []
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            print("[theoddsapi][WARN] Resposta inesperada (não é lista).", file=sys.stderr)
            return []
        return data
    except requests.RequestException as e:
        print(f"[theoddsapi][ERROR] Falha ao consultar TheOddsAPI: {e}", file=sys.stderr)
        return []


# ========================= Extração de odds H2H =========================

def extract_h2h_prices(event: dict) -> Tuple[Optional[str], Optional[str], List[Tuple[str, float]]]:
    """
    Retorna (home_team, away_team, lista_de_odds) em que lista_de_odds contém tuplas (nome, odd).
    Agrega odds percorrendo todos os bookmakers e coletando o market 'h2h'.
    """
    home = event.get("home_team")
    away = event.get("away_team")

    prices: Dict[str, List[float]] = {}
    books = event.get("bookmakers") or []
    for bk in books:
        markets = bk.get("markets") or []
        for mk in markets:
            if (mk.get("key") or "").lower() != "h2h":
                continue
            outcomes = mk.get("outcomes") or []
            for o in outcomes:
                name = o.get("name")
                price = o.get("price")
                if name is None or price is None:
                    continue
                key = _slug(name)
                prices.setdefault(key, []).append(float(price))

    averaged = []
    for k, vals in prices.items():
        if not vals:
            continue
        averaged.append((k, sum(vals) / len(vals)))

    return home, away, averaged


def match_event_to_pair(event_home: str, event_away: str, our_home: str, our_away: str) -> Tuple[bool, bool]:
    """
    Tenta casar (event_home,event_away) com (our_home,our_away) por slug.
    Retorna (matched, reversed):
      matched=True se houver match
      reversed=True se o evento vier invertido (home/away trocados)
    """
    eh = _slug(event_home)
    ea = _slug(event_away)
    oh = _slug(our_home)
    oa = _slug(our_away)

    if eh == oh and ea == oa:
        return True, False
    if eh == oa and ea == oh:
        return True, True
    return False, False


def pick_h2h_triplet(averaged_prices: List[Tuple[str, float]], names_home: List[str], names_away: List[str]) -> Tuple[Optional[float], Optional[float], Optional[float], int]:
    """
    A partir da lista média (key_slug, odd), seleciona odds de:
      - home (nomes possíveis de casa)
      - draw (empate)
      - away (nomes possíveis de fora)
    Também retorna 'books_count' estimado como o máximo de amostras usadas entre os três buckets.
    """
    # Mapeia chave slug -> odd
    price_map = {k: v for k, v in averaged_prices}

    draw_keys = {_slug("Draw"), _slug("Empate")}
    # gera conjunto de chaves possíveis para nomes
    home_keys = {_slug(n) for n in names_home if n}
    away_keys = {_slug(n) for n in names_away if n}

    # Busca por match direto; se não achar, tenta aproximações simples
    def _first_in(keys: set) -> Optional[float]:
        for k in keys:
            if k in price_map:
                return price_map[k]
        return None

    o_home = _first_in(home_keys)
    o_away = _first_in(away_keys)
    o_draw = _first_in(draw_keys)

    # books_count aproximado: não temos contagem por outcome aqui; usa a quantidade média de entradas agregadas
    # Como aproximação simples (e estável), usa 0 se não temos preços; senão usa 1.
    books_count = 1 if any(x is not None for x in (o_home, o_draw, o_away)) else 0

    return o_home, o_draw, o_away, books_count


# ========================= Pipeline principal =========================

def main():
    ap = argparse.ArgumentParser(description="Ingest odds from TheOddsAPI and match with provided fixtures.")
    ap.add_argument("--rodada", required=True, help="Diretório de saída desta rodada (ex: data/out/<run_id>)")
    ap.add_argument("--regions", required=True, help="Regiões da TheOddsAPI (ex: uk,eu,us,au)")
    ap.add_argument("--source_csv", required=True, help="CSV com match_id/home/away (ou home_norm/away_norm)")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "odds_theoddsapi.csv")

    api_key = os.environ.get("THEODDS_API_KEY", "").strip()
    if not api_key:
        print("::error::THEODDS_API_KEY não configurada no ambiente.", file=sys.stderr)
        sys.exit(5)

    # Carrega fixtures
    try:
        df = pd.read_csv(args.source_csv)
    except Exception as e:
        print(f"::error::Falha ao ler {args.source_csv}: {e}", file=sys.stderr)
        sys.exit(5)

    mid_col = pick_id_col(df)
    home_col, away_col = pick_home_away_cols(df)

    # Normaliza strings
    df = df[[mid_col, home_col, away_col]].copy()
    df.columns = ["match_id", "home", "away"]
    df["home"] = df["home"].astype(str).str.strip()
    df["away"] = df["away"].astype(str).str.strip()

    # Consulta API
    events = fetch_upcoming_odds(api_key=api_key, regions=args.regions, sport_key="upcoming", markets="h2h")
    if not events:
        # Ainda assim, gera arquivo vazio com cabeçalho para não quebrar etapas posteriores
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "books_count"])
        print(f"[theoddsapi]Eventos=0 | jogoselecionados={len(df)} — nenhum match. Arquivo vazio criado.")
        return

    # Pré-processa eventos: extrai tuplas úteis
    processed_events = []
    for ev in events:
        ehome, eaway, prices = extract_h2h_prices(ev)
        if not ehome or not eaway:
            # Alguns esportes não têm esses campos — ignorar
            continue
        processed_events.append((ehome, eaway, prices))

    rows_out: List[List] = []
    matched = 0

    for _, r in df.iterrows():
        mid = r["match_id"]
        our_home = r["home"]
        our_away = r["away"]

        found = False
        best_row = None

        for (ehome, eaway, prices) in processed_events:
            ok, reversed_flag = match_event_to_pair(ehome, eaway, our_home, our_away)
            if not ok:
                continue

            # nomes possíveis para outcomes (para resolver diferenças de escrita nos outcomes):
            # quando reversed_flag=True, o 'home' do evento é nosso away.
            if not reversed_flag:
                names_home = [ehome, our_home]
                names_away = [eaway, our_away]
            else:
                names_home = [eaway, our_home]  # invertido
                names_away = [ehome, our_away]

            o_home, o_draw, o_away, books_count = pick_h2h_triplet(prices, names_home, names_away)

            # Só aceitamos se temos pelo menos 2 odds (home e away). Draw pode faltar em alguns books.
            if o_home is not None and o_away is not None:
                # Se vier invertido, os nomes finais devem refletir nosso CSV
                out_home_name = our_home
                out_away_name = our_away
                best_row = [mid, out_home_name, out_away_name, o_home, o_draw, o_away, books_count]
                found = True
                break

        if found and best_row:
            rows_out.append(best_row)
            matched += 1

    # Salva CSV mesmo que vazio (com header)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "books_count"])
        w.writerows(rows_out)

    print(f"[theoddsapi]Eventos={len(events)} | jogoselecionados={len(df)} | pareados={matched} — salvo em {out_csv}")


if __name__ == "__main__":
    main()