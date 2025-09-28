#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_theoddsapi.py — Framework Loteca v4.3
Coleta odds 1x2 da TheOddsAPI para competições BR (e similares),
usa endpoint CORRETO por evento (v4: sports/{sport_key}/events/{event_id}/odds),
normaliza nomes e salva CSV + debug JSON. Integra com W&B (opcional).

Uso:
  python scripts/ingest_odds_theoddsapi.py --rodada 2025-09-27_1213 --regions "uk,eu,us,au" [--aliases data/aliases_br.json] [--debug]
Requer:
  - env var THEODDSAPI_KEY
Entradas:
  data/in/<RODADA>/matches_source.csv (colunas: match_id, home, away [,date])
Saídas:
  data/out/<RODADA>/odds_theoddsapi.csv
  data/out/<RODADA>/theoddsapi_debug.json
"""

from __future__ import annotations
import os
import sys
import json
import time
import math
import argparse
import unicodedata
from typing import Dict, List, Tuple, Any

import pandas as pd
import requests

# ------- W&B (opcional) -------
try:
    import wandb
    WANDB_OK = True
except Exception:
    WANDB_OK = False


# ----------------- Utils ----------------- #
def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)

def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    # normalizações leves
    s = s.replace(" fc", "").replace(" afc", "").replace(" ec", "")
    s = s.replace(".", "").replace("-", " ").replace("_", " ")
    s = " ".join(s.split())
    return s

def load_aliases(paths: List[str]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for p in paths or []:
        if not p:
            continue
        if os.path.isfile(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    j = json.load(f)
                    for k, arr in j.items():
                        nk = norm(k)
                        out.setdefault(nk, [])
                        # garanta dedup
                        vals = [norm(x) for x in (arr if isinstance(arr, list) else [arr])]
                        for v in vals:
                            if v and v not in out[nk]:
                                out[nk].append(v)
            except Exception:
                pass
    return out

def alias_map_lookup(n: str, aliases: Dict[str, List[str]]) -> str:
    # se n bater como chave, ok
    if n in aliases:
        return n
    # se n bater em algum value, retorna a chave canônica
    for k, arr in aliases.items():
        if n == k:
            return k
        if n in arr:
            return k
    return n


# ----------------- TheOddsAPI ----------------- #
BASE = "https://api.the-odds-api.com/v4"

def _req(path: str, params: Dict[str, Any], debug: bool=False) -> Any:
    url = f"{BASE}/{path}"
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        if debug:
            print(f"[theoddsapi] WARN: resposta não-JSON em {url}")
        return None

def list_sports(key: str) -> List[Dict[str, Any]]:
    return _req("sports", {"apiKey": key}, debug=False) or []

def list_events_for_sport(key: str, sport_key: str) -> List[Dict[str, Any]]:
    # v4 correto: /sports/{sport_key}/events
    return _req(f"sports/{sport_key}/events", {"apiKey": key}, debug=False) or []

def get_odds_for_event(key: str, sport_key: str, event_id: str, regions: str, markets: str="h2h") -> Any:
    # v4 correto: /sports/{sport_key}/events/{event_id}/odds
    return _req(
        f"sports/{sport_key}/events/{event_id}/odds",
        {"apiKey": key, "regions": regions, "markets": markets},
        debug=False
    ) or []


# ----------------- Coleta principal ----------------- #
def collect_theodds(matches: pd.DataFrame,
                    out_dir: str,
                    api_key: str,
                    regions: str,
                    aliases_files: List[str],
                    debug: bool=False,
                    wandb_run=None) -> pd.DataFrame:

    # aliases
    aliases = load_aliases(aliases_files)

    # detectar sports BR disponíveis na conta
    sports = list_sports(api_key)
    br_like = []
    for s in sports:
        sk = s.get("key", "")
        if any(tag in sk for tag in ["brazil", "campeonato", "serie_b", "serie a", "campeonato"]):
            br_like.append(sk)

    # fallback padrão (campeonato brasileiro + série b)
    fallbacks = ["soccer_brazil_campeonato", "soccer_brazil_serie_b"]
    # mantém somente os que existem na conta
    sport_keys = [x for x in br_like if x] or fallbacks
    if debug:
        print(f"[theoddsapi] sports detectados (BR): {sport_keys}")

    diag = []
    rows = []
    total_events = 0
    total_odds_found = 0

    # normalização de matches
    matches = matches.copy()
    matches["home_n"] = matches["home"].map(norm)
    matches["away_n"] = matches["away"].map(norm)
    # aplica aliases nas equipes do CSV
    matches["home_n"] = matches["home_n"].map(lambda x: alias_map_lookup(x, aliases))
    matches["away_n"] = matches["away_n"].map(lambda x: alias_map_lookup(x, aliases))

    for skey in sport_keys:
        events = list_events_for_sport(api_key, skey)
        total_events += len(events)
        odds_counter_this_sport = 0

        if debug:
            print(f"[theoddsapi] {skey}: eventos listados={len(events)}")

        for e in events:
            event_id = e.get("id")
            if not event_id:
                continue

            # coleta odds POR EVENTO (endpoint correto)
            try:
                odds_payload = get_odds_for_event(api_key, skey, event_id, regions=regions, markets="h2h")
            except requests.HTTPError as ex:
                if debug:
                    print(f"[theoddsapi] HTTPError {skey}/{event_id}: {ex}")
                continue
            except Exception as ex:
                if debug:
                    print(f"[theoddsapi] erro genérico {skey}/{event_id}: {ex}")
                continue

            # payload é lista de bookmakers com markets/outcomes
            # precisamos achar o melhor (qualquer) 1x2 (home/draw/away)
            # e extrair preços
            best = None
            best_bk = None
            commence_time = e.get("commence_time")

            for bk in odds_payload:
                mkt_list = bk.get("markets") or []
                for m in mkt_list:
                    if (m.get("key") or "").lower() != "h2h":
                        continue
                    outcomes = m.get("outcomes") or []
                    # outcomes ex: [{"name": "Home", "price": 1.90, "point": null}, ...]
                    # mapeia por nome
                    price_map = {}
                    for outc in outcomes:
                        nm = norm(outc.get("name"))
                        price_map[nm] = outc.get("price")
                    # tentativas de chaves
                    h = price_map.get("home") or price_map.get("team1")
                    d = price_map.get("draw") or price_map.get("empate") or price_map.get("tie")
                    a = price_map.get("away") or price_map.get("team2")
                    if h and a and d:
                        best = (h, d, a)
                        best_bk = bk.get("bookmaker")
                        break
                if best:
                    break

            if not best:
                continue

            # nomes do evento (da TheOddsAPI)
            home_name = norm(e.get("home_team"))
            away_name = norm(e.get("away_team"))

            # aplica aliases nos nomes do evento para cruzar com matches
            home_name = alias_map_lookup(home_name, aliases)
            away_name = alias_map_lookup(away_name, aliases)

            # tenta cruzar com matches da rodada
            hit = matches[(matches["home_n"] == home_name) & (matches["away_n"] == away_name)]
            if len(hit) == 0:
                # tenta também invertido (por garantia, embora soccer costume ser home/away direto)
                hit = matches[(matches["home_n"] == away_name) & (matches["away_n"] == home_name)]
                invert = True
            else:
                invert = False

            if len(hit) == 0:
                # sem match, mas ainda assim registramos as odds “soltas”?
                # aqui vamos descartar (foco em casar com matches da rodada)
                continue

            row = {
                "match_id": hit.iloc[0]["match_id"],
                "home": hit.iloc[0]["home"],
                "away": hit.iloc[0]["away"],
                "home_n": hit.iloc[0]["home_n"],
                "away_n": hit.iloc[0]["away_n"],
                "sport": skey,
                "event_id": event_id,
                "commence_time": commence_time,
                "bookmaker": best_bk or "",
                "odd_home": best[2] if invert else best[0],
                "odd_draw": best[1],
                "odd_away": best[0] if invert else best[2],
            }
            rows.append(row)
            odds_counter_this_sport += 1
            total_odds_found += 1

        diag.append({
            "sport": skey,
            "events": len(events),
            "odds_events": odds_counter_this_sport,
            "skipped_404": False
        })

    # monta df final
    out = pd.DataFrame(rows, columns=[
        "match_id","home","away","home_n","away_n",
        "sport","event_id","commence_time","bookmaker",
        "odd_home","odd_draw","odd_away"
    ])

    # salva
    out_path = os.path.join(out_dir, "odds_theoddsapi.csv")
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[theoddsapi] OK -> {out_path} ({len(out)} linhas)")

    # debug json
    dbg = {
        "when": pd.Timestamp.now(tz="America/Sao_Paulo").isoformat(),
        "regions": regions,
        "sports": sport_keys,
        "diag": diag
    }
    dbg_path = os.path.join(out_dir, "theoddsapi_debug.json")
    with open(dbg_path, "w", encoding="utf-8") as f:
        json.dump(dbg, f, ensure_ascii=False, indent=2)
    if debug:
        print(f"[theoddsapi] debug salvo em: {dbg_path}")

    # W&B
    if wandb_run is not None:
        wandb_run.log({
            "sports_consultados": len(sport_keys),
            "theoddsapi_rows": len(out),
        })

    return out


# ----------------- Entradas / Main ----------------- #
def load_matches(rodada: str) -> pd.DataFrame:
    p = os.path.join("data", "in", rodada, "matches_source.csv")
    if not os.path.isfile(p):
        raise FileNotFoundError(f"[theoddsapi] arquivo ausente: {p}")
    df = pd.read_csv(p)
    req = ["match_id", "home", "away"]
    for c in req:
        if c not in df.columns:
            raise RuntimeError(f"[theoddsapi] coluna obrigatória ausente em matches_source.csv: {c}")
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="ex.: 2025-09-27_1213")
    parser.add_argument("--regions", default="uk,eu,us,au")
    parser.add_argument("--aliases", action="append", default=[], help="caminho(s) para JSON de aliases; pode repetir")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    key = os.environ.get("THEODDSAPI_KEY", "")
    if not key:
        print("ERRO: defina THEODDSAPI_KEY no ambiente.", file=sys.stderr)
        sys.exit(2)

    out_dir = os.path.join("data", "out", args.rodada)
    safe_mkdir(out_dir)

    matches = load_matches(args.rodada)

    wb = None
    if WANDB_OK and os.environ.get("WANDB_API_KEY"):
        # substitui reinit=True por finish_previous=True (sem warning)
        wb = wandb.init(project="loteca",
                        name=f"theoddsapi_{args.rodada}",
                        settings=wandb.Settings(start_method="thread"),
                        finish_previous=True)
    try:
        df = collect_theodds(matches, out_dir, key, args.regions, args.aliases, debug=args.debug, wandb_run=wb)
    finally:
        if wb is not None:
            wb.finish()

if __name__ == "__main__":
    main()
