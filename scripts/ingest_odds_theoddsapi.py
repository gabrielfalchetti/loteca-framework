#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_theoddsapi.py
Coleta odds H2H no TheOddsAPI com:
- Inferência/mapeamento de sport_key por liga (league_map).
- Fallback automático quando sport_key do CSV é inválido/indisponível:
  tenta um pool de sports do Brasil (Série A/B/C/D) para achar o evento.
- Matching robusto de times com aliases + normalização (Unidecode).
- Exporta k1 (mandante), kx (empate), k2 (visitante).

Saída: data/out/<rodada>/odds_theoddsapi.csv
"""

import os
import sys
import json
import argparse
import time
from typing import Dict, List, Tuple

import requests
import pandas as pd
from rapidfuzz import fuzz, process
from unidecode import unidecode

DEFAULT_REGIONS = "uk,eu,us,au"
ODDS_MARKET = "h2h"
ODDS_FORMAT = "decimal"
DATE_FORMAT = "iso"
TIMEOUT = 25

# Pool de sports para fallback (ordem de prioridade)
# IMPORTANTE: ajuste conforme seu plano do TheOddsAPI
BRAZIL_SPORT_POOL = [
    "soccer_brazil_serie_a",
    "soccer_brazil_serie_b",
    "soccer_brazil_serie_c",
    "soccer_brazil_serie_d"
    # se sua conta tiver copa habilitada e a chave correta, adicione aqui
    # "soccer_brazil_cup"
]

def log(msg: str):
    print(f"[theoddsapi] {msg}", flush=True)

def warn(msg: str):
    print(f"[theoddsapi] AVISO {msg}", flush=True)

def err(msg: str):
    print(f"[theoddsapi] ERRO {msg}", flush=True)

def read_aliases(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        return {"teams": {}}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def normalize_name(name: str, alias_map: Dict[str, str]) -> str:
    # aplica aliases e remove acentos/variações
    base = name.strip()
    # match por chave case-insensitive
    keyspace = {k.lower(): v for k, v in alias_map.items()}
    if base.lower() in keyspace:
        base = keyspace[base.lower()]
    return unidecode(base).lower()

def best_match(target: str, candidates: List[str]) -> Tuple[str, float]:
    # retorna (melhor_candidato, score)
    if not candidates:
        return ("", 0.0)
    match, score, _ = process.extractOne(
        target, candidates, scorer=fuzz.token_set_ratio
    )
    return (match, float(score))

def fetch_sport_events(api_key: str, sport_key: str, regions: str) -> List[dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": ODDS_MARKET,
        "oddsFormat": ODDS_FORMAT,
        "dateFormat": DATE_FORMAT
    }
    r = requests.get(url, params=params, timeout=TIMEOUT)
    if r.status_code == 404:
        # sport desconhecido na sua conta/plano
        warn(f"{sport_key}: 404 UNKNOWN_SPORT")
        return []
    if r.status_code != 200:
        warn(f"{sport_key}: HTTP {r.status_code} -> {r.text[:200]}")
        return []
    try:
        return r.json()
    except Exception:
        warn(f"{sport_key}: JSON inválido")
        return []

def league_to_sport(league: str, league_map: Dict) -> str:
    if not league:
        return ""
    l = unidecode(league).lower().strip()
    # percorre os blocos do json e procura em aliases
    for _, node in league_map.items():
        skey = node.get("sport_key", "")
        aliases = [unidecode(a).lower() for a in node.get("aliases", [])]
        if l == skey or l in aliases or any(l == a for a in aliases):
            return skey
        # match parcial
        scores = [fuzz.partial_ratio(l, a) for a in aliases]
        if scores and max(scores) >= 85:
            return skey
    return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ex: 2025-09-27_1213")
    ap.add_argument("--regions", default=DEFAULT_REGIONS)
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--league_map", default="data/theoddsapi_league_map.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    api_key = os.environ.get("THEODDS_API_KEY", "").strip()
    if not api_key:
        err("THEODDS_API_KEY não definido.")
        sys.exit(2)

    in_csv = f"data/in/{args.rodada}/matches_source.csv"
    out_csv = f"data/out/{args.rodada}/odds_theoddsapi.csv"
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    if not os.path.exists(in_csv):
        err(f"Arquivo de entrada inexistente: {in_csv}")
        sys.exit(2)

    matches = pd.read_csv(in_csv)
    if not {"match_id","home","away"}.issubset(set(matches.columns)):
        err("matches_source.csv precisa de colunas: match_id,home,away[,date,league,sport_key]")
        sys.exit(2)

    aliases = read_aliases(args.aliases)
    alias_map = aliases.get("teams", {})

    if os.path.exists(args.league_map):
        with open(args.league_map, "r", encoding="utf-8") as f:
            league_map = json.load(f)
    else:
        league_map = {}

    # Normaliza nomes dos jogos
    matches["_home_norm"] = matches["home"].apply(lambda x: normalize_name(str(x), alias_map))
    matches["_away_norm"] = matches["away"].apply(lambda x: normalize_name(str(x), alias_map))

    # Define sport_key final por linha (prioriza CSV; senão tenta mapear pela league)
    def _pick_sport(row):
        s = str(row.get("sport_key", "")).strip()
        if s:
            return s
        lig = str(row.get("league","")).strip()
        mapped = league_to_sport(lig, league_map)
        return mapped

    matches["_sport"] = matches.apply(_pick_sport, axis=1)

    # Carrega eventos por sport_key único
    unique_sports = sorted({s for s in matches["_sport"].tolist() if s})
    sport_events_cache: Dict[str, List[dict]] = {}

    for sk in unique_sports:
        evs = fetch_sport_events(api_key, sk, args.regions)
        if evs:
            log(f"{sk} -> {len(evs)} eventos")
        else:
            warn(f"{sk} vazio/indisponível")
        sport_events_cache[sk] = evs
        time.sleep(0.35)  # rate polite

    rows = []

    # Lista de candidatos de fallback
    # Se o sport da linha está vazio ou retornou nada, tentaremos estes.
    fallback_pool = list(BRAZIL_SPORT_POOL)

    # Pré-indexa candidates por sport para matching rápido
    def build_candidates(evs: List[dict]) -> List[Tuple[str,str,dict]]:
        cands = []
        for ev in evs or []:
            # Cada evento tem 'home' e 'away'? TheOddsAPI usa 'teams' + 'home_team'
            teams = [unidecode(t).lower() for t in ev.get("teams", [])]
            hteam = unidecode(ev.get("home_team","")).lower()
            if len(teams) == 2 and hteam:
                # Definir mandante/visitante
                if teams[0] == hteam:
                    ateam = teams[1]
                else:
                    ateam = teams[0]
                cands.append((hteam, ateam, ev))
        return cands

    sport_candidates: Dict[str, List[Tuple[str,str,dict]]] = {
        sk: build_candidates(sport_events_cache.get(sk, []))
        for sk in unique_sports
    }

    # Se algum sport do CSV não retornou nada, prepare também o cache do fallback
    if any(len(sport_events_cache.get(sk, [])) == 0 for sk in unique_sports):
        for sk in fallback_pool:
            if sk in sport_events_cache:
                continue
            evs = fetch_sport_events(api_key, sk, args.regions)
            if evs:
                log(f"[fallback] {sk} -> {len(evs)} eventos")
            else:
                warn(f"[fallback] {sk} vazio/indisponível")
            sport_events_cache[sk] = evs
            sport_candidates[sk] = build_candidates(evs)
            time.sleep(0.35)

    # Função que tenta casar um match com um sport (e seus eventos)
    def match_on_sport(row, sk) -> Tuple[dict, float]:
        cands = sport_candidates.get(sk, [])
        if not cands:
            return (None, 0.0)
        target_h = row["_home_norm"]
        target_a = row["_away_norm"]
        # Melhor par pelo score médio home+away
        best_ev, best_score = None, 0.0
        for h, a, ev in cands:
            s_h = fuzz.token_set_ratio(target_h, h)
            s_a = fuzz.token_set_ratio(target_a, a)
            score = 0.5 * (s_h + s_a)
            if score > best_score:
                best_score = score
                best_ev = ev
        return (best_ev, best_score)

    MIN_SCORE = 70.0  # se abaixo, consideramos fraco

    for _, row in matches.iterrows():
        mid = row["match_id"]
        home = row["home"]
        away = row["away"]
        sk = row["_sport"]

        # 1) tenta no sport da linha (se houver)
        tried_sports = []
        chosen_ev, chosen_score, chosen_sk = None, 0.0, None

        if sk:
            ev, sc = match_on_sport(row, sk)
            tried_sports.append(sk)
            if ev is not None:
                chosen_ev, chosen_score, chosen_sk = ev, sc, sk

        # 2) se vazio/ruim, tenta fallback pool
        if (chosen_ev is None or chosen_score < MIN_SCORE):
            for fsk in fallback_pool:
                if fsk in tried_sports:
                    continue
                ev, sc = match_on_sport(row, fsk)
                tried_sports.append(fsk)
                if ev is not None and sc > chosen_score:
                    chosen_ev, chosen_score, chosen_sk = ev, sc, fsk
                if chosen_score >= MIN_SCORE:
                    break

        if chosen_ev is None:
            warn(f"{mid}: nenhum evento casado -> '{home}' vs '{away}'")
            continue

        if chosen_score < MIN_SCORE:
            warn(f"{mid}: matching fraco ({chosen_score}) - '{home}' x '{away}' no sport {chosen_sk}")

        # Extrai odds H2H (k1,kx,k2) dos bookies (pega melhor preço)
        best_home, best_draw, best_away = None, None, None

        for bk in chosen_ev.get("bookmakers", []):
            for mk in bk.get("markets", []):
                if mk.get("key") != ODDS_MARKET:
                    continue
                outcomes = mk.get("outcomes", [])
                # TheOddsAPI outcomes têm "name": "Home"/"Away"/"Draw" (varia por liga)
                price_home = next((o.get("price") for o in outcomes if unidecode(o.get("name","")).lower() in ["home","hometeam","home team", unidecode(home).lower()]), None)
                price_draw = next((o.get("price") for o in outcomes if unidecode(o.get("name","")).lower() in ["draw","empate","tie","x"]), None)
                price_away = next((o.get("price") for o in outcomes if unidecode(o.get("name","")).lower() in ["away","awayteam","away team", unidecode(away).lower()]), None)

                if price_home:
                    best_home = max(best_home or 0, float(price_home))
                if price_draw:
                    best_draw = max(best_draw or 0, float(price_draw))
                if price_away:
                    best_away = max(best_away or 0, float(price_away))

        if not (best_home and best_draw and best_away):
            warn(f"{mid}: odds H2H incompletas no evento casado (sport={chosen_sk}).")
            continue

        rows.append({
            "match_id": mid,
            "home": home,
            "away": away,
            "sport_key": chosen_sk,
            "match_score": round(chosen_score, 2),
            "k1": best_home,
            "kx": best_draw,
            "k2": best_away
        })

    df = pd.DataFrame(rows)
    if df.empty:
        warn(f"nenhum par de odds casou com os jogos (arquivo vazio salvo em {out_csv})")
        df = pd.DataFrame(columns=["match_id","home","away","sport_key","match_score","k1","kx","k2"])

    df.to_csv(out_csv, index=False, encoding="utf-8")
    log(f"OK -> {out_csv} ({len(df)} linhas)")


if __name__ == "__main__":
    main()
