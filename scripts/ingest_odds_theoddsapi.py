#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_theoddsapi.py
Coleta odds H2H no TheOddsAPI com:
- Fallback de sport_key para Brasil (A/B/C/D) quando necessário;
- Matching robusto de times: normaliza (unidecode, lower), remove EC/FC/“futebol clube”,
  pontuação e hífens; tenta (home,away) e também invertido; usa fuzzy score mais permissivo;
- Exporta k1 (mandante), kx (empate), k2 (visitante).

Saída: data/out/<rodada>/odds_theoddsapi.csv
"""

import os
import sys
import json
import argparse
import time
import re
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
BRAZIL_SPORT_POOL = [
    "soccer_brazil_serie_b",
    "soccer_brazil_serie_a",
    "soccer_brazil_serie_c",
    "soccer_brazil_serie_d"
    # "soccer_brazil_cup"  # inclua se seu plano expõe Copa do Brasil
]

CLEAN_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)

def log(msg: str): print(f"[theoddsapi] {msg}", flush=True)
def warn(msg: str): print(f"[theoddsapi] AVISO {msg}", flush=True)
def err(msg: str): print(f"[theoddsapi] ERRO {msg}", flush=True)

def read_json(path: str, default):
    if not os.path.exists(path): return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_team(name: str) -> str:
    if not name: return ""
    s = unidecode(str(name)).lower().strip()
    # remove pontuação
    s = CLEAN_RE.sub(" ", s)
    # remove sufixos/comuns de clubes
    tokens = [t for t in s.split() if t not in {
        "ec","fc","sc","ac","esporte","sport","clube","clube","futebol","futbol","clube","de",
        "associacao","association","athletico","atletico"  # mantemos "atletico" às vezes, mas OK
    }]
    s = " ".join(tokens)
    # normalizações específicas rápidas
    s = s.replace("america mg","america mineiro")
    s = s.replace("avai","avai")
    s = s.replace("ceara","ceara")
    s = s.replace("volta redonda","volta redonda")
    s = s.replace("coritiba","coritiba")
    s = s.replace("amazonas","amazonas")
    # espreme espaços
    s = re.sub(r"\s+", " ", s).strip()
    return s

def apply_alias(name: str, alias_map: Dict[str, str]) -> str:
    if not name: return ""
    keyspace = {unidecode(k).lower(): v for k, v in alias_map.items()}
    k = unidecode(name).lower()
    return keyspace.get(k, name)

def best_pair_score(target_h: str, target_a: str, cand_h: str, cand_a: str) -> float:
    # score médio considerando também possibilidade de inversão
    s1 = 0.5 * (fuzz.token_set_ratio(target_h, cand_h) + fuzz.token_set_ratio(target_a, cand_a))
    s2 = 0.5 * (fuzz.token_set_ratio(target_h, cand_a) + fuzz.token_set_ratio(target_a, cand_h))
    return max(float(s1), float(s2))

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

def build_candidates(evs: List[dict]) -> List[Tuple[str,str,dict]]:
    cands = []
    for ev in evs or []:
        teams = ev.get("teams", [])
        hteam = ev.get("home_team","")
        if len(teams) != 2 or not hteam:
            continue
        t_norm = [normalize_team(t) for t in teams]
        h_norm = normalize_team(hteam)
        # descobrir away norm a partir do vetor
        if normalize_team(teams[0]) == h_norm:
            a_norm = normalize_team(teams[1])
        else:
            a_norm = normalize_team(teams[0])
        cands.append((h_norm, a_norm, ev))
    return cands

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

    aliases = read_json(args.aliases, {"teams": {}})
    alias_map = aliases.get("teams", {})

    league_map = read_json(args.league_map, {})

    matches = pd.read_csv(in_csv)
    required = {"match_id","home","away"}
    if not required.issubset(set(matches.columns)):
        err("matches_source.csv precisa de colunas: match_id,home,away[,date,league,sport_key]")
        sys.exit(2)

    # Aplica aliases e normaliza
    matches["home_std"] = matches["home"].apply(lambda x: apply_alias(str(x), alias_map))
    matches["away_std"] = matches["away"].apply(lambda x: apply_alias(str(x), alias_map))
    matches["_home_norm"] = matches["home_std"].apply(normalize_team)
    matches["_away_norm"] = matches["away_std"].apply(normalize_team)

    # Escolhe sport da linha (se houver), senão tenta inferir por league_map
    def pick_sport(row):
        sk = str(row.get("sport_key","") or "").strip()
        if sk: return sk
        league = unidecode(str(row.get("league",""))).lower().strip()
        # tenta achar o sport_key por aliases de liga
        for _, node in league_map.items():
            skey = node.get("sport_key","")
            aliases = [unidecode(a).lower() for a in node.get("aliases",[])]
            if league in aliases or league == skey:
                return skey
        return ""

    matches["_sport"] = matches.apply(pick_sport, axis=1)

    # Esport(es) a consultar
    unique_sports = sorted({s for s in matches["_sport"].tolist() if s})
    if not unique_sports:
        # se nada informado, comece pelo pool Brasil
        unique_sports = list(BRAZIL_SPORT_POOL)

    sport_events_cache: Dict[str,List[dict]] = {}
    sport_candidates: Dict[str,List[Tuple[str,str,dict]]] = {}

    for sk in unique_sports:
        evs = fetch_sport_events(api_key, sk, args.regions)
        if not evs:
            warn(f"{sk} vazio/indisponível")
        else:
            log(f"{sk} -> {len(evs)} eventos")
        sport_events_cache[sk] = evs
        sport_candidates[sk] = build_candidates(evs)
        time.sleep(0.35)

    # Prepara fallback caso algum sport venha vazio
    if any(len(v)==0 for v in sport_events_cache.values()):
        for fsk in BRAZIL_SPORT_POOL:
            if fsk in sport_events_cache: 
                continue
            evs = fetch_sport_events(api_key, fsk, args.regions)
            if not evs:
                warn(f"[fallback] {fsk} vazio/indisponível")
            else:
                log(f"[fallback] {fsk} -> {len(evs)} eventos")
            sport_events_cache[fsk] = evs
            sport_candidates[fsk] = build_candidates(evs)
            time.sleep(0.35)

    MIN_SCORE = 55.0  # ficou mais permissivo

    rows = []
    for _, row in matches.iterrows():
        mid = row["match_id"]
        home = row["home_std"]
        away = row["away_std"]
        hn = row["_home_norm"]
        an = row["_away_norm"]

        # lista de sports a tentar: 1) da linha (se houver), 2) todos os demais do pool
        sports_to_try = []
        if row["_sport"]:
            sports_to_try.append(row["_sport"])
        for s in BRAZIL_SPORT_POOL:
            if s not in sports_to_try:
                sports_to_try.append(s)

        best_ev, best_sc, best_sk = None, 0.0, None
        for sk in sports_to_try:
            cands = sport_candidates.get(sk, [])
            if not cands: 
                continue
            for ch, ca, ev in cands:
                sc = best_pair_score(hn, an, ch, ca)
                if sc > best_sc:
                    best_sc, best_ev, best_sk = sc, ev, sk
            if best_sc >= MIN_SCORE:
                break

        if best_ev is None:
            warn(f"{mid}: nenhum evento casado -> '{home}' vs '{away}'")
            continue

        if best_sc < MIN_SCORE:
            warn(f"{mid}: matching fraco ({best_sc}) - '{home}' x '{away}' (sport={best_sk})")

        # Extrai odds (melhor preço entre bookmakers)
        best_home, best_draw, best_away = None, None, None
        for bk in best_ev.get("bookmakers", []):
            for mk in bk.get("markets", []):
                if mk.get("key") != ODDS_MARKET: 
                    continue
                for o in mk.get("outcomes", []):
                    nm = unidecode(o.get("name","")).lower().strip()
                    price = o.get("price")
                    if price is None: 
                        continue
                    if nm in {"home","home team","hometeam"} or fuzz.partial_ratio(nm, normalize_team(home)) >= 80:
                        best_home = max(best_home or 0.0, float(price))
                    elif nm in {"away","away team","awayteam"} or fuzz.partial_ratio(nm, normalize_team(away)) >= 80:
                        best_away = max(best_away or 0.0, float(price))
                    elif nm in {"draw","empate","tie","x"}:
                        best_draw = max(best_draw or 0.0, float(price))

        if not (best_home and best_draw and best_away):
            warn(f"{mid}: odds H2H incompletas no evento casado (sport={best_sk}).")
            continue

        rows.append({
            "match_id": mid,
            "home": home,
            "away": away,
            "sport_key": best_sk,
            "match_score": round(best_sc, 2),
            "k1": best_home,
            "kx": best_draw,
            "k2": best_away
        })

    df = pd.DataFrame(rows, columns=["match_id","home","away","sport_key","match_score","k1","kx","k2"])
    if df.empty:
        warn(f"nenhum par de odds casou com os jogos (arquivo vazio salvo em {out_csv})")
    df.to_csv(out_csv, index=False, encoding="utf-8")
    log(f"OK -> {out_csv} ({len(df)} linhas)")

if __name__ == "__main__":
    main()