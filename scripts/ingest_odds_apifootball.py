#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestão (placeholder) via API-Football:
- Resolve times/fixtures a partir de nomes BR (com aliases + busca multi-termos + match por similaridade).
- Odds reais só aparecem se seu plano/liberação incluir /odds; aqui focamos em alimentar o consenso com teams/fixtures.
Saída: {rodada}/odds_apifootball.csv com colunas:
  team_home,team_away,odds_home,odds_draw,odds_away
(se odds não disponíveis, sairá apenas o cabeçalho)
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from unidecode import unidecode

API_BASE = "https://v3.football.api-sports.io"
API_KEY_ENV = "API_FOOTBALL_KEY"

# ---- aliases/sinônimos focados no BR ----
ALIAS_BASE = {
    # Sufixos de estado e variações frequentes
    "athletico-pr": ["athletico pr", "athletico paranaense", "club athletico paranaense"],
    "atletico-pr": ["athletico pr", "athletico paranaense", "club athletico paranaense"],
    "atlético-pr": ["athletico pr", "athletico paranaense", "club athletico paranaense"],

    "atletico-go": ["atletico go", "atletico goianiense", "atletico goiania", "atletico goi"],
    "atlético-go": ["atletico go", "atletico goianiense", "atletico goiania", "atletico goi"],

    "botafogo-sp": ["botafogo sp", "botafogo ribeirao preto", "botafogo ribeirão preto"],
    "chapecoense": ["chapecoense", "chapecoense sc"],
    "chapecoense-sc": ["chapecoense", "chapecoense sc"],
    "avai": ["avai", "avai fc", "ec avai"],
    "avaí": ["avai", "avai fc", "ec avai"],

    "volta redonda": ["volta redonda", "volta redonda fc"],
    "crb": ["crb", "clube de regatas brasil", "crb maceio", "crb maceió"],

    "ferroviaria": ["ferroviaria", "ferroviaria sp", "afe", "associacao ferroviaria de esportes"],
    "ferroviária": ["ferroviaria", "ferroviaria sp", "afe", "associacao ferroviaria de esportes"],

    "paysandu": ["paysandu", "paysandu sport club"],
    "remo": ["remo", "clube do remo"],

    # grafias sem acento/sinais
    "atletico goianiense": ["atletico go", "atletico-go", "atletico goiania"],
    "athletico paranaense": ["athletico pr", "athletico-pr", "cap", "club athletico paranaense"],
    "botafogo ribeirao preto": ["botafogo sp", "botafogo-sp"],
}

STATE_SUFFIXES = {"-sp", " sp", "(sp)", " sp)", "(sp", "/sp",
                  "-rj", " rj", "(rj)", "(rj", "/rj",
                  "-pr", " pr", "(pr)", "(pr", "/pr",
                  "-sc", " sc", "(sc)", "(sc", "/sc",
                  "-go", " go", "(go)", "(go", "/go",
                  "-pa", " pa", "(pa)", "(pa", "/pa"}

def _clean(s: str) -> str:
    if not s:
        return ""
    s = unidecode(s).lower()
    s = s.replace(" - ", "-")
    s = re.sub(r"[^\w\s\-/()]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def generate_search_terms(name: str) -> List[str]:
    """Gera uma lista de termos de busca progressivamente mais frouxos."""
    base = _clean(name)
    terms = {base}

    # troca separadores
    terms.add(base.replace("-", " "))
    terms.add(base.replace("/", " "))
    terms.add(base.replace("(", " ").replace(")", " "))

    # remove sufixos de estado
    for suff in STATE_SUFFIXES:
        if base.endswith(suff):
            terms.add(base[: -len(suff)].strip())

    # aplica aliases conhecidos
    if base in ALIAS_BASE:
        for alt in ALIAS_BASE[base]:
            t = _clean(alt)
            terms.add(t)
            terms.add(t.replace("-", " "))
            terms.add(t.replace("/", " "))

    # fallback: primeira palavra (ex.: “botafogo-sp” → “botafogo”)
    parts = base.replace("-", " ").split()
    if parts:
        terms.add(parts[0])

    return [t for t in terms if t]

def token_set(s: str) -> set:
    s = _clean(s)
    toks = set(s.replace("-", " ").split())
    # remove tokens muito genéricos
    return {t for t in toks if t not in {"fc", "ec", "clube", "club", "do", "de", "sc"}}

def jaccard(a: str, b: str) -> float:
    A, B = token_set(a), token_set(b)
    if not A or not B:
        return 0.0
    inter = len(A & B)
    uni = len(A | B)
    return inter / uni if uni else 0.0

def api_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    key = os.environ.get(API_KEY_ENV, "").strip()
    if not key:
        raise SystemExit("[apifootball][CRITICAL] API_FOOTBALL_KEY ausente no ambiente.")
    headers = {"x-apisports-key": key, "Accept": "application/json"}
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=25)
    if r.status_code == 429:
        time.sleep(2.5)
        r = requests.get(url, headers=headers, params=params, timeout=25)
    r.raise_for_status()
    return r.json()

def find_team_id_by_name(raw_name: str) -> Optional[int]:
    """Tenta várias queries e escolhe o melhor match por similaridade de tokens."""
    queries = generate_search_terms(raw_name)
    best_id, best_score, best_name = None, 0.0, None

    for q in queries:
        try:
            js = api_get("/teams", {"search": q})
        except Exception:
            continue
        for it in js.get("response", []):
            api_name = (it.get("team", {}) or {}).get("name") or ""
            score = jaccard(api_name, raw_name)
            if score > best_score:
                best_score = score
                best_id = (it.get("team", {}) or {}).get("id")
                best_name = api_name

    # aceita se a similaridade for minimamente convincente
    if best_id and best_score >= 0.45:
        # print(f"[apifootball][DEBUG] '{raw_name}' -> '{best_name}' (score={best_score:.2f})")
        return best_id
    return None

def fixtures_next_by_team(team_id: int, season: Optional[int]) -> List[Dict[str, Any]]:
    params = {"team": team_id, "next": 20}
    if season:
        params["season"] = season
    try:
        data = api_get("/fixtures", params)
        return data.get("response", []) or []
    except Exception:
        return []

def find_fixture_by_window(home_id: int, away_id: int, since: datetime, until: datetime, season: Optional[int]) -> Optional[Dict[str, Any]]:
    params = {
        "from": since.strftime("%Y-%m-%d"),
        "to": until.strftime("%Y-%m-%d"),
        "team": home_id,
    }
    if season:
        params["season"] = season
    try:
        data = api_get("/fixtures", params)
        for fx in data.get("response", []):
            th = fx.get("teams", {}).get("home", {}).get("id")
            ta = fx.get("teams", {}).get("away", {}).get("id")
            if th == home_id and ta == away_id:
                return fx
    except Exception:
        pass
    return None

def find_fixture_h2h(home_id: int, away_id: int, season: Optional[int]) -> Optional[Dict[str, Any]]:
    for tid in (home_id, away_id):
        for fx in fixtures_next_by_team(tid, season):
            th = fx.get("teams", {}).get("home", {}).get("id")
            ta = fx.get("teams", {}).get("away", {}).get("id")
            if th == home_id and ta == away_id:
                return fx
    return None

def read_matches_csv(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        hdr = [c.strip().lower() for c in (reader.fieldnames or [])]
        col_home = "home" if "home" in hdr else "team_home" if "team_home" in hdr else None
        col_away = "away" if "away" in hdr else "team_away" if "team_away" in hdr else None
        col_id = "match_id" if "match_id" in hdr else None
        if not (col_home and col_away and col_id):
            raise SystemExit(f"[apifootball][CRITICAL] CSV {path} precisa de colunas match_id,home,away (ou team_home/team_away).")
        for r in reader:
            rows.append({"match_id": r.get(col_id, "").strip(),
                         "home": r.get(col_home, "").strip(),
                         "away": r.get(col_away, "").strip()})
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída (ex: data/out/12345)")
    ap.add_argument("--source_csv", required=True, help="CSV com match_id,home,away (normalizado)")
    ap.add_argument("--season", type=int, default=int(os.environ.get("SEASON", "0")) or None)
    args = ap.parse_args()

    os.makedirs(args.rodada, exist_ok=True)
    out_csv = os.path.join(args.rodada, "odds_apifootball.csv")

    matches = read_matches_csv(args.source_csv)
    print(f"[apifootball]Iniciando busca direcionada para {len(matches)} jogos do arquivo de origem.")

    now = datetime.utcnow()
    lookahead = int(os.environ.get("LOOKAHEAD_DAYS", "3") or 3)

    # acumulador (se houver odds no seu plano)
    rows_out: List[Dict[str, Any]] = []

    for m in matches:
        home_raw, away_raw = m["home"], m["away"]

        home_id = find_team_id_by_name(home_raw)
        away_id = find_team_id_by_name(away_raw)
        if not home_id or not away_id:
            print(f"[apifootball][WARN] Sem team_id para: {home_raw} vs {away_raw}")
            continue

        # tenta janelas diferentes
        fx = find_fixture_by_window(home_id, away_id, now, now + timedelta(days=lookahead), args.season)
        if not fx:
            fx = find_fixture_by_window(home_id, away_id, now, now + timedelta(days=14), args.season)
        if not fx:
            fx = find_fixture_h2h(home_id, away_id, args.season)

        if not fx:
            print(f"[apifootball][WARN] Sem fixture_id para: {home_raw} vs {away_raw}")
            continue

        # Odds via API-Football dependem do plano; mantemos CSV com cabeçalho.
        # Exemplo de coleta (desativado por padrão):
        # fixture_id = fx.get("fixture", {}).get("id")
        # try:
        #     o = api_get("/odds", {"fixture": fixture_id})
        #     # parse e adicionar a rows_out aqui...
        # except Exception:
        #     pass

    # Sempre grava cabeçalho (mesmo com 0 linhas)
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["team_home", "team_away", "odds_home", "odds_draw", "odds_away"])
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    print(f"[apifootball]Arquivo odds_apifootball.csv gerado com {len(rows_out)} jogos encontrados.")

if __name__ == "__main__":
    main()