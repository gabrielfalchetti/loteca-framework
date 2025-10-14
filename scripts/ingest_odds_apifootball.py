# scripts/ingest_odds_apifootball.py
# -*- coding: utf-8 -*-
"""
Ingesta "odds" via API-Football (na prática, usamos a API para padronizar nomes,
encontrar teams/fixtures e tentamos ler odds se o plano permitir).
Sempre gera `${rodada}/odds_apifootball.csv` com cabeçalho, mesmo sem linhas.

Uso:
  python -m scripts.ingest_odds_apifootball \
      --rodada data/out/1760XXXX \
      --source_csv data/out/1760XXXX/matches_norm.csv [--season 2025]

Requer:
  - API_FOOTBALL_KEY (API-Sports) OU X_RAPIDAPI_KEY (RapidAPI)
Colunas esperadas no source_csv:
  match_id,home,away
Saída (CSV):
  team_home,team_away,odds_home,odds_draw,odds_away,fixture_id,source
"""

import argparse
import csv
import os
import sys
import time
import re
import json
import unicodedata
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List, Tuple

import requests


# ============================== CONFIG HTTP ===============================

API_BASE_DIRECT = "https://v3.football.api-sports.io"
API_BASE_RAPID  = "https://api-football-v1.p.rapidapi.com/v3"

def get_http_config() -> Tuple[str, Dict[str, str]]:
    key_direct = os.getenv("API_FOOTBALL_KEY", "").strip()
    key_rapid  = os.getenv("X_RAPIDAPI_KEY", "").strip()

    if key_direct:
        return API_BASE_DIRECT, {"x-apisports-key": key_direct}
    if key_rapid:
        return API_BASE_RAPID, {
            "X-RapidAPI-Key": key_rapid,
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
        }
    return "", {}


def api_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    base, headers = get_http_config()
    if not base:
        raise RuntimeError("Sem chave (API_FOOTBALL_KEY ou X_RAPIDAPI_KEY).")
    url = f"{base}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# ============================== NORMALIZAÇÃO ==============================

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm_cmp(s: str) -> str:
    s = strip_accents(s).lower()
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)
    s = re.sub(r"[-_/\.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Aliases BR frequentes -> nome canônico que costuma "bater" na API
BR_ALIASES = {
    # Athletico e variantes
    "athletico-pr": "Athletico Paranaense",
    "atletico-pr": "Athletico Paranaense",
    "athletico paranaense": "Athletico Paranaense",
    "atletico paranaense": "Athletico Paranaense",

    # Atletico GO
    "atletico-go": "Atletico GO",
    "atlético-go": "Atletico GO",
    "atletico goianiense": "Atletico GO",

    # Avaí
    "avai": "Avai",
    "avaí": "Avai",
    "avai sc": "Avai",

    # Botafogo-SP
    "botafogo-sp": "Botafogo SP",
    "botafogo sp": "Botafogo SP",

    # Ferroviária (SP)
    "ferroviaria": "Ferroviaria",
    "ferroviária": "Ferroviaria",
    "ferroviaria sp": "Ferroviaria",

    # Chapecoense
    "chapecoense": "Chapecoense-SC",
    "chapecoense sc": "Chapecoense-SC",

    # Paysandu / Remo
    "paysandu": "Paysandu",
    "remo": "Remo",

    # Volta Redonda
    "volta redonda": "Volta Redonda",
}

def normalize_for_search(name: str) -> str:
    key = norm_cmp(name)
    if key in BR_ALIASES:
        return BR_ALIASES[key]
    # Remove UF entre parênteses ou sufixos " - UF"
    cleaned = re.sub(r"\s*[\-/]?\s*\b([A-Z]{2})\b", "", name).strip()
    cleaned = re.sub(r"\((?:[A-Z]{2})\)", "", cleaned).strip()
    # Tira acentos (a API costuma responder sem acento)
    return strip_accents(cleaned)


# ============================== BUSCAS NA API =============================

def token_match_score(a: str, b: str) -> float:
    sa = set(norm_cmp(a).split())
    sb = set(norm_cmp(b).split())
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    uni   = len(sa | sb)
    return inter / uni

def search_team_id(name: str, country_hint: Optional[str] = "Brazil") -> Optional[Dict[str, Any]]:
    q = normalize_for_search(name)
    params = {"search": q}
    if country_hint:
        params["country"] = country_hint

    try:
        data = api_get("/teams", params)
    except Exception:
        data = {}

    choices = data.get("response", []) or []
    best, best_score = None, 0.0
    for item in choices:
        t = (item.get("team") or {})
        cname = t.get("name") or ""
        score = token_match_score(q, cname)
        if score > best_score:
            best, best_score = item, score

    # fallback sem country
    if best_score < 0.5:
        try:
            data2 = api_get("/teams", {"search": q})
            for item in (data2.get("response", []) or []):
                t = (item.get("team") or {})
                cname = t.get("name") or ""
                score = token_match_score(q, cname)
                if score > best_score:
                    best, best_score = item, score
        except Exception:
            pass

    return best


def find_fixture_by_window(home_id: int, away_id: int, start_dt: datetime, end_dt: datetime, season: Optional[int]) -> Optional[Dict[str, Any]]:
    """Busca fixture futuro dentro de uma janela consultando por time."""
    from_s = start_dt.strftime("%Y-%m-%d")
    to_s   = end_dt.strftime("%Y-%m-%d")

    for tid in (home_id, away_id):
        params = {"team": tid, "from": from_s, "to": to_s}
        if season:
            params["season"] = season
        try:
            data = api_get("/fixtures", params)
        except Exception:
            data = {}
        for fx in data.get("response", []) or []:
            th = fx.get("teams", {}).get("home", {}).get("id")
            ta = fx.get("teams", {}).get("away", {}).get("id")
            if th == home_id and ta == away_id:
                return fx
    return None


def fixtures_headtohead_next(home_id: int, away_id: int, season: Optional[int], next_n: int = 12) -> Optional[Dict[str, Any]]:
    """Fallback forte: usa /fixtures/headtohead para próximos confrontos."""
    params = {"h2h": f"{home_id}-{away_id}", "next": next_n}
    if season:
        params["season"] = season
    try:
        data = api_get("/fixtures/headtohead", params)
        for fx in data.get("response", []) or []:
            th = fx.get("teams", {}).get("home", {}).get("id")
            ta = fx.get("teams", {}).get("away", {}).get("id")
            if th == home_id and ta == away_id:
                return fx
    except Exception:
        pass

    # tenta invertido
    params["h2h"] = f"{away_id}-{home_id}"
    try:
        data = api_get("/fixtures/headtohead", params)
        for fx in data.get("response", []) or []:
            th = fx.get("teams", {}).get("home", {}).get("id")
            ta = fx.get("teams", {}).get("away", {}).get("id")
            if th == home_id and ta == away_id:
                return fx
    except Exception:
        pass
    return None


def find_fixture_in_brazil_leagues(home_id: int, away_id: int, season: Optional[int], max_leagues: int = 12) -> Optional[Dict[str, Any]]:
    """Varre ligas brasileiras na season e tenta achar o confronto."""
    if not season:
        return None
    try:
        leagues = api_get("/leagues", {"country": "Brazil", "season": season}).get("response", []) or []
    except Exception:
        leagues = []

    leagues = leagues[:max_leagues]

    for lg in leagues:
        league_id = (lg.get("league") or {}).get("id")
        if not league_id:
            continue
        for tid in (home_id, away_id):
            try:
                data = api_get("/fixtures", {"league": league_id, "season": season, "team": tid, "next": 20})
            except Exception:
                continue
            for fx in data.get("response", []) or []:
                th = fx.get("teams", {}).get("home", {}).get("id")
                ta = fx.get("teams", {}).get("away", {}).get("id")
                if th == home_id and ta == away_id:
                    return fx
    return None


def fetch_odds_for_fixture(fixture_id: int) -> Optional[Tuple[float, float, float]]:
    """Tenta pegar odds (1X2) do fixture. Nem sempre disponível no plano."""
    try:
        data = api_get("/odds", {"fixture": fixture_id})
    except Exception:
        return None

    # Formato da API-Football: response -> bookmakers -> bets -> values
    resp = data.get("response", []) or []
    best = None  # (home, draw, away) com melhor cobertura
    for entry in resp:
        for bookmaker in entry.get("bookmakers", []) or []:
            for bet in bookmaker.get("bets", []) or []:
                if str(bet.get("name", "")).lower() in {"match winner","1x2","winner"}:
                    vals = bet.get("values", []) or []
                    home = draw = away = None
                    for v in vals:
                        label = str(v.get("value","")).strip().lower()
                        odd   = v.get("odd")
                        try:
                            odd = float(odd)
                        except Exception:
                            odd = None
                        if odd is None:
                            continue
                        if label in {"home","1","1 (home)","equipa 1"}:
                            home = odd
                        elif label in {"draw","x","empate"}:
                            draw = odd
                        elif label in {"away","2","2 (away)","equipa 2"}:
                            away = odd
                    if home and draw and away:
                        # escolhe a primeira tripla completa (ou poderíamos max/min)
                        return (home, draw, away)
    return best


# ============================== PIPELINE MAIN =============================

def read_matches(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        need = {"match_id","home","away"}
        if not need.issubset({c.strip() for c in rd.fieldnames or []}):
            raise RuntimeError(f"Arquivo {path} deve conter cabeçalho: match_id,home,away")
        for r in rd:
            if not r.get("home") or not r.get("away"):
                continue
            rows.append({"match_id": r.get("match_id","").strip(),
                         "home": r.get("home","").strip(),
                         "away": r.get("away","").strip()})
    return rows


def ensure_outfile_with_header(out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    # sempre cria com cabeçalho (evita "No columns to parse from file")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["team_home","team_away","odds_home","odds_draw","odds_away","fixture_id","source"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (onde salvar o CSV)")
    ap.add_argument("--source_csv", required=True, help="CSV com match_id,home,away")
    ap.add_argument("--season", type=int, default=None, help="Temporada (opcional, ex. 2025)")
    ap.add_argument("--lookahead_days", type=int, default=int(os.getenv("LOOKAHEAD_DAYS", "30")),
                    help="Janela curta de busca futura (dias)")
    args = ap.parse_args()

    out_file = os.path.join(args.rodada, "odds_apifootball.csv")
    ensure_outfile_with_header(out_file)

    base, headers = get_http_config()
    if not base:
        print("[apifootball][WARN] Sem chave para API-Football; arquivo será apenas o cabeçalho.")
        return 0

    matches = read_matches(args.source_csv)
    total = len(matches)
    print(f"[apifootball]Iniciando busca direcionada para {total} jogos do arquivo de origem.")

    now = datetime.utcnow()
    lookahead = max(1, int(args.lookahead_days))

    found_rows: List[List[Any]] = []

    for row in matches:
        home_raw = row["home"]
        away_raw = row["away"]

        # 1) resolve times
        th = search_team_id(home_raw, country_hint="Brazil")
        ta = search_team_id(away_raw, country_hint="Brazil")

        if not th:
            th = search_team_id(home_raw, country_hint=None)
        if not ta:
            ta = search_team_id(away_raw, country_hint=None)

        if not th or not ta:
            miss = []
            if not th: miss.append("team_id(home)")
            if not ta: miss.append("team_id(away)")
            print(f"[apifootball][WARN] Sem {', '.join(miss)} para: {home_raw} vs {away_raw}")
            continue

        home_id = (th.get("team") or {}).get("id")
        away_id = (ta.get("team") or {}).get("id")
        if not home_id or not away_id:
            print(f"[apifootball][WARN] Sem team_id para: {home_raw} vs {away_raw}")
            continue

        # 2) encontra fixture com cascata de fallbacks
        fx = find_fixture_by_window(home_id, away_id, now, now + timedelta(days=lookahead), args.season)
        if not fx:
            fx = find_fixture_by_window(home_id, away_id, now, now + timedelta(days=14), args.season)
        if not fx:
            fx = fixtures_headtohead_next(home_id, away_id, args.season, next_n=12)
        if not fx:
            fx = find_fixture_in_brazil_leagues(home_id, away_id, args.season, max_leagues=12)

        if not fx:
            print(f"[apifootball][WARN] Sem fixture_id para: {home_raw} vs {away_raw}")
            continue

        fixture_id = (fx.get("fixture") or {}).get("id")
        home_name  = ((fx.get("teams") or {}).get("home") or {}).get("name") or home_raw
        away_name  = ((fx.get("teams") or {}).get("away") or {}).get("name") or away_raw

        # 3) tenta odds (pode não retornar no plano)
        odds = fetch_odds_for_fixture(fixture_id) if fixture_id else None
        if odds:
            oh, od, oa = odds
            found_rows.append([home_name, away_name, oh, od, oa, fixture_id, "api-football"])
        else:
            # Não tem odds no plano — não vamos inventar linha; consenso trata a falta.
            # (Se preferir registrar linha com vazios, descomente abaixo)
            # found_rows.append([home_name, away_name, "", "", "", fixture_id, "api-football"])
            pass

        # proteção simples contra rate-limit
        time.sleep(0.2)

    # grava saída (cabeçalho já existe)
    if found_rows:
        with open(out_file, "a", encoding="utf-8", newline="") as f:
            wr = csv.writer(f)
            for r in found_rows:
                wr.writerow(r)
        print(f"[apifootball]Arquivo odds_apifootball.csv gerado com {len(found_rows)} jogos encontrados.")
    else:
        print("[apifootball]Arquivo odds_apifootball.csv gerado com 0 jogos encontrados.")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except requests.HTTPError as e:
        print(f"[apifootball][ERROR] HTTP {e.response.status_code} — {e}", file=sys.stderr)
        sys.exit(0)  # não quebrar o workflow; deixamos arquivo só com cabeçalho
    except Exception as e:
        print(f"[apifootball][ERROR] {e}", file=sys.stderr)
        sys.exit(0)  # idem