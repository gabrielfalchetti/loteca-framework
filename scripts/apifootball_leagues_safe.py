# scripts/apifootball_leagues_safe.py
# -*- coding: utf-8 -*-
"""
Coleta segura da lista de ligas/competições do API-FOOTBALL (API-Sports),
com suporte a acesso direto (API_FOOTBALL_KEY) ou via RapidAPI (X_RAPIDAPI_KEY),
filtros e gravação no diretório da rodada.

Uso:
  python scripts/apifootball_leagues_safe.py --rodada data/out/123456789 \
      [--country Brazil] [--season 2025] [--active-only] [--use-direct] [--debug]

Ou:
  python scripts/apifootball_leagues_safe.py --out-dir data/out/123456789 --debug
"""

from __future__ import annotations
import os
import sys
import csv
import time
import argparse
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd


# ----------------------------
# Utilidades de impressão/log
# ----------------------------
def log(msg: str) -> None:
    print(f"[leagues] {msg}")

def dbg(msg: str, enabled: bool) -> None:
    if enabled:
        print(f"[leagues][DEBUG] {msg}")

def warn(msg: str) -> None:
    print(f"::warning::[leagues] {msg}")

def fail(msg: str, code: int = 1) -> None:
    print(f"::error::[leagues] {msg}")
    sys.exit(code)


# ----------------------------
# HTTP com retry/timeout
# ----------------------------
def build_headers(use_direct: bool, api_key: Optional[str], rapid_key: Optional[str]) -> Tuple[str, Dict[str, str]]:
    """
    Retorna (base_url, headers) para chamada.
    - Direto: https://v3.football.api-sports.io
      Header: {'x-apisports-key': API_FOOTBALL_KEY}
    - Rapid:  https://api-football-v1.p.rapidapi.com
      Header: {'X-RapidAPI-Key': X_RAPIDAPI_KEY, 'X-RapidAPI-Host': 'api-football-v1.p.rapidapi.com'}
    """
    if use_direct:
        if not api_key:
            fail("API_FOOTBALL_KEY ausente para acesso direto. Informe --api-key ou defina env API_FOOTBALL_KEY.", 2)
        return (
            "https://v3.football.api-sports.io",
            {"x-apisports-key": api_key}
        )
    else:
        if not rapid_key:
            fail("X_RAPIDAPI_KEY ausente para acesso RapidAPI. Informe --api-key ou defina env X_RAPIDAPI_KEY.", 2)
        return (
            "https://api-football-v1.p.rapidapi.com",
            {"X-RapidAPI-Key": rapid_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
        )

def http_get_json(url: str, headers: Dict[str, str], params: Dict[str, Any],
                  retries: int = 3, backoff: float = 1.5, timeout: float = 25.0,
                  debug: bool = False) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            if debug:
                dbg(f"GET {url} params={params}", True)
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            warn(f"Falha GET (tentativa {attempt}/{retries}): {e}. Retentando em {backoff:.1f}s...")
            time.sleep(backoff)
            backoff *= 1.7
    fail(f"Erro de rede ao consultar {url}: {last_err}", 6)
    raise RuntimeError("Unreachable")

# ----------------------------
# Parsing do payload / normalização
# ----------------------------
def flatten_leagues(payload: Dict[str, Any], debug: bool = False) -> List[Dict[str, Any]]:
    """
    Converte o payload /v3/leagues em linhas (uma por season).
    """
    data = payload.get("response", []) or []
    rows: List[Dict[str, Any]] = []

    for item in data:
        league = item.get("league") or {}
        country = item.get("country") or {}
        seasons = item.get("seasons") or []

        league_id = league.get("id")
        league_name = league.get("name")
        league_type = league.get("type")  # League/Cup
        league_logo = league.get("logo")

        country_name = country.get("name")
        country_code = country.get("code")
        country_flag = country.get("flag")

        if not seasons:
            # Ainda assim grava uma linha "sem season" para referência
            rows.append({
                "league_id": league_id,
                "league_name": league_name,
                "league_type": league_type,
                "country": country_name,
                "country_code": country_code,
                "season": None,
                "season_start": None,
                "season_end": None,
                "current": None,
                "logo": league_logo,
                "flag": country_flag,
                "coverage_standings": None,
                "coverage_players": None,
                "coverage_top_scorers": None,
                "coverage_predictions": None,
                "coverage_odds": None,
            })
            continue

        for s in seasons:
            coverage = (s.get("coverage") or {})
            rows.append({
                "league_id": league_id,
                "league_name": league_name,
                "league_type": league_type,
                "country": country_name,
                "country_code": country_code,
                "season": s.get("year"),
                "season_start": s.get("start"),
                "season_end": s.get("end"),
                "current": bool(s.get("current")),
                "logo": league_logo,
                "flag": country_flag,
                "coverage_standings": bool(coverage.get("standings")) if isinstance(coverage.get("standings"), (bool, type(None))) else None,
                "coverage_players": bool(coverage.get("players")) if isinstance(coverage.get("players"), (bool, type(None))) else None,
                "coverage_top_scorers": bool(coverage.get("top_scorers")) if isinstance(coverage.get("top_scorers"), (bool, type(None))) else None,
                "coverage_predictions": bool(coverage.get("predictions")) if isinstance(coverage.get("predictions"), (bool, type(None))) else None,
                "coverage_odds": bool(coverage.get("odds")) if isinstance(coverage.get("odds"), (bool, type(None))) else None,
            })

    if debug:
        dbg(f"flatten_leagues: {len(rows)} linhas", True)
    return rows

# ----------------------------
# Coleta principal
# ----------------------------
def fetch_leagues(
    use_direct: bool,
    api_key_direct: Optional[str],
    api_key_rapid: Optional[str],
    country: Optional[str],
    season: Optional[int],
    active_only: bool,
    debug: bool,
) -> pd.DataFrame:
    base, headers = build_headers(use_direct, api_key_direct, api_key_rapid)
    url = f"{base}/v3/leagues" if base.endswith(".com") else f"{base}/leagues"  # ambos formam .../v3/leagues

    params: Dict[str, Any] = {}
    if country:
        params["country"] = country
    if season:
        params["season"] = int(season)
    # active_only: manter apenas temporadas current=true (filtro pós-resposta)
    payload = http_get_json(url, headers, params, retries=4, backoff=1.2, timeout=25.0, debug=debug)
    rows = flatten_leagues(payload, debug=debug)

    df = pd.DataFrame(rows)
    if active_only:
        if "current" in df.columns:
            df = df[df["current"] == True].copy()  # noqa: E712
        else:
            warn("Campo 'current' ausente, não foi possível filtrar active-only.")

    # Ordenação amigável
    order_cols = [
        "country", "league_name", "season", "league_id", "league_type",
        "season_start", "season_end", "current", "coverage_standings",
        "coverage_players", "coverage_top_scorers", "coverage_predictions",
        "coverage_odds", "country_code", "logo", "flag"
    ]
    df = df.reindex(columns=[c for c in order_cols if c in df.columns] + [c for c in df.columns if c not in order_cols])
    return df


# ----------------------------
# CLI
# ----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Coleta de ligas do API-FOOTBALL (segura).")
    g_out = p.add_mutually_exclusive_group(required=True)
    g_out.add_argument("--rodada", dest="rodada", help="Diretório de rodada (ex.: data/out/1699999999)")
    g_out.add_argument("--out-dir", dest="out_dir", help="Diretório de saída (equivalente a --rodada)")

    p.add_argument("--country", help="Filtro por país (ex.: Brazil, England...)")
    p.add_argument("--season", type=int, help="Filtro por temporada (ex.: 2025)")
    p.add_argument("--active-only", action="store_true", help="Mantém apenas seasons current=true")
    p.add_argument("--use-direct", action="store_true",
                   help="Força uso do host direto API-Sports (requer API_FOOTBALL_KEY). Por padrão decide automaticamente.")
    p.add_argument("--api-key", help="Força chave (usa como x-apisports-key se --use-direct, senão como X-RapidAPI-Key).")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = args.rodada or args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    # Detecção de chave e modo
    env_direct = os.getenv("API_FOOTBALL_KEY")
    env_rapid = os.getenv("X_RAPIDAPI_KEY")

    # Se --api-key foi passado, injeta no modo correspondente
    api_key_direct = env_direct
    api_key_rapid = env_rapid

    # Heurística de modo:
    # 1) Se --use-direct, tenta direto com API_FOOTBALL_KEY (ou --api-key).
    # 2) Caso contrário, usa direto se API_FOOTBALL_KEY existir, senão RapidAPI.
    use_direct = args.use_direct or bool(env_direct)

    if args.api_key:
        if use_direct:
            api_key_direct = args.api_key
        else:
            api_key_rapid = args.api_key

    dbg(f"modo={'direto' if use_direct else 'rapidapi'}", args.debug)

    df = fetch_leagues(
        use_direct=use_direct,
        api_key_direct=api_key_direct,
        api_key_rapid=api_key_rapid,
        country=args.country,
        season=args.season,
        active_only=args.active_only,
        debug=args.debug,
    )

    out_file = os.path.join(out_dir, "apifootball_leagues.csv")
    if df.empty:
        warn("Nenhuma liga retornada com os filtros atuais. Criando CSV vazio com cabeçalho.")
        cols = ["country","league_name","season","league_id","league_type","season_start","season_end","current",
                "coverage_standings","coverage_players","coverage_top_scorers","coverage_predictions","coverage_odds",
                "country_code","logo","flag"]
        pd.DataFrame(columns=cols).to_csv(out_file, index=False, quoting=csv.QUOTE_MINIMAL)
        # Mesmo vazio, sair com código 0 — step opcional no workflow
        print("source,rows,out_file")
        print(f"api-football,0,{out_file}")
        return

    # Grava
    df.to_csv(out_file, index=False, quoting=csv.QUOTE_MINIMAL)

    # Log/resumo
    log(f"OK -> {out_file} (linhas={len(df)})")
    if args.debug:
        try:
            print(df.head(10).to_string(index=False))
        except Exception:
            pass

    # Emite “tabela” simples para facilitar debug no Actions
    print("source,rows,out_file")
    print(f"api-football,{len(df)},{out_file}")


if __name__ == "__main__":
    main()