#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cliente fino para API-Football via RapidAPI.
Uso interno pelos scripts de ingestão. Falha "alto e claro" se a resposta vier vazia/erro.
"""

import os
import time
import requests
from typing import Dict, Any, Optional

BASE_URL = "https://api-football-v1.p.rapidapi.com/v3"

def _headers() -> Dict[str, str]:
    api_key = os.getenv("X_RAPIDAPI_KEY", "").strip()
    if not api_key:
        raise SystemExit("[apifoot] ERRO: X_RAPIDAPI_KEY ausente.")
    return {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }

def _get(path: str, params: Optional[Dict[str, Any]] = None, debug: bool = False) -> Dict[str, Any]:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    if debug:
        print(f"[apifoot][DEBUG] GET {url} params={params}")
    r = requests.get(url, headers=_headers(), params=params or {}, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"[apifoot] HTTP {r.status_code} em {url} body={r.text[:300]}")
    data = r.json()
    if "response" not in data:
        raise SystemExit(f"[apifoot] ERRO: payload inesperado: {data.keys()}")
    return data

def fixtures_by_date(date_str: str, league_id: int, season: int, debug: bool=False) -> Dict[str, Any]:
    return _get("fixtures", {"date": date_str, "league": league_id, "season": season}, debug)

def odds_by_fixture(fixture_id: int, debug: bool=False) -> Dict[str, Any]:
    return _get("odds", {"fixture": fixture_id}, debug)

def lineups_by_fixture(fixture_id: int, debug: bool=False) -> Dict[str, Any]:
    return _get("fixtures/lineups", {"fixture": fixture_id}, debug)

def injuries_by_date_league(date_str: str, league_id: int, season: int, debug: bool=False) -> Dict[str, Any]:
    return _get("injuries", {"date": date_str, "league": league_id, "season": season}, debug)

def h2h(team1_id: int, team2_id: int, debug: bool=False) -> Dict[str, Any]:
    return _get("fixtures/headtohead", {"h2h": f"{team1_id}-{team2_id}"}, debug)

def standings(league_id: int, season: int, debug: bool=False) -> Dict[str, Any]:
    return _get("standings", {"league": league_id, "season": season}, debug)

def teams_stats(league_id: int, season: int, team_id: int, debug: bool=False) -> Dict[str, Any]:
    return _get("teams/statistics", {"league": league_id, "season": season, "team": team_id}, debug)

def fixture_stats(fixture_id: int, debug: bool=False) -> Dict[str, Any]:
    return _get("fixtures/statistics", {"fixture": fixture_id}, debug)

def sleep_rl():
    # simples “throttle” para respeitar rate-limit
    time.sleep(0.35)