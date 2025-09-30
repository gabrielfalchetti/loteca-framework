# utils/apifootball.py
from __future__ import annotations
import os, requests, unicodedata, re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

API_HOST = "api-football-v1.p.rapidapi.com"

def _get_env(*names: str) -> str:
    for n in names:
        v = os.environ.get(n)
        if v and v.strip():
            return v.strip()
    return ""

# Aceita RAPIDAPI_KEY ou RAPID_API_KEY
API_KEY  = _get_env("RAPIDAPI_KEY", "RAPID_API_KEY")

class ApiFootballError(RuntimeError):
    pass

def _normalize(s: str) -> str:
    s2 = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s2 = re.sub(r"[^a-zA-Z0-9 ]+"," ", s2).lower()
    s2 = re.sub(r"\b(ec|fc|afc|sc|ac|esporte clube|futebol clube)\b", "", s2)
    return " ".join(s2.split())

def _get(path: str, params: Dict[str, Any]) -> Any:
    if not API_KEY:
        raise ApiFootballError("RAPIDAPI_KEY/RAPID_API_KEY ausente no ambiente.")
    url = f"https://{API_HOST}/v3{path}"
    headers = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": API_HOST}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 429:
        raise ApiFootballError("RapidAPI rate limited (429).")
    r.raise_for_status()
    payload = r.json()
    if payload.get("errors"):
        raise ApiFootballError(f"API-Football error: {payload['errors']}")
    return payload.get("response", [])

def resolve_league_id(country: str = "Brazil", league_name: str = "Serie A") -> int:
    leagues = _get("/leagues", {"country": country})
    target = _normalize(league_name)
    for item in leagues:
        nm = _normalize(item["league"]["name"])
        if target in nm or nm in target:
            return int(item["league"]["id"])
    for item in leagues:
        nm = _normalize(item["league"]["name"])
        if any(k in nm for k in ("brasileirao","serie a","serie b","serie c","serie d")) and target.split()[-1] in nm:
            return int(item["league"]["id"])
    raise ApiFootballError(f"Liga não encontrada: {country}/{league_name}")

def resolve_current_season(league_id: int) -> int:
    leagues = _get("/leagues", {"id": league_id})
    if not leagues:
        raise ApiFootballError(f"Liga {league_id} não encontrada para season.")
    seasons = leagues[0].get("seasons", [])
    for s in seasons:
        if s.get("current"):
            return int(s["year"])
    return int(seasons[-1]["year"])

def find_fixture_id(date_iso: str, home: str, away: str, league_id: int, season: int, window: int = 1) -> Optional[int]:
    dt0 = datetime.fromisoformat(date_iso).date()
    for delta in range(-abs(window), abs(window)+1):
        d = (dt0 + timedelta(days=delta)).isoformat()
        fixtures = _get("/fixtures", {"date": d, "league": league_id, "season": season})
        hkey = _normalize(home); akey = _normalize(away)
        for f in fixtures:
            th = _normalize(f["teams"]["home"]["name"])
            ta = _normalize(f["teams"]["away"]["name"])
            if th == hkey and ta == akey:
                return int(f["fixture"]["id"])
    return None

def fetch_odds_by_fixture(fixture_id: int) -> List[Dict[str, Any]]:
    return _get("/odds", {"fixture": fixture_id})
