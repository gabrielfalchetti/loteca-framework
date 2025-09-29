# utils/oddsapi.py
from __future__ import annotations
import os, requests, unicodedata, time
from typing import Dict, List, Any, Iterable

ODDSAPI_BASE = "https://api.the-odds-api.com/v4"
ODDSAPI_KEY = os.environ.get("THEODDSAPI_KEY", "").strip()

class OddsApiError(RuntimeError):
    pass

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().replace("&"," and ")
    for token in (" ec"," fc"," afc"," sc"," ac"," esporte clube"," futebol clube"):
        s = s.replace(token,"")
    return " ".join(s.split())

def fetch_sports(active_only: bool=False) -> List[Dict[str, Any]]:
    if not ODDSAPI_KEY:
        raise OddsApiError("THEODDSAPI_KEY ausente no ambiente.")
    r = requests.get(f"{ODDSAPI_BASE}/sports",
                     params={"apiKey": ODDSAPI_KEY, "all": "false" if active_only else "true"},
                     timeout=25)
    if r.status_code == 401:
        raise OddsApiError("TheOddsAPI 401 (chave inválida/plano).")
    r.raise_for_status()
    return r.json()

def resolve_brazil_soccer_sport_keys() -> List[str]:
    sports = fetch_sports(active_only=False)
    keys = []
    for s in sports:
        title = _norm(f'{s.get("title","")} {s.get("description","")} {s.get("group","")}')
        if "soccer" in title and ("brazil" in title or "brasil" in title):
            keys.append(s["key"])
    if not keys:
        keys = ["soccer_brazil_serie_a","soccer_brazil_serie_b","soccer_brazil_serie_c","soccer_brazil_serie_d"]
    return list(dict.fromkeys(keys))

def fetch_odds_for_sport(sport_key: str, regions: Iterable[str]) -> List[Dict[str, Any]]:
    if not ODDSAPI_KEY:
        raise OddsApiError("THEODDSAPI_KEY ausente no ambiente.")
    params = {"apiKey": ODDSAPI_KEY, "regions": ",".join(regions),
              "markets": "h2h,totals", "oddsFormat": "decimal"}
    url = f"{ODDSAPI_BASE}/sports/{sport_key}/odds"
    r = requests.get(url, params=params, timeout=30)
    remaining = r.headers.get("x-requests-remaining")
    used = r.headers.get("x-requests-used")
    if remaining is not None:
        print(f"[theoddsapi] quota remaining={remaining}, used={used}")
    if r.status_code == 404:
        print(f"[theoddsapi] AVISO {sport_key}: 404 UNKNOWN_SPORT")
        return []
    if r.status_code == 429:
        print("[theoddsapi] AVISO: rate limited (429). Aguardando 5s…")
        time.sleep(5)
        r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()
