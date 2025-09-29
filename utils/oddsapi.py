# utils/oddsapi.py
# Compatível com Python 3.11+
# Funções auxiliares para TheOddsAPI: resolução dinâmica de sport_key,
# coleta de eventos/odds e normalização básica.

from __future__ import annotations
import os, requests, unicodedata, difflib, time
from typing import Dict, List, Any, Iterable

ODDSAPI_BASE = "https://api.the-odds-api.com/v4"
ODDSAPI_KEY = os.environ.get("THEODDSAPI_KEY", "").strip()

class OddsApiError(RuntimeError):
    pass

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().replace("&", " and ")
    for token in (" ec", " fc", " afc", " sc", " ac", " esporte clube", " futebol clube"):
        s = s.replace(token, "")
    s = " ".join(s.split())
    return s

def fetch_sports(active_only: bool = False) -> List[Dict[str, Any]]:
    if not ODDSAPI_KEY:
        raise OddsApiError("THEODDSAPI_KEY ausente no ambiente.")
    r = requests.get(f"{ODDSAPI_BASE}/sports",
                     params={"apiKey": ODDSAPI_KEY, "all": "false" if active_only else "true"},
                     timeout=25)
    if r.status_code == 401:
        raise OddsApiError("TheOddsAPI 401 (chave inválida ou fora do plano).")
    r.raise_for_status()
    return r.json()

def resolve_brazil_soccer_sport_keys() -> List[str]:
    """
    Encontra todos os sport_key de futebol do Brasil disponíveis (Serie A/B/C/D ou equivalentes).
    Evita hardcode; usa title/description/group para detectar BR.
    """
    sports = fetch_sports(active_only=False)
    keys = []
    for s in sports:
        title = _norm(f'{s.get("title","")} {s.get("description","")} {s.get("group","")}')
        if "soccer" in title and ("brazil" in title or "brasil" in title):
            keys.append(s["key"])
    # fallback: se nada encontrado, tente chaves típicas conhecidas (sem quebrar se 404)
    if not keys:
        keys = ["soccer_brazil_serie_a", "soccer_brazil_serie_b", "soccer_brazil_serie_c", "soccer_brazil_serie_d"]
    return list(dict.fromkeys(keys))

def fetch_odds_for_sport(sport_key: str, regions: Iterable[str]) -> List[Dict[str, Any]]:
    if not ODDSAPI_KEY:
        raise OddsApiError("THEODDSAPI_KEY ausente no ambiente.")
    params = {
        "apiKey": ODDSAPI_KEY,
        "regions": ",".join(regions),
        "markets": "h2h,totals",
        "oddsFormat": "decimal"
    }
    url = f"{ODDSAPI_BASE}/sports/{sport_key}/odds"
    r = requests.get(url, params=params, timeout=30)
    # log de limites de cota (se presente)
    remaining = r.headers.get("x-requests-remaining")
    used = r.headers.get("x-requests-used")
    if remaining is not None:
        print(f"[theoddsapi] quota remaining={remaining}, used={used}")
    if r.status_code == 404:
        # sport_key inválido/antigo
        print(f"[theoddsapi] AVISO {sport_key}: 404 UNKNOWN_SPORT")
        return []
    if r.status_code == 429:
        print("[theoddsapi] AVISO: rate limited (429). Aguardando 5s e tentando novamente...")
        time.sleep(5)
        r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    try:
        data = r.json()
    except Exception as e:
        raise OddsApiError(f"Falha ao decodificar JSON ({sport_key}): {e}")
    # data é uma lista de eventos com bookmakers/markets
    return data

def best_match(target: str, candidates: List[str], min_ratio: float = 0.92) -> str | None:
    if not candidates:
        return None
    target_n = _norm(target)
    cands_n = list({_norm(c) for c in candidates})
    ratio_best = -1.0
    best = None
    for c in cands_n:
        r = difflib.SequenceMatcher(a=target_n, b=c).ratio()
        if r > ratio_best:
            ratio_best = r
            best = c
    return best if ratio_best >= min_ratio else None
