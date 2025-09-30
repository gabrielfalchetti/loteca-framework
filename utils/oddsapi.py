#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

THEODDS_V4_BASE = "https://api.the-odds-api.com/v4"

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def _get_timeout() -> float:
    try:
        return float(os.getenv("ODDS_HTTP_TIMEOUT", "20"))
    except Exception:
        return 20.0

def _sleep_backoff(attempt: int) -> None:
    # exponencial leve: 1s, 2s, 4s, 8s…
    time.sleep(min(8, 2 ** max(0, attempt - 1)))

def _get(
    path: str,
    params: Dict[str, Any],
    timeout: Optional[float] = None,
    max_retries: int = 3,
) -> Optional[requests.Response]:
    url = f"{THEODDS_V4_BASE.rstrip('/')}/{path.lstrip('/')}"
    timeout = timeout or _get_timeout()
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            # 2xx
            if 200 <= r.status_code < 300:
                return r
            # 401 → chave inválida/expirada — não adianta retry
            if r.status_code == 401:
                if _env_bool("DEBUG", False):
                    print(f"[theoddsapi] 401 Unauthorized em {url} — verifique THEODDS_API_KEY/quota.")
                return None
            # 429 → retry com backoff
            if r.status_code == 429:
                if _env_bool("DEBUG", False):
                    print(f"[theoddsapi] 429 Too Many Requests (tentativa {attempt}/{max_retries})")
                _sleep_backoff(attempt)
                continue
            # Demais 4xx/5xx — tenta novamente até estourar tentativas
            if _env_bool("DEBUG", False):
                print(f"[theoddsapi] HTTP {r.status_code} em {url} (tentativa {attempt}/{max_retries})")
            _sleep_backoff(attempt)
        except requests.Timeout:
            if _env_bool("DEBUG", False):
                print(f"[theoddsapi] timeout após {timeout}s (tentativa {attempt}/{max_retries})")
            _sleep_backoff(attempt)
        except requests.RequestException as e:
            if _env_bool("DEBUG", False):
                print(f"[theoddsapi] erro de rede: {e} (tentativa {attempt}/{max_retries})")
            _sleep_backoff(attempt)
    return None

def fetch_odds_for_sport(
    sport_key: str,
    regions: Sequence[str] = ("uk", "eu", "us", "au"),
    markets: Sequence[str] = ("h2h", "totals"),
    odds_format: str = "decimal",
) -> List[Dict[str, Any]]:
    """
    Wrapper resiliente para TheOddsAPI v4 /odds
    Retorna [] em caso de erro/401/429/timeout para que o pipeline SAFE prossiga.
    """
    api_key = os.getenv("THEODDS_API_KEY", "").strip()
    if not api_key:
        if _env_bool("DEBUG", False):
            print("[theoddsapi] THEODDS_API_KEY ausente — retornando vazio.")
        return []

    params = {
        "apiKey": api_key,
        "regions": ",".join(regions),
        "markets": ",".join(markets),
        "oddsFormat": odds_format,
    }

    resp = _get(f"sports/{sport_key}/odds", params=params)
    if resp is None:
        return []

    try:
        data = resp.json()
        # TheOddsAPI retorna lista de eventos; garantimos lista
        if isinstance(data, list):
            return data  # type: ignore[return-value]
        return []
    except ValueError:
        if _env_bool("DEBUG", False):
            print("[theoddsapi] falha ao decodificar JSON — retornando vazio.")
        return []
