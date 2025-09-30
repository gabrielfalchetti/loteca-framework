# utils/oddsapi.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests


class OddsApiError(RuntimeError):
    """Erro de alto nível ao falar com a The Odds API."""


_API_BASE = "https://api.the-odds-api.com/v4"


def _get_api_key() -> str:
    key = os.environ.get("THEODDS_API_KEY") or os.environ.get("THEODDSAPI_KEY")
    if not key:
        raise OddsApiError("THEODDS_API_KEY não definido no ambiente.")
    return key


def _req(
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
    retry: int = 1,
) -> requests.Response:
    """
    Requisição básica com pequenos retries para status transitórios (>=500).
    Propaga 401/403/429 de imediato para o chamador lidar.
    """
    url = f"{_API_BASE}/{path.lstrip('/')}"
    params = dict(params or {})
    params["apiKey"] = _get_api_key()

    last_exc: Optional[Exception] = None
    for attempt in range(retry + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code in (401, 403, 429):
                # credencial inválida, não adianta retentar aqui
                r.raise_for_status()
            if r.status_code >= 500 and attempt < retry:
                time.sleep(1.0 + attempt * 0.5)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            if attempt >= retry:
                if isinstance(e, requests.HTTPError):
                    # Empacota num erro de domínio para o caller
                    raise OddsApiError(f"HTTP {r.status_code} em {url}") from e
                raise OddsApiError(f"Falha de rede ao acessar {url}") from e
    # não deve chegar aqui
    assert last_exc is not None
    raise OddsApiError(str(last_exc))


# -------------------- API de alto nível usada pelos scripts --------------------


def resolve_brazil_soccer_sport_keys() -> List[str]:
    """
    Retorna os sport_keys relevantes para futebol no Brasil na The Odds API.
    Implementação 'defensiva': tenta consultar /sports; caso falhe (quota, 401, etc.),
    cai em uma lista estática suficiente para o pipeline.
    """
    try:
        r = _req("sports")
        data = r.json()
        keys = [item["key"] for item in data if isinstance(item, dict) and "brazil" in (item.get("title") or "").lower()]
        # Garante os dois padrões conhecidos se não vier vazio
        base = {"soccer_brazil_campeonato", "soccer_brazil_serie_b"}
        keys = list(sorted(set(keys) | base))
        return keys or ["soccer_brazil_campeonato", "soccer_brazil_serie_b"]
    except OddsApiError:
        # fallback estático
        return ["soccer_brazil_campeonato", "soccer_brazil_serie_b"]


def fetch_odds_for_sport(
    sport_key: str,
    *,
    regions: List[str] | str = "uk,eu,us,au",
    markets: List[str] | str = ("h2h", "totals"),
    odds_format: str = "decimal",
) -> List[Dict[str, Any]]:
    """
    Busca eventos/odds para um sport_key.
    Retorna a lista JSON de eventos (cada item é um dict).
    Levanta OddsApiError em problemas irrecuperáveis.
    """
    if isinstance(regions, list):
        regions = ",".join(regions)
    if isinstance(markets, (list, tuple)):
        markets = ",".join(markets)

    r = _req(
        f"sports/{sport_key}/odds",
        params={
            "regions": regions,
            "markets": markets,
            "oddsFormat": odds_format,
        },
    )
    try:
        data = r.json()
        if not isinstance(data, list):
            raise OddsApiError(f"Resposta inesperada para {sport_key}: tipo {type(data)}")
        return data
    except ValueError as e:
        raise OddsApiError("Falha ao decodificar JSON de odds") from e
