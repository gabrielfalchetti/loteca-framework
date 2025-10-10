#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera whitelist (apenas FUTEBOL) a partir da The Odds API v4.

Saída CSV: match_id,home,away

Como funciona:
1) Lista todos os esportes: /v4/sports?all=true
2) Filtra apenas sport_key contendo "soccer"
3) Para cada liga de soccer encontrada, chama /v4/sports/{sport_key}/odds (markets=h2h)
   com regions e commenceTimeTo (em formato Z), agregando partidas.
4) Aplica aliases se fornecido (--aliases), deduplica e grava CSV.

Compatibilidade com workflow:
- Aceita --season (ignorado aqui) para não quebrar steps existentes.
- Mantém parâmetros --regions, --lookahead-days, --sports (default "soccer"), --aliases, --debug.
- Lê THEODDS_API_KEY do ambiente.
"""

from __future__ import annotations
import argparse
import csv
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests

DEBUG = False
BASE = "https://api.the-odds-api.com/v4"


def log(msg: str) -> None:
    print(msg, flush=True)


def dlog(msg: str) -> None:
    if DEBUG:
        log(msg)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, help="Arquivo CSV de saída (match_id,home,away)")
    p.add_argument("--regions", required=True, help="Regiões (ex.: uk,eu,us,au)")
    p.add_argument("--lookahead-days", type=int, default=3, help="Dias à frente para buscar jogos")
    p.add_argument("--sports", default="soccer", help="Filtro de esporte (default: soccer)")
    p.add_argument("--aliases", default=None, help="JSON com {'teams': {...}}")
    p.add_argument("--season", default=None, help="Compatibilidade; não usado na coleta")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()


def iso_utc_Z(dt_obj: datetime) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    dt_utc = dt_obj.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def short_hash(s: str, n: int = 12) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:n]


def load_aliases(path: Optional[str]) -> Dict[str, str]:
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        teams = data.get("teams", {})
        return {str(k).strip().lower(): str(v).strip() for k, v in teams.items()}
    except FileNotFoundError:
        log(f"[whitelist][WARN] aliases não encontrado: {path}")
        return {}
    except Exception as e:
        log(f"[whitelist][WARN] falha lendo aliases: {e}")
        return {}


def apply_alias(name: str, aliases: Dict[str, str]) -> str:
    if not name:
        return name
    return aliases.get(name.strip().lower(), name).strip()


def http_get(url: str, params: dict, timeout: int = 25) -> Tuple[int, Any]:
    dlog(f"[HTTP GET] {url} params={params}")
    r = requests.get(url, params=params, timeout=timeout)
    try:
        js = r.json()
    except Exception:
        js = {"message": r.text}
    return r.status_code, js


def list_sports(api_key: str) -> List[Dict[str, Any]]:
    url = f"{BASE}/sports"
    params = {"all": "true", "apiKey": api_key}
    status, payload = http_get(url, params)
    if status != 200 or not isinstance(payload, list):
        log(f"##[error][whitelist] falha ao listar esportes: HTTP {status} {payload}")
        return []
    return payload


def fetch_odds_for_sport(api_key: str, sport_key: str, regions: str, commence_to: str) -> List[Dict[str, Any]]:
    """
    Chama /v4/sports/{sport_key}/odds (markets=h2h). Retorna lista de eventos.
    """
    url = f"{BASE}/sports/{sport_key}/odds"
    params = {
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
        "apiKey": api_key,
        "commenceTimeTo": commence_to,  # precisa terminar com Z
    }
    status, payload = http_get(url, params)
    if status != 200 or not isinstance(payload, list):
        log(f"[whitelist][WARN] liga {sport_key} sem dados: HTTP {status} {payload}")
        return []
    return payload


def main() -> None:
    global DEBUG
    args = parse_args()
    DEBUG = bool(args.debug)

    api_key = os.environ.get("THEODDS_API_KEY", "").strip()
    if not api_key:
        log("##[error]THEODDS_API_KEY ausente em secrets/ambiente")
        sys.exit(3)

    regions = args.regions.strip()
    lookahead = max(1, int(args.lookahead_days))
    sports_filter = (args.sports or "soccer").strip().lower()
    aliases = load_aliases(args.aliases)

    now_utc = datetime.now(timezone.utc)
    commence_to = iso_utc_Z(now_utc + timedelta(days=lookahead))

    log(f"[whitelist] params: regions={regions}, lookahead={lookahead}, sports={sports_filter}")

    # 1) lista esportes
    sports = list_sports(api_key)
    if not sports:
        sys.exit(3)

    # 2) filtra apenas ligas com "soccer" no sport_key (ex.: soccer_epl, soccer_brazil_campeonato, etc)
    soccer_keys = [s.get("key") for s in sports if sports_filter in str(s.get("key", "")).lower()]
    if not soccer_keys:
        log("##[error]Nenhuma liga de futebol disponível na API.")
        sys.exit(3)

    # 3) busca odds por liga e agrega
    rows_raw: List[Tuple[str, str, str]] = []
    collected = 0
    for idx, skey in enumerate(soccer_keys, start=1):
        events = fetch_odds_for_sport(api_key, skey, regions, commence_to)
        dlog(f"[whitelist] liga {idx}/{len(soccer_keys)} {skey}: {len(events)} eventos")
        for ev in events:
            home = ev.get("home_team")
            away = ev.get("away_team")
            ctime = ev.get("commence_time")
            if not (home and away and ctime):
                continue
            home = apply_alias(str(home), aliases)
            away = apply_alias(str(away), aliases)
            rows_raw.append((home.strip(), away.strip(), str(ctime).strip()))
            collected += 1
        # rate-limit leve para evitar 429
        time.sleep(0.2)

    if collected == 0:
        log("##[error]Nenhum jogo de futebol encontrado na janela solicitada.")
        sys.exit(3)

    # Dedup por (home, away, ctime)
    dedup = {}
    for home, away, ctime in rows_raw:
        dedup[(home, away, ctime)] = True

    rows: List[Tuple[str, str, str]] = []
    for (home, away, ctime) in dedup.keys():
        mid = short_hash(f"{home}|{away}|{ctime}", 12)
        rows.append((mid, home, away))

    rows.sort(key=lambda x: (x[1].lower(), x[2].lower()))

    # 4) grava CSV
    out_path = args.out
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "home", "away"])
        for mid, home, away in rows:
            w.writerow([mid, home, away])

    log(f"[whitelist] escrito: {out_path}  linhas={len(rows)}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log(f"##[error]Falha inesperada na whitelist: {e}")
        sys.exit(3)