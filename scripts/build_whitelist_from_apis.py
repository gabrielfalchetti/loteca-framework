#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera uma whitelist de partidas a partir da The Odds API (v4) filtrando
apenas FUTEBOL (soccer) e escrevendo CSV com colunas mínimas:
  match_id,home,away

Compatível com o workflow existente:
  - Aceita --season (opcional, ignorado aqui, mas mantido p/ compatibilidade)
  - Usa --regions, --lookahead-days, --sports (default: soccer) e --aliases
  - Corrige o formato de commenceTimeTo -> YYYY-MM-DDTHH:MM:SSZ (evita 422)

Requer:
  env THEODDS_API_KEY (secrets)
  pip: requests, pandas, python-dateutil, unidecode (opcional), rapidfuzz (opcional)

Observações:
  - O endpoint usado é /v4/sports/upcoming/odds (markets=h2h)
  - Filtragem por futebol baseada em sport_key/sport_title contendo "soccer"
  - Gera match_id determinístico (sha1 truncado) baseado em "home|away|commence_time"
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
from typing import Dict, Any, List, Tuple, Optional

import requests
from dateutil import tz

DEBUG = False

THE_ODDS_BASE = "https://api.the-odds-api.com/v4"


def log(msg: str) -> None:
    print(msg, flush=True)


def dlog(msg: str) -> None:
    if DEBUG:
        log(msg)


def load_aliases(path: Optional[str]) -> Dict[str, str]:
    """Carrega aliases em formato {"teams": { "Alias A": "Nome Canonico", ... }}"""
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        teams = data.get("teams", {})
        # normaliza chaves para comparação case-insensitive
        return {k.strip().lower(): v.strip() for k, v in teams.items() if isinstance(k, str) and isinstance(v, str)}
    except FileNotFoundError:
        log(f"[whitelist][WARN] aliases não encontrado: {path}")
        return {}
    except Exception as e:
        log(f"[whitelist][WARN] falha lendo aliases: {e}")
        return {}


def apply_alias(name: str, aliases: Dict[str, str]) -> str:
    if not name:
        return name
    key = name.strip().lower()
    return aliases.get(key, name).strip()


def iso_utc_with_Z(dt_obj: datetime) -> str:
    """Formata datetime UTC para 'YYYY-MM-DDTHH:MM:SSZ' (ex.: 2025-10-12T16:05:00Z)."""
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    dt_utc = dt_obj.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def short_hash(s: str, n: int = 12) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:n]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Arquivo CSV de saída (match_id,home,away)")
    parser.add_argument("--regions", required=True, help="Regiões da TheOdds API (ex.: uk,eu,us,au)")
    parser.add_argument("--lookahead-days", type=int, default=3, help="Dias à frente para buscar jogos")
    parser.add_argument("--sports", default="soccer", help="Filtro de esporte (default: soccer)")
    parser.add_argument("--aliases", default=None, help="JSON com {'teams': {...}} para normalização de nomes")
    # Mantido por compatibilidade com o workflow (não usado neste script)
    parser.add_argument("--season", default=None, help="Temporada (compatibilidade; não usado nesta coleta)")
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def fetch_upcoming_odds(
    api_key: str,
    regions: str,
    commence_to_utc_z: str,
    page: int = 1,
) -> Tuple[int, List[Dict[str, Any]]]:
    """Chama /sports/upcoming/odds (markets=h2h) para uma página."""
    url = f"{THE_ODDS_BASE}/sports/upcoming/odds"
    params = {
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
        "apiKey": api_key,
        "commenceTimeTo": commence_to_utc_z,
        "page": page,
    }
    dlog(f"[whitelist][theodds] GET {url} params={params}")
    r = requests.get(url, params=params, timeout=25)
    status = r.status_code
    if status != 200:
        try:
            payload = r.json()
        except Exception:
            payload = {"message": r.text}
        log(f"##[error][whitelist][theodds] HTTP {status}: {json.dumps(payload)}")
        return status, []
    try:
        data = r.json()
    except Exception as e:
        log(f"##[error][whitelist][theodds] resposta inválida: {e}")
        return 500, []
    return status, data


def is_soccer_record(rec: Dict[str, Any], sports_filter: str = "soccer") -> bool:
    """Filtra apenas futebol com base em sport_key e sport_title contendo 'soccer'."""
    sfilter = (sports_filter or "soccer").strip().lower()
    skey = str(rec.get("sport_key", "")).lower()
    stitle = str(rec.get("sport_title", "")).lower()
    return (sfilter in skey) or (sfilter in stitle)


def extract_pair(rec: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
    """
    Extrai (home, away, commence_time) de um registro do odds endpoint.
    O schema típico tem: home_team, away_team, commence_time.
    """
    home = rec.get("home_team")
    away = rec.get("away_team")
    ctime = rec.get("commence_time")  # ISO 8601
    if not (home and away and ctime):
        return None
    return str(home).strip(), str(away).strip(), str(ctime).strip()


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

    # Construir commenceTimeTo no formato correto (Z)
    now_utc = datetime.now(tz=timezone.utc)
    to_utc = now_utc + timedelta(days=lookahead)
    commence_to = iso_utc_with_Z(to_utc)
    log(f"[whitelist] params: regions={regions}, lookahead={lookahead}, sports={sports_filter}")

    # Paginação simples: até 5 páginas ou até retornar vazio
    all_rows: List[Tuple[str, str, str]] = []
    max_pages = 5
    for page in range(1, max_pages + 1):
        status, payload = fetch_upcoming_odds(api_key, regions, commence_to, page=page)
        if status != 200:
            # erro já logado; deixar o workflow fazer retry total (step)
            break
        if not payload:
            dlog(f"[whitelist] page={page} vazia; encerrando paginação.")
            break

        kept = 0
        for rec in payload:
            if not is_soccer_record(rec, sports_filter):
                continue
            pair = extract_pair(rec)
            if not pair:
                continue
            home, away, ctime = pair
            # aplica aliases
            home = apply_alias(home, aliases)
            away = apply_alias(away, aliases)
            # salva
            all_rows.append((home, away, ctime))
            kept += 1
        dlog(f"[whitelist] page={page} kept={kept} total_acum={len(all_rows)}")

        # Heurística: se veio menos de 5, talvez esgotou
        if len(payload) < 5:
            break

        # rate-limit simples
        time.sleep(0.5)

    if not all_rows:
        log("##[error]Nenhum jogo de futebol encontrado na janela solicitada.")
        sys.exit(3)

    # Construir DataFrame leve com CSV nativo (evita depender de pandas aqui)
    # match_id determinístico: sha1(f"{home}|{away}|{commence_time}")[:12]
    out_path = args.out
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    # Remover duplicados (home,away,ctime)
    dedup = {}
    for home, away, ctime in all_rows:
        key = (home, away, ctime)
        dedup[key] = True

    rows_final: List[Tuple[str, str, str]] = []
    for (home, away, ctime) in dedup.keys():
        mid = short_hash(f"{home}|{away}|{ctime}", 12)
        rows_final.append((mid, home, away))

    # Ordena por home/away para estabilidade
    rows_final.sort(key=lambda x: (x[1].lower(), x[2].lower()))

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "home", "away"])
        for mid, home, away in rows_final:
            w.writerow([mid, home, away])

    log(f"[whitelist] escrito: {out_path}  linhas={len(rows_final)}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log(f"##[error]Falha inesperada na whitelist: {e}")
        sys.exit(3)