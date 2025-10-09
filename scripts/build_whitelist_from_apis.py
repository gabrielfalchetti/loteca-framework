#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_whitelist_from_apis.py
Gera data/in/matches_whitelist.csv com colunas mínimas (match_id, home, away)
a partir da TheOddsAPI (v4), usando apenas dados reais.

Uso:
  python scripts/build_whitelist_from_apis.py \
    --out data/in/matches_whitelist.csv \
    --season 2025 \
    --regions "uk,eu,us,au" \
    --lookahead-days 3 \
    --debug

Requisitos de ambiente:
  - THEODDS_API_KEY (obrigatório)
"""

from __future__ import annotations
import argparse
import hashlib
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import json
import csv
import urllib.parse
import urllib.request


def log(msg: str) -> None:
    print(msg, flush=True)


def err(msg: str) -> None:
    print(f"##[error]{msg}", flush=True)


def debug_log(enabled: bool, msg: str) -> None:
    if enabled:
        print(msg, flush=True)


def http_get_json(url: str, timeout: int = 20) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "loteca-bot/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        return json.loads(data.decode("utf-8"))


def to_commence_time_to_utc_z(lookahead_days: int) -> str:
    """
    Retorna string UTC no formato exigido pela TheOddsAPI:
    YYYY-MM-DDTHH:MM:SSZ (sem offset +00:00)
    """
    now_utc = datetime.now(timezone.utc)
    target = now_utc + timedelta(days=lookahead_days)
    # zulu format sem micros, com 'Z'
    return target.strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_team_name(name: Optional[str]) -> str:
    return (name or "").strip()


def make_match_id(home: str, away: str, commence_iso: str) -> str:
    """
    ID determinístico curto baseado em (home, away, commence_time)
    """
    base = f"{home}|{away}|{commence_iso}".lower()
    h = hashlib.md5(base.encode("utf-8")).hexdigest()[:12]
    return h


def fetch_upcoming_h2h(
    api_key: str,
    regions: str,
    lookahead_days: int,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Chamada única à TheOddsAPI /v4/sports/upcoming/odds (markets=h2h).
    Retorna a lista raw de eventos.
    """
    commence_to = to_commence_time_to_utc_z(lookahead_days)
    params = {
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
        "apiKey": api_key,
        "commenceTimeTo": commence_to,  # <-- formato Z correto
    }
    base_url = "https://api.the-odds-api.com/v4/sports/upcoming/odds"
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    debug_log(debug, f"[whitelist][theodds] URL -> {url}")

    data = http_get_json(url)
    # Em caso de erro, a API retorna dict com 'message'
    if isinstance(data, dict) and "message" in data:
        raise RuntimeError(
            f"[whitelist][theodds] API error: {data.get('message')} "
            f"(error_code={data.get('error_code')})"
        )
    if not isinstance(data, list):
        raise RuntimeError("[whitelist][theodds] Resposta inesperada (não é lista).")
    debug_log(debug, f"[whitelist][theodds] eventos retornados: {len(data)}")
    return data


def extract_h2h_rows(events: List[Dict[str, Any]], debug: bool = False) -> List[Dict[str, str]]:
    """
    Converte a resposta da TheOddsAPI em linhas (match_id, home, away)
    usando os nomes das equipes do campo 'home_team' / 'away_team'.
    """
    rows: List[Dict[str, str]] = []

    for ev in events:
        home = safe_team_name(ev.get("home_team"))
        away = safe_team_name(ev.get("away_team"))
        commence = ev.get("commence_time") or ""
        if not home or not away:
            debug_log(debug, f"[whitelist][skip] evento sem home/away: {ev.get('id')}")
            continue

        # Alguns eventos vêm com commence_time ISO com 'Z' — manter como veio
        commence_iso = str(commence).strip()

        match_id = make_match_id(home, away, commence_iso)
        rows.append(
            {
                "match_id": match_id,
                "home": home,
                "away": away,
                "commence_time": commence_iso,
                "source": "theoddsapi",
            }
        )

    # Remover duplicatas por (home, away, commence_time)
    seen = set()
    dedup: List[Dict[str, str]] = []
    for r in rows:
        key = (r["home"].lower(), r["away"].lower(), r["commence_time"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)

    debug_log(debug, f"[whitelist] linhas após dedupe: {len(dedup)}")
    return dedup


def write_csv(out_path: str, rows: List[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fieldnames = ["match_id", "home", "away", "commence_time", "source"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera matches_whitelist.csv a partir da TheOddsAPI (v4)."
    )
    parser.add_argument("--out", required=True, help="Caminho do CSV de saída.")
    parser.add_argument("--season", required=True, help="Temporada (ex.: 2025).")
    parser.add_argument(
        "--regions", default="uk,eu,us,au", help="Regiões da TheOddsAPI."
    )
    parser.add_argument(
        "--lookahead-days", type=int, default=3, help="Janela de busca em dias (UTC)."
    )
    parser.add_argument(
        "--debug", action="store_true", help="Logs extras."
    )
    args = parser.parse_args()

    api_key = os.getenv("THEODDS_API_KEY", "").strip()
    if not api_key:
        err("THEODDS_API_KEY ausente no ambiente.")
        sys.exit(3)

    log(f"[whitelist] params: season={args.season}, regions={args.regions}, lookahead={args.lookahead_days}")

    try:
        events = fetch_upcoming_h2h(
            api_key=api_key,
            regions=args.regions,
            lookahead_days=args.lookahead_days,
            debug=args.debug,
        )
    except Exception as e:
        err(str(e))
        sys.exit(3)

    rows = extract_h2h_rows(events, debug=args.debug)

    if len(rows) == 0:
        err("Nenhum jogo retornado pela TheOddsAPI dentro da janela. Whitelist vazia.")
        sys.exit(3)

    write_csv(args.out, rows)
    # Preview curto
    try:
        import pandas as pd  # só para preview; se não houver, ignora
        df = pd.read_csv(args.out)
        debug_log(args.debug, "===== Preview whitelist (top 10) =====")
        debug_log(args.debug, df.head(10).to_string(index=False))
    except Exception:
        pass

    log(f"[whitelist] OK -> {args.out} ({len(rows)} jogos)")


if __name__ == "__main__":
    main()
