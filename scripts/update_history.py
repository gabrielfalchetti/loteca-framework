#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Atualiza o histórico de resultados a partir da API-Football (API-Sports).
Se falhar (sem chave, erro HTTP, payload inesperado, 0 jogos ou duplicatas), cria/mede um stub mínimo
para manter o pipeline funcionando. Extrai features táticas como xG e formação.

Saída: CSV com cabeçalho:
date,home,away,home_goals,away_goals,xG_home,xG_away,formation_home,formation_away

Uso:
  python -m scripts.update_history --since_days 21 --out data/history/results.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
from typing import List, Dict, Any

import requests
import pandas as pd
from tqdm import tqdm

API_URL = "https://v3.football.api-sports.io/fixtures"  # API-Football (API-Sports)
MAX_PAGES = 100  # Limite para evitar loops infinitos

def _iso_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def _log(msg: str) -> None:
    print(f"[update_history] {msg}", flush=True)

def _write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "home", "away", "home_goals", "away_goals", "xG_home", "xG_away", "formation_home", "formation_away"])
        for r in rows:
            w.writerow([r["date"], r["home"], r["away"], r["home_goals"], r["away_goals"], 
                       r.get("xG_home", 0), r.get("xG_away", 0), r.get("formation_home", ""), r.get("formation_away", "")])

def _stub(path: str) -> None:
    _log("criando stub BOOT vs BOOT (0-0)")
    _write_csv(
        path,
        [
            {
                "date": "1970-01-01",
                "home": "BOOT",
                "away": "BOOT",
                "home_goals": 0,
                "away_goals": 0,
                "xG_home": 0,
                "xG_away": 0,
                "formation_home": "",
                "formation_away": ""
            }
        ],
    )

def _fetch_finished_fixtures(api_key: str, date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """
    Busca partidas finalizadas (status='FT') no intervalo [date_from, date_to].
    Implementa paginação segura com limite e extrai xG/formações.
    """
    headers = {"x-apisports-key": api_key}
    page = 1
    rows: List[Dict[str, Any]] = []

    while page <= MAX_PAGES:
        params = {
            "from": date_from,
            "to": date_to,
            "status": "FT",      # finalizado
            "page": page,
        }
        resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            _log(f"HTTP {resp.status_code} — {resp.text[:300]}")
            break

        payload = resp.json()
        if not isinstance(payload, dict) or "response" not in payload:
            _log("payload inesperado da API-Football")
            break

        response = payload.get("response") or []
        for fx in tqdm(response, desc=f"Page {page}", leave=False):
            try:
                fixture = fx.get("fixture", {})
                date_iso = fixture.get("date", "")[:10]  # YYYY-MM-DD
                teams = fx.get("teams", {})
                th = teams.get("home", {}) or {}
                ta = teams.get("away", {}) or {}
                goals = fx.get("goals", {}) or {}
                stats = fx.get("statistics", [{}])[0] if fx.get("statistics") else {}
                gh = goals.get("home", 0) if goals.get("home") is not None else 0
                ga = goals.get("away", 0) if goals.get("away") is not None else 0
                xg_home = stats.get("home", {}).get("expected_goals", 0)
                xg_away = stats.get("away", {}).get("expected_goals", 0)
                form_home = stats.get("home", {}).get("formation", "")
                form_away = stats.get("away", {}).get("formation", "")

                home_name = (th.get("name") or "").strip()
                away_name = (ta.get("name") or "").strip()
                if not home_name or not away_name or not date_iso:
                    continue

                rows.append(
                    {
                        "date": date_iso,
                        "home": home_name,
                        "away": away_name,
                        "home_goals": int(gh),
                        "away_goals": int(ga),
                        "xG_home": float(xg_home) if xg_home else 0,
                        "xG_away": float(xg_away) if xg_away else 0,
                        "formation_home": form_home,
                        "formation_away": form_away
                    }
                )
            except Exception as e:
                _log(f"Erro em linha malformada: {e}")
                continue

        paging = payload.get("paging") or {}
        current = int(paging.get("current", 1))
        total = int(paging.get("total", 1))
        if current >= total or page >= MAX_PAGES:
            break
        page += 1
        time.sleep(1)  # Rate limiting para API (100 req/min)

    # Checar duplicatas
    df = pd.DataFrame(rows)
    if df.duplicated(subset=["date", "home", "away"]).any():
        _log("Duplicatas detectadas, removendo...")
        df = df.drop_duplicates(subset=["date", "home", "away"])
        rows = df.to_dict("records")

    return rows

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since_days", type=int, default=21, help="janela retrospectiva em dias (p.ex. 21)")
    parser.add_argument("--out", type=str, required=True, help="caminho do CSV de saída")
    args = parser.parse_args()

    out_csv = args.out
    since_days = max(1, int(args.since_days))

    # Intervalo de datas (UTC)
    today = dt.datetime.utcnow().date()
    date_from = _iso_date(today - dt.timedelta(days=since_days))
    date_to = _iso_date(today)

    api_key = os.environ.get("API_FOOTBALL_KEY", "").strip()

    if not api_key:
        _log("API_FOOTBALL_KEY vazia — usando stub.")
        _stub(out_csv)
        print("[update_history] OK — gravado stub em", out_csv)
        return

    try:
        _log(f"buscando partidas finalizadas de {date_from} até {date_to} (UTC) …")
        rows = _fetch_finished_fixtures(api_key, date_from, date_to)
        if not rows:
            _log("API retornou 0 partidas — caindo no stub.")
            _stub(out_csv)
        else:
            _write_csv(out_csv, rows)
            _log(f"OK — gravadas {len(rows)} partidas em {out_csv}")
    except Exception as e:
        _log(f"erro ao buscar API-Football: {e}. Gerando stub.")
        _stub(out_csv)
        _log(f"OK — stub gravado em {out_csv}")

if __name__ == "__main__":
    main()