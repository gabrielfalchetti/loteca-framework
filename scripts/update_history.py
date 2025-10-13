#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Atualiza base histórica de resultados em CSV unificado (date,home,away,gf,ga).

Fontes suportadas (tentadas em ordem):
  1) API-FOOTBALL (API-Sports v3) - https://v3.football.api-sports.io
     Header: x-apisports-key: {API_FOOTBALL_KEY}
     Estratégia: busca dia-a-dia por fixtures e filtra status finalizados (FT/AET/PEN).

  2) APIFOOTBALL (apiv3.apifootball.com) - https://apiv3.apifootball.com/
     Params: action=get_events&from=YYYY-MM-DD&to=YYYY-MM-DD&timezone=UTC&APIkey=...

Uso:
  python -m scripts.update_history --since_days 14 --out data/history/results.csv
"""

from __future__ import annotations
import os
import sys
import time
import json
import argparse
import datetime as dt
from typing import List, Dict, Any, Optional

import requests
import pandas as pd


STATUS_ENDED_API_SPORTS = {"FT", "AET", "PEN"}
UTC = "UTC"


def log(level: str, msg: str) -> None:
    print(f"[update_history][{level}] {msg}")


def daterange_days(start_date: dt.date, end_date: dt.date) -> List[dt.date]:
    # inclusive range [start_date, end_date]
    days = []
    cur = start_date
    while cur <= end_date:
        days.append(cur)
        cur += dt.timedelta(days=1)
    return days


def normalize_row(date_str: str, home: str, away: str, gf: Any, ga: Any, provider: str) -> Dict[str, Any]:
    def to_int(x):
        try:
            return int(x)
        except Exception:
            return None
    return {
        "date": date_str[:10],
        "home": str(home or "").strip(),
        "away": str(away or "").strip(),
        "gf": to_int(gf),
        "ga": to_int(ga),
        "provider": provider,
    }


def fetch_api_sports(api_key: str, start: dt.date, end: dt.date, timezone: str = UTC, sleep_s: float = 0.2) -> List[Dict[str, Any]]:
    """
    API-FOOTBALL (API-Sports v3):
      GET https://v3.football.api-sports.io/fixtures?date=YYYY-MM-DD&timezone=UTC
      Header: x-apisports-key
    Filtra status.short em FT/AET/PEN.
    """
    base = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key}
    rows: List[Dict[str, Any]] = []

    for day in daterange_days(start, end):
        params = {"date": day.strftime("%Y-%m-%d"), "timezone": timezone}
        try:
            r = requests.get(base, headers=headers, params=params, timeout=30)
            if r.status_code != 200:
                log("INFO", f"API-Sports HTTP {r.status_code} params={params}")
                time.sleep(sleep_s)
                continue
            data = r.json()
            resp = data.get("response", [])
            for fx in resp:
                fixture = fx.get("fixture", {}) or {}
                status = (fixture.get("status", {}) or {}).get("short")
                if status not in STATUS_ENDED_API_SPORTS:
                    continue
                teams = fx.get("teams", {}) or {}
                home_t = (teams.get("home", {}) or {}).get("name")
                away_t = (teams.get("away", {}) or {}).get("name")
                goals = fx.get("goals", {}) or {}
                gf = goals.get("home")
                ga = goals.get("away")
                date_str = (fixture.get("date") or params["date"])[:10]
                rows.append(normalize_row(date_str, home_t, away_t, gf, ga, "api-sports"))
        except Exception as e:
            log("INFO", f"API-Sports exception day={day}: {e}")
        time.sleep(sleep_s)
    return rows


def fetch_apifootball(api_key: str, start: dt.date, end: dt.date, timezone: str = UTC) -> List[Dict[str, Any]]:
    """
    APIFOOTBALL clássico (apiv3.apifootball.com):
      GET https://apiv3.apifootball.com/
          ?action=get_events
          &from=YYYY-MM-DD
          &to=YYYY-MM-DD
          &timezone=UTC
          &APIkey=...
    Não usar 'date' nem 'page'. Retorna partidas (inclui finalizadas).
    Precisamos filtrar por status 'Finished' (ou similares).
    """
    base = "https://apiv3.apifootball.com/"
    params = {
        "action": "get_events",
        "from": start.strftime("%Y-%m-%d"),
        "to": end.strftime("%Y-%m-%d"),
        "timezone": timezone,
        "APIkey": api_key,
    }
    rows: List[Dict[str, Any]] = []
    try:
        r = requests.get(base, params=params, timeout=45)
        if r.status_code != 200:
            log("INFO", f"apifootball HTTP {r.status_code} params={params}")
            return rows
        # A API pode retornar lista ou dict de erro
        data = r.json()
        if isinstance(data, dict) and data.get("error"):
            log("INFO", f"apifootball API errors={data.get('error')} params={params}")
            return rows
        if not isinstance(data, list):
            log("INFO", f"apifootball unexpected payload type={type(data)}")
            return rows

        for ev in data:
            status = str(ev.get("match_status", "")).strip().lower()
            # Exemplos vistos: "Finished", "After Pen.", "After ET", etc.
            ended = any(k in status for k in ["finish", "pen", "et", "aet"])
            if not ended:
                continue
            home = ev.get("match_hometeam_name")
            away = ev.get("match_awayteam_name")
            gf = ev.get("match_hometeam_score")
            ga = ev.get("match_awayteam_score")
            # match_date pode vir como "YYYY-MM-DD" (sem hora)
            date_raw = ev.get("match_date") or ev.get("match_time") or ""
            date_str = str(date_raw)[:10] if date_raw else start.strftime("%Y-%m-%d")
            rows.append(normalize_row(date_str, home, away, gf, ga, "apifootball"))
    except Exception as e:
        log("INFO", f"apifootball exception: {e}")
    return rows


def unify_and_write(rows: List[Dict[str, Any]], out_csv: str) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    # Limpa linhas inválidas
    df = df.dropna(subset=["home", "away", "gf", "ga"])
    # Dedupe por (date, home, away)
    df = df.sort_values(["date", "home", "away"]).drop_duplicates(["date", "home", "away"], keep="last")
    # Ordena por data
    df = df.sort_values("date")
    # Garante tipos
    df["gf"] = df["gf"].astype(int)
    df["ga"] = df["ga"].astype(int)
    # Apenas as colunas essenciais para o restante do pipeline
    out_cols = ["date", "home", "away", "gf", "ga"]
    for c in out_cols:
        if c not in df.columns:
            df[c] = None
    df[out_cols].to_csv(out_csv, index=False, encoding="utf-8")
    return len(df)


def run_once(since_days: int, out_csv: str) -> int:
    api_key = os.getenv("API_FOOTBALL_KEY", "").strip()
    if not api_key:
        log("ERROR", "API_FOOTBALL_KEY vazio. Configure o secret API_FOOTBALL_KEY.")
        return 2

    end = dt.datetime.utcnow().date()
    start = end - dt.timedelta(days=since_days)

    # 1) Tenta API-Sports (dia a dia)
    log("INFO", f"fetching finished fixtures for window {start} -> {end} (UTC) via API-Sports...")
    rows = fetch_api_sports(api_key, start, end, timezone=UTC)
    if rows:
        log("INFO", f"API-Sports retornou {len(rows)} partidas finalizadas.")
        return unify_and_write(rows, out_csv)

    # 2) Fallback: apifootball clássico (janela toda)
    log("INFO", f"API-Sports sem dados. Tentando apifootball classic window {start} -> {end}...")
    rows = fetch_apifootball(api_key, start, end, timezone=UTC)
    if rows:
        log("INFO", f"apifootball retornou {len(rows)} partidas finalizadas.")
        return unify_and_write(rows, out_csv)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since_days", type=int, default=14, help="Janela (dias) para buscar jogos finalizados.")
    parser.add_argument("--out", type=str, required=True, help="Caminho do CSV de saída (results.csv).")
    args = parser.parse_args()

    # Primeira tentativa com janela solicitada
    n = run_once(args.since_days, args.out)
    if n > 0:
        print(f"[update_history] OK — gravadas {n} partidas em {args.out}")
        return 0

    log("WARN", f"Janela retornou 0 jogos. Ativando fallback since_days=30...")
    n = run_once(30, args.out)
    if n > 0:
        print(f"[update_history] OK (fallback 30d) — gravadas {n} partidas em {args.out}")
        return 0

    log("ERROR", "Nenhum jogo finalizado encontrado. Provedores tentados: api-sports, apifootball.")
    return 2


if __name__ == "__main__":
    sys.exit(main())