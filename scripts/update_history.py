# scripts/update_history.py
# Atualiza histórico com resultados FINALIZADOS via API-FOOTBALL.
# Estratégia robusta:
#   1) Tenta /fixtures?from=YYYY-MM-DD&to=YYYY-MM-DD&timezone=UTC (com paginação).
#   2) Se não houver linhas, faz fallback em /fixtures?last=200&status=FT&timezone=UTC,
#      e filtra as partidas para ficar apenas no intervalo pedido.
#
# Uso no workflow:
#   python -m scripts.update_history --since_days 14 --out data/history/results.csv
#
# Saída: CSV com colunas:
#   date_utc,home,away,home_goals,away_goals

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

API_BASE = "https://v3.football.api-sports.io"
HEADERS_NAME = "x-apisports-key"


def utc_today() -> datetime:
    return datetime.now(timezone.utc)


def iso_date(d: datetime) -> str:
    return d.date().isoformat()


def api_get(url: str, headers: Dict[str, str], params: Dict[str, Any], max_retries: int = 3, backoff: float = 1.6) -> Optional[Dict[str, Any]]:
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                wait = backoff ** attempt
                print(f"[update_history][WARN] status={r.status_code} retry in {wait:.1f}s (attempt {attempt}/{max_retries})")
                time.sleep(wait)
                continue
            print(f"[update_history][ERROR] GET {url} params={params} status={r.status_code} body={r.text[:400]}")
            return None
        except requests.RequestException as e:
            wait = backoff ** attempt
            print(f"[update_history][WARN] exception={e} retry in {wait:.1f}s (attempt {attempt}/{max_retries})")
            time.sleep(wait)
    print("[update_history][ERROR] max retries exceeded")
    return None


def extract_finished_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not payload or "response" not in payload:
        return rows

    for item in payload.get("response", []):
        try:
            fixture = item.get("fixture", {}) or {}
            teams = item.get("teams", {}) or {}
            goals = item.get("goals", {}) or {}

            status_short = (fixture.get("status", {}) or {}).get("short")
            # Considera finalizados: FT (tempo normal), AET (prorrogação), PEN (pênaltis)
            if status_short not in {"FT", "AET", "PEN"}:
                continue

            # Data UTC em ISO
            date_iso = fixture.get("date")
            if not date_iso:
                ts = fixture.get("timestamp")
                if ts:
                    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    date_iso = dt.isoformat().replace("+00:00", "Z")
            if date_iso and date_iso.endswith("+00:00"):
                date_iso = date_iso.replace("+00:00", "Z")

            home_name = (teams.get("home", {}) or {}).get("name")
            away_name = (teams.get("away", {}) or {}).get("name")
            hg = goals.get("home")
            ag = goals.get("away")

            if any(v is None for v in [date_iso, home_name, away_name, hg, ag]):
                continue

            rows.append(
                {
                    "date_utc": str(date_iso).strip(),
                    "home": str(home_name).strip(),
                    "away": str(away_name).strip(),
                    "home_goals": int(hg),
                    "away_goals": int(ag),
                }
            )
        except Exception as e:
            print(f"[update_history][WARN] skipping item due to parse error: {e}")
    return rows


def fetch_range(from_date: str, to_date: str, api_key: str) -> List[Dict[str, Any]]:
    headers = {HEADERS_NAME: api_key}
    all_rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        params = {"from": from_date, "to": to_date, "timezone": "UTC", "page": page}
        payload = api_get(f"{API_BASE}/fixtures", headers, params)
        if not payload:
            break
        rows = extract_finished_rows(payload)
        all_rows.extend(rows)

        paging = payload.get("paging") or {}
        current = paging.get("current", 1)
        total = paging.get("total", 1)
        if current >= total:
            break
        page += 1

    # Dedup por (date_utc, home, away, home_goals, away_goals)
    uniq = {(r["date_utc"], r["home"], r["away"], r["home_goals"], r["away_goals"]) for r in all_rows}
    return [
        {
            "date_utc": t[0],
            "home": t[1],
            "away": t[2],
            "home_goals": t[3],
            "away_goals": t[4],
        }
        for t in sorted(uniq, key=lambda x: x[0])
    ]


def fetch_fallback_last(last: int, api_key: str) -> List[Dict[str, Any]]:
    """Busca os 'last' fixtures finalizados mais recentes."""
    headers = {HEADERS_NAME: api_key}
    params = {"last": last, "status": "FT", "timezone": "UTC"}
    payload = api_get(f"{API_BASE}/fixtures", headers, params)
    return extract_finished_rows(payload) if payload else []


def filter_by_window(rows: List[Dict[str, Any]], start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            # date_utc vem ISO; garantir timezone UTC
            dt = datetime.fromisoformat(r["date_utc"].replace("Z", "+00:00"))
            if start_dt <= dt <= end_dt:
                out.append(r)
        except Exception:
            continue
    # Dedup e sort
    uniq = {(x["date_utc"], x["home"], x["away"], x["home_goals"], x["away_goals"]) for x in out}
    return [
        {
            "date_utc": t[0],
            "home": t[1],
            "away": t[2],
            "home_goals": t[3],
            "away_goals": t[4],
        }
        for t in sorted(uniq, key=lambda x: x[0])
    ]


def write_csv(out_path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date_utc", "home", "away", "home_goals", "away_goals"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    parser = argparse.ArgumentParser(description="Atualiza histórico com resultados finalizados via API-FOOTBALL.")
    parser.add_argument("--since_days", type=int, default=14, help="Janela de dias para trás (inclui hoje).")
    parser.add_argument("--out", required=True, help="CSV de saída.")
    args = parser.parse_args()

    api_key = os.getenv("API_FOOTBALL_KEY", "").strip()
    if not api_key:
        print("[update_history][CRITICAL] Variável de ambiente API_FOOTBALL_KEY ausente.", file=sys.stderr)
        return 2

    end_dt = utc_today().replace(hour=23, minute=59, second=59, microsecond=0)
    start_dt = (end_dt - timedelta(days=args.since_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    from_date = iso_date(start_dt)
    to_date = iso_date(end_dt)

    try:
        print(f"[update_history] fetching finished fixtures in range {from_date} -> {to_date} (UTC)...")
        rows = fetch_range(from_date, to_date, api_key)

        if not rows:
            print("[update_history][WARN] Intervalo retornou 0 jogos. Ativando fallback last=200...")
            fallback = fetch_fallback_last(last=200, api_key=api_key)
            rows = filter_by_window(fallback, start_dt, end_dt)

        if not rows:
            print("[update_history][ERROR] Nenhum jogo finalizado encontrado no intervalo solicitado.", file=sys.stderr)
            return 2

        write_csv(args.out, rows)
        print(f"[update_history] written {len(rows)} rows to {args.out}")
        return 0

    except Exception as e:
        print(f"[update_history][CRITICAL] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())