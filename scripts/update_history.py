# scripts/update_history.py
# Baixa resultados FINALIZADOS dos últimos N dias via API-FOOTBALL
# e salva em CSV padronizado para o feature_engineer.py.
#
# Uso:
#   python -m scripts.update_history --since_days 14 --out data/history/results.csv
#
# Saída (CSV):
#   date_utc,home,away,home_goals,away_goals
#   2025-10-10T19:00:00Z,Time A,Time B,2,1
#
# Observações:
# - Só grava o arquivo se houver pelo menos 1 partida; caso contrário, sai com código 2 e mensagem clara.
# - Tratei paginação/retry básicos. O endpoint usado é /fixtures?date=YYYY-MM-DD&timezone=UTC

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import requests


API_BASE = "https://v3.football.api-sports.io"
HEADERS_NAME = "x-apisports-key"


def utc_today() -> datetime:
    return datetime.now(timezone.utc)


def daterange_utc(days_back: int) -> List[str]:
    """Lista as datas (YYYY-MM-DD) de hoje até N dias atrás (inclusive), em UTC."""
    today = utc_today().date()
    dates = []
    for i in range(days_back, -1, -1):
        d = today - timedelta(days=i)
        dates.append(d.isoformat())
    return dates


def api_get(url: str, headers: Dict[str, str], params: Dict[str, Any], max_retries: int = 3, backoff: float = 1.5) -> Optional[Dict[str, Any]]:
    """GET com retries exponenciais simples."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            # 429 / 5xx -> backoff
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = backoff ** attempt
                print(f"[update_history][WARN] status={resp.status_code} retry in {wait:.1f}s (attempt {attempt}/{max_retries})")
                time.sleep(wait)
                continue
            # outros códigos: loga e para
            print(f"[update_history][ERROR] GET {url} params={params} status={resp.status_code} body={resp.text[:400]}")
            return None
        except requests.RequestException as e:
            wait = backoff ** attempt
            print(f"[update_history][WARN] exception={e} retry in {wait:.1f}s (attempt {attempt}/{max_retries})")
            time.sleep(wait)
    print("[update_history][ERROR] max retries exceeded")
    return None


def extract_rows(fixtures_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extrai linhas (finalizadas) de um payload /fixtures."""
    rows: List[Dict[str, Any]] = []
    if not fixtures_payload or "response" not in fixtures_payload:
        return rows

    for item in fixtures_payload.get("response", []):
        try:
            fixture = item.get("fixture", {})
            teams = item.get("teams", {})
            goals = item.get("goals", {})

            status_short = (fixture.get("status", {}) or {}).get("short")
            # Considera finalizados: FT (tempo regul.), AET (prorrogação), PEN (pênaltis)
            if status_short not in {"FT", "AET", "PEN"}:
                continue

            # Data em UTC (a API já retorna ISO 8601 com tz). Normalizamos em Z.
            date_iso = fixture.get("date")
            if not date_iso:
                # fallback via timestamp
                ts = fixture.get("timestamp")
                if ts:
                    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    date_iso = dt.isoformat().replace("+00:00", "Z")
            # normalização
            if date_iso and date_iso.endswith("+00:00"):
                date_iso = date_iso.replace("+00:00", "Z")

            home_name = (teams.get("home", {}) or {}).get("name")
            away_name = (teams.get("away", {}) or {}).get("name")

            hg = goals.get("home")
            ag = goals.get("away")

            if any(v is None for v in [date_iso, home_name, away_name, hg, ag]):
                continue

            row = {
                "date_utc": date_iso,
                "home": str(home_name).strip(),
                "away": str(away_name).strip(),
                "home_goals": int(hg),
                "away_goals": int(ag),
            }
            rows.append(row)
        except Exception as e:
            print(f"[update_history][WARN] skipping item due to parse error: {e}")

    return rows


def fetch_finished_since(days: int, api_key: str) -> List[Dict[str, Any]]:
    headers = {HEADERS_NAME: api_key}
    all_rows: List[Dict[str, Any]] = []

    # A API aceita "date=YYYY-MM-DD". Iteramos datas (UTC) de hoje até days atrás.
    dates = daterange_utc(days)
    for d in dates:
        page = 1
        while True:
            params = {"date": d, "timezone": "UTC", "page": page}
            payload = api_get(f"{API_BASE}/fixtures", headers=headers, params=params)
            if not payload:
                break
            rows = extract_rows(payload)
            all_rows.extend(rows)

            # paginação
            paging = payload.get("paging") or {}
            current = paging.get("current", 1)
            total = paging.get("total", 1)
            if current >= total:
                break
            page += 1

    # Remove duplicatas simples por (date_utc, home, away, home_goals, away_goals)
    uniq = {(r["date_utc"], r["home"], r["away"], r["home_goals"], r["away_goals"]) for r in all_rows}
    deduped = [
        {
            "date_utc": t[0],
            "home": t[1],
            "away": t[2],
            "home_goals": t[3],
            "away_goals": t[4],
        }
        for t in sorted(list(uniq), key=lambda x: x[0])
    ]
    return deduped


def write_csv(out_path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["date_utc", "home", "away", "home_goals", "away_goals"],
        )
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> int:
    parser = argparse.ArgumentParser(description="Atualiza histórico com resultados finalizados via API-FOOTBALL.")
    parser.add_argument("--since_days", type=int, default=14, help="Quantos dias para trás (UTC) buscar (inclui hoje).")
    parser.add_argument("--out", required=True, help="Caminho do CSV de saída.")
    args = parser.parse_args()

    api_key = os.getenv("API_FOOTBALL_KEY", "").strip()
    if not api_key:
        print("[update_history][CRITICAL] Variável de ambiente API_FOOTBALL_KEY ausente.", file=sys.stderr)
        return 2

    try:
        print(f"[update_history] fetching finished fixtures for last {args.since_days} day(s)...")
        rows = fetch_finished_since(args.since_days, api_key)
        if not rows:
            print("[update_history][ERROR] Nenhum jogo finalizado encontrado no intervalo solicitado.", file=sys.stderr)
            # NÃO grava arquivo vazio; deixa o passo seguinte falhar com clareza
            return 2

        write_csv(args.out, rows)
        print(f"[update_history] written {len(rows)} rows to {args.out}")
        return 0
    except Exception as e:
        print(f"[update_history][CRITICAL] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())