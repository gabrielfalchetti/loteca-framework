from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

PROVIDERS = [
    {
        "name": "apifootball-direct",
        "base": "https://v3.football.api-sports.io",
        "key_env": "API_FOOTBALL_KEY",
        "key_header": "x-apisports-key",
        "host_header": None,
        "host_value": None,
    },
    {
        "name": "apifootball-rapidapi",
        "base": "https://api-football-v1.p.rapidapi.com/v3",
        "key_env": "X_RAPIDAPI_KEY",
        "key_header": "X-RapidAPI-Key",
        "host_header": "X-RapidAPI-Host",
        "host_value": "api-football-v1.p.rapidapi.com",
    },
]

FT_STATUSES = {"FT", "AET", "PEN"}
MAX_RETRIES = 3
BACKOFF = 1.6


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_date(d: datetime) -> str:
    return d.date().isoformat()


def make_headers(provider: Dict[str, Any], api_key: str) -> Dict[str, str]:
    headers = {provider["key_header"]: api_key}
    if provider.get("host_header") and provider.get("host_value"):
        headers[provider["host_header"]] = provider["host_value"]
    return headers


def api_get(url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                wait = BACKOFF ** attempt
                print(f"[update_history][WARN] {url} status={r.status_code} retry in {wait:.1f}s ({attempt}/{MAX_RETRIES}) params={params}")
                time.sleep(wait)
                continue
            print(f"[update_history][ERROR] GET {url} status={r.status_code} body={r.text[:400]}")
            return None
        except requests.RequestException as e:
            wait = BACKOFF ** attempt
            print(f"[update_history][WARN] exception={e} retry in {wait:.1f}s ({attempt}/{MAX_RETRIES})")
            time.sleep(wait)
    print(f"[update_history][ERROR] max retries exceeded for {url}")
    return None


def log_payload_issues(name: str, payload: Dict[str, Any], params: Dict[str, Any]) -> None:
    if not payload:
        return
    errs = payload.get("errors") or {}
    if errs:
        print(f"[update_history][INFO] provider={name} API errors={errs} params={params}")
    results = payload.get("results")
    if isinstance(results, int) and results == 0:
        print(f"[update_history][INFO] provider={name} results=0 params={params}")


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
            if status_short not in FT_STATUSES:
                continue

            date_iso = fixture.get("date")
            if not date_iso:
                ts = fixture.get("timestamp")
                if ts:
                    date_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
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


def dedup_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    uniq = {(r["date_utc"], r["home"], r["away"], r["home_goals"], r["away_goals"]) for r in rows}
    return [
        {"date_utc": t[0], "home": t[1], "away": t[2], "home_goals": t[3], "away_goals": t[4]}
        for t in sorted(uniq, key=lambda x: x[0])
    ]


def filter_by_window(rows: List[Dict[str, Any]], start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            dt = datetime.fromisoformat(r["date_utc"].replace("Z", "+00:00"))
            if start_dt <= dt <= end_dt:
                out.append(r)
        except Exception:
            continue
    return dedup_sort(out)


def fetch_day_by_day(provider: Dict[str, Any], api_key: str, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    base = provider["base"]
    headers = make_headers(provider, api_key)
    all_rows: List[Dict[str, Any]] = []
    cur = start_dt
    while cur.date() <= end_dt.date():
        day_iso = iso_date(cur)
        page = 1
        while True:
            params = {"date": day_iso, "timezone": "UTC", "page": page}
            payload = api_get(f"{base}/fixtures", headers, params)
            log_payload_issues(provider["name"], payload or {}, params)
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
            time.sleep(0.25)
        cur += timedelta(days=1)
    return dedup_sort(all_rows)


def fetch_from_to(provider: Dict[str, Any], api_key: str, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    base = provider["base"]
    headers = make_headers(provider, api_key)
    all_rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        params = {"from": iso_date(start_dt), "to": iso_date(end_dt), "timezone": "UTC", "page": page}
        payload = api_get(f"{base}/fixtures", headers, params)
        log_payload_issues(provider["name"], payload or {}, params)
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
        time.sleep(0.25)
    return dedup_sort(all_rows)


def fetch_last(provider: Dict[str, Any], api_key: str, last: int) -> List[Dict[str, Any]]:
    base = provider["base"]
    headers = make_headers(provider, api_key)
    params = {"last": last, "timezone": "UTC"}
    payload = api_get(f"{base}/fixtures", headers, params)
    log_payload_issues(provider["name"], payload or {}, params)
    return dedup_sort(extract_finished_rows(payload) if payload else [])


def try_provider(provider: Dict[str, Any], start_dt: datetime, end_dt: datetime) -> Tuple[str, List[Dict[str, Any]]]:
    api_key = os.getenv(provider["key_env"], "").strip()
    if not api_key:
        print(f"[update_history][INFO] provider={provider['name']} skipped (no {provider['key_env']})")
        return (provider["name"], [])

    print(f"[update_history] provider={provider['name']} try day-by-day...")
    rows = fetch_day_by_day(provider, api_key, start_dt, end_dt)
    if rows:
        return (provider["name"], filter_by_window(rows, start_dt, end_dt))

    print(f"[update_history] provider={provider['name']} try from/to...")
    rows = fetch_from_to(provider, api_key, start_dt, end_dt)
    if rows:
        return (provider["name"], filter_by_window(rows, start_dt, end_dt))

    print(f"[update_history] provider={provider['name']} try last=400 + window filter...")
    rows = fetch_last(provider, api_key, last=400)
    rows = filter_by_window(rows, start_dt, end_dt)
    return (provider["name"], rows)


def write_csv(out_path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date_utc", "home", "away", "home_goals", "away_goals"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    parser = argparse.ArgumentParser(description="Atualiza histórico com resultados finalizados (API-Football / RapidAPI).")
    parser.add_argument("--since_days", type=int, default=14, help="Janela de dias para trás (inclui hoje).")
    parser.add_argument("--out", required=True, help="CSV de saída.")
    args = parser.parse_args()

    end_dt = utc_now().replace(hour=23, minute=59, second=59, microsecond=0)
    start_dt = (end_dt - timedelta(days=args.since_days)).replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        print(f"[update_history] fetching finished fixtures for window {iso_date(start_dt)} -> {iso_date(end_dt)} (UTC)...")

        all_rows: List[Dict[str, Any]] = []
        tried = []

        for prov in PROVIDERS:
            name, rows = try_provider(prov, start_dt, end_dt)
            tried.append(name)
            if rows:
                all_rows = rows
                print(f"[update_history] provider={name} returned {len(all_rows)} rows.")
                break
            else:
                print(f"[update_history] provider={name} returned 0 rows.")

        if not all_rows:
            print(f"[update_history][ERROR] Nenhum jogo finalizado encontrado. Provedores tentados: {', '.join(tried)}.", file=sys.stderr)
            return 2

        write_csv(args.out, all_rows)
        print(f"[update_history] written {len(all_rows)} rows to {args.out}")
        return 0

    except Exception as e:
        print(f"[update_history][CRITICAL] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())