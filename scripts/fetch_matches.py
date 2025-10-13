# scripts/fetch_matches.py
# Gera data/out/<RUN_ID>/matches_whitelist.csv com base em fontes públicas (API-FOOTBALL e TheOddsAPI)
# Saída: CSV com colunas: match_id,home,away,utc_kickoff (UTC ISO8601)
# Uso:
#   python -m scripts.fetch_matches --out data/out/<RUN_ID>/matches_whitelist.csv --lookahead 3 [--max_matches 14]

from __future__ import annotations

import os
import sys
import csv
import json
import time
import math
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict

import requests
from unidecode import unidecode


@dataclass
class Match:
    home: str
    away: str
    utc_kickoff: datetime
    src: str
    src_id: str


UTC = timezone.utc


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _iso_utc(dt: datetime) -> str:
    # Padroniza ISO em UTC (sem micros)
    return dt.astimezone(UTC).replace(microsecond=0).isoformat()


def _norm_team(name: str) -> str:
    # Normalização leve para deduplicação
    return unidecode(name or "").strip().lower()


def _unique_key(m: Match) -> Tuple[str, str, int]:
    # Chave de unicidade: (home_norm, away_norm, epoch_hour)
    # Agrupamos por hora para tolerar pequenas variações de minuto entre fontes
    epoch_hour = int(m.utc_kickoff.timestamp() // 3600)
    return (_norm_team(m.home), _norm_team(m.away), epoch_hour)


def _dedupe(matches: List[Match]) -> List[Match]:
    seen: Dict[Tuple[str, str, int], Match] = {}
    for m in matches:
        k = _unique_key(m)
        # Mantém o primeiro visto (já ordenaremos por kickoff antes)
        if k not in seen:
            seen[k] = m
    return list(seen.values())


def _bounded(dt: datetime, start: datetime, end: datetime) -> bool:
    return start <= dt <= end


# -------------------------- API-FOOTBALL --------------------------------- #

def fetch_apifootball(from_dt: datetime, to_dt: datetime, key: str, extra_headers: Optional[Dict[str, str]] = None, timeout: int = 15) -> List[Match]:
    """
    Busca fixtures entre from_dt e to_dt (UTC) via API-FOOTBALL v3.
    Necessita header: x-apisports-key
    """
    base = "https://v3.football.api-sports.io"
    url = f"{base}/fixtures"
    headers = {
        "x-apisports-key": key.strip(),
    }
    # Caso o usuário use via RapidAPI (não é o default da API-FOOTBALL, mas alguns usam)
    rapid = os.getenv("X_RAPIDAPI_KEY", "").strip()
    if rapid:
        headers["X-RapidAPI-Key"] = rapid

    if extra_headers:
        headers.update(extra_headers)

    params = {
        "from": from_dt.strftime("%Y-%m-%d"),
        "to": to_dt.strftime("%Y-%m-%d"),
        "timezone": "UTC",
    }

    print(f"[fetch][apifootball] GET {url} params={params}")
    try:
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        print(f"[fetch][apifootball][ERROR] {e}", file=sys.stderr)
        return []

    if not isinstance(payload, dict) or "response" not in payload:
        print("[fetch][apifootball][WARN] Resposta inesperada (sem 'response')", file=sys.stderr)
        return []

    out: List[Match] = []
    items = payload.get("response", []) or []
    now = _now_utc()
    for it in items:
        try:
            fixture = it.get("fixture", {})
            teams = it.get("teams", {})
            fid = str(fixture.get("id", "")) or ""
            date_iso = fixture.get("date")  # ISO já em UTC (pela query timezone=UTC)
            if not date_iso:
                continue
            try:
                dt = datetime.fromisoformat(date_iso.replace("Z", "+00:00")).astimezone(UTC)
            except Exception:
                continue
            # Filtra somente futuros dentro da janela
            if dt < now or not _bounded(dt, from_dt, to_dt + timedelta(days=1)):
                continue

            home = (teams.get("home") or {}).get("name") or ""
            away = (teams.get("away") or {}).get("name") or ""
            if not home or not away:
                continue

            out.append(Match(home=home, away=away, utc_kickoff=dt, src="apifootball", src_id=fid))
        except Exception:
            # ignora item malformado
            continue

    print(f"[fetch][apifootball] coletados={len(out)}")
    return out


# ---------------------------- THEODDSAPI --------------------------------- #

def _theodds_list_sports(key: str, timeout: int = 15) -> List[dict]:
    url = "https://api.the-odds-api.com/v4/sports"
    params = {"apiKey": key.strip()}
    print(f"[fetch][theodds] GET {url}")
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json() or []
    except Exception as e:
        print(f"[fetch][theodds][ERROR] list_sports: {e}", file=sys.stderr)
        return []


def _theodds_list_events(key: str, sport_key: str, timeout: int = 15) -> List[dict]:
    # Endpoint de eventos (sem odds) — mais leve e suficiente para whitelist
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events"
    params = {"apiKey": key.strip()}
    print(f"[fetch][theodds] GET {url}")
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json() or []
    except Exception as e:
        print(f"[fetch][theodds][ERROR] list_events[{sport_key}]: {e}", file=sys.stderr)
        return []


def fetch_theodds(from_dt: datetime, to_dt: datetime, key: str, max_matches: int) -> List[Match]:
    """
    Varre esportes de 'soccer' e junta próximos eventos dentro da janela.
    Limita requisições para não estourar cota.
    """
    sports = _theodds_list_sports(key)
    soccer_sports = [s for s in sports if isinstance(s, dict) and (str(s.get("key", "")).startswith("soccer_") or "Soccer" in str(s.get("group", "")))]

    out: List[Match] = []
    now = _now_utc()

    for s in soccer_sports:
        if len(out) >= max_matches:
            break
        skey = s.get("key", "")
        if not skey:
            continue
        events = _theodds_list_events(key, skey)
        for ev in events:
            try:
                home = ev.get("home_team") or ""
                away = ev.get("away_team") or ""
                commence = ev.get("commence_time")  # ISO8601 UTC
                evid = str(ev.get("id", "")) or ""
                if not (home and away and commence):
                    continue
                dt = datetime.fromisoformat(commence.replace("Z", "+00:00")).astimezone(UTC)
                if dt < now or not _bounded(dt, from_dt, to_dt + timedelta(days=1)):
                    continue
                out.append(Match(home=home, away=away, utc_kickoff=dt, src="theodds", src_id=evid))
            except Exception:
                continue

        # Pequena pausa para evitar rate limit agressivo
        time.sleep(0.3)

    print(f"[fetch][theodds] coletados={len(out)}")
    return out


# ----------------------------- ORQUESTRAÇÃO ------------------------------ #

def build_whitelist(lookahead_days: int, max_matches: int) -> List[Match]:
    start = _now_utc()
    end = start + timedelta(days=max(0, lookahead_days))

    all_matches: List[Match] = []

    # Preferência: API-FOOTBALL
    api_key = os.getenv("API_FOOTBALL_KEY", "").strip()
    if api_key:
        try:
            apif = fetch_apifootball(start, end, api_key)
            all_matches.extend(apif)
        except Exception as e:
            print(f"[fetch][apifootball][EXCEPTION] {e}", file=sys.stderr)

    # Fallback/Complemento: TheOddsAPI
    if len(all_matches) < max_matches:
        theodds_key = os.getenv("THEODDS_API_KEY", "").strip()
        if theodds_key:
            try:
                need = max_matches - len(all_matches)
                theo = fetch_theodds(start, end, theodds_key, max_matches=need)
                all_matches.extend(theo)
            except Exception as e:
                print(f"[fetch][theodds][EXCEPTION] {e}", file=sys.stderr)

    # Ordena por kickoff, deduplica e corta no limite
    all_matches.sort(key=lambda m: m.utc_kickoff)
    unique = _dedupe(all_matches)
    unique.sort(key=lambda m: m.utc_kickoff)
    if len(unique) > max_matches:
        unique = unique[:max_matches]

    print(f"[fetch] total_unico={len(unique)} (solicitado max={max_matches})")
    return unique


def write_whitelist_csv(path_out: str, matches: List[Match]) -> None:
    os.makedirs(os.path.dirname(path_out) or ".", exist_ok=True)
    with open(path_out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "home", "away", "utc_kickoff"])
        for i, m in enumerate(matches, start=1):
            w.writerow([i, m.home, m.away, _iso_utc(m.utc_kickoff)])
    print(f"[fetch] whitelist gravada: {path_out}  linhas={len(matches)}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gera matches_whitelist.csv automaticamente a partir de fontes públicas.")
    p.add_argument("--out", required=True, help="Caminho do CSV de saída (ex: data/out/<RUN_ID>/matches_whitelist.csv)")
    p.add_argument("--lookahead", type=int, default=3, help="Dias à frente para buscar partidas (padrão: 3)")
    p.add_argument("--max_matches", type=int, default=14, help="Quantidade máxima de partidas na whitelist (padrão: 14)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    lookahead = max(0, int(args.lookahead))
    max_matches = max(1, int(args.max_matches))

    # Apenas log informativo das chaves disponíveis (sem expor valores)
    has_api = bool(os.getenv("API_FOOTBALL_KEY", "").strip())
    has_theo = bool(os.getenv("THEODDS_API_KEY", "").strip())
    print(f"[fetch] lookahead_days={lookahead} max_matches={max_matches} sources: apifootball={'ok' if has_api else 'off'} theodds={'ok' if has_theo else 'off'}")

    matches = build_whitelist(lookahead_days=lookahead, max_matches=max_matches)
    if not matches:
        print("[fetch][CRITICAL] Nenhuma partida encontrada nas fontes configuradas dentro da janela.", file=sys.stderr)
        return 4

    write_whitelist_csv(args.out, matches)
    return 0


if __name__ == "__main__":
    sys.exit(main())