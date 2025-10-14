# scripts/harvest_aliases.py
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests
from unidecode import unidecode

# -------------------- Config & Utils --------------------

API_ODDS_BASE = "https://api.the-odds-api.com/v4"

def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def _parse_dt(s: str) -> Optional[dt.datetime]:
    # TheOddsAPI commence_time é ISO8601; API-Football também retorna ISO
    try:
        # pandas não disponível aqui de propósito; usamos fromisoformat quando possível
        # Tolerância a 'Z'
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)
    except Exception:
        return None

def _norm(s: str) -> str:
    return unidecode((s or "").strip())

def _ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

# -------------------- API-Football --------------------

def _apifoot_headers() -> Dict[str, str]:
    """
    Suporta duas formas:
      - RapidAPI: usa X_RAPIDAPI_KEY e host api-football-v1.p.rapidapi.com
      - API-SPORTS direta: usa API_FOOTBALL_KEY em x-apisports-key e host v3.football.api-sports.io
    Daremos preferência ao API-SPORTS direto se não houver RapidAPI.
    """
    xr = os.environ.get("X_RAPIDAPI_KEY", "").strip()
    k = os.environ.get("API_FOOTBALL_KEY", "").strip()

    if xr:  # RapidAPI
        return {"mode": "rapid", "key": xr, "host": "api-football-v1.p.rapidapi.com"}
    elif k:  # API-SPORTS
        return {"mode": "apisports", "key": k, "host": "v3.football.api-sports.io"}
    else:
        return {"mode": "none", "key": "", "host": ""}

def _apifoot_get(path: str, params: Dict[str, str]) -> Optional[dict]:
    cfg = _apifoot_headers()
    if cfg["mode"] == "none":
        print("[harvest][WARN] API-Football keys ausentes; pulando bloco API-Football", file=sys.stderr)
        return None

    if cfg["mode"] == "rapid":
        url = f"https://{cfg['host']}{path}"
        headers = {"X-RapidAPI-Key": cfg["key"], "X-RapidAPI-Host": cfg["host"]}
    else:
        url = f"https://{cfg['host']}{path}"
        headers = {"x-apisports-key": cfg["key"]}

    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[harvest][ERROR] API-Football GET {path} falhou: {e}", file=sys.stderr)
        return None

def fetch_apifoot_fixtures(hours: int) -> List[dict]:
    """
    Busca fixtures no intervalo [now, now+hours], status=NS (não iniciado) e LIVE agendados.
    """
    t0 = _now_utc()
    t1 = t0 + dt.timedelta(hours=hours)
    params = {
        "from": t0.strftime("%Y-%m-%d"),
        "to": t1.strftime("%Y-%m-%d"),
        "timezone": "UTC",
        "status": "NS,POSTP",  # pré-jogo; você pode incluir 'TBD' ou similares se quiser
    }
    data = _apifoot_get("/v3/fixtures", params) or {}
    resp = data.get("response", []) if isinstance(data, dict) else []
    fixtures: List[dict] = []
    for x in resp:
        try:
            league = x.get("league", {}) or {}
            teams = x.get("teams", {}) or {}
            fix = x.get("fixture", {}) or {}
            home = _norm(teams.get("home", {}).get("name", ""))
            away = _norm(teams.get("away", {}).get("name", ""))
            kick = _parse_dt(str(fix.get("date", "")))
            if not (home and away and kick):
                continue
            # filtra por janela exata
            if not (t0 <= kick <= t1):
                continue
            fixtures.append({
                "home": home,
                "away": away,
                "kickoff": kick,
                "league": _norm(league.get("name","")),
                "country": _norm(league.get("country","")),
                "fixture_id": x.get("fixture",{}).get("id")
            })
        except Exception:
            continue
    return fixtures

# -------------------- TheOddsAPI --------------------

def _odds_get(url: str, params: Dict[str, str]) -> Optional[list]:
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[harvest][ERROR] TheOddsAPI GET falhou: {e}", file=sys.stderr)
        return None

def fetch_odds_upcoming(api_key: str, regions: str) -> List[dict]:
    url = f"{API_ODDS_BASE}/sports/upcoming/odds"
    params = dict(apiKey=api_key, regions=regions, markets="h2h", oddsFormat="decimal")
    return _odds_get(url, params) or []

def fetch_odds_soccer_keys(api_key: str) -> List[str]:
    url = f"{API_ODDS_BASE}/sports"
    params = dict(apiKey=api_key, all="true")
    data = _odds_get(url, params) or []
    keys = []
    for s in data:
        if str(s.get("group","")).lower().startswith("soccer"):
            k = s.get("key")
            if isinstance(k, str):
                keys.append(k)
    return keys

def fetch_odds_by_sport(api_key: str, sport_key: str, regions: str) -> List[dict]:
    url = f"{API_ODDS_BASE}/sports/{sport_key}/odds"
    params = dict(apiKey=api_key, regions=regions, markets="h2h", oddsFormat="decimal")
    return _odds_get(url, params) or []

def collect_odds_events(payload: List[dict]) -> List[dict]:
    out = []
    for e in payload or []:
        try:
            home = _norm(e.get("home_team",""))
            away = _norm(e.get("away_team",""))
            kick = _parse_dt(str(e.get("commence_time","")))
            if not (home and away and kick):
                continue
            out.append({"home": home, "away": away, "kickoff": kick})
        except Exception:
            continue
    return out

# -------------------- Matching --------------------

def _time_close(a: dt.datetime, b: dt.datetime, tol_hours: float = 3.0) -> bool:
    return abs((a - b).total_seconds()) <= tol_hours * 3600

def _tok(s: str) -> List[str]:
    # tokeniza simples para fuzzy “bag of words”
    return [t for t in _norm(s).lower().replace("-", " ").replace("/", " ").split() if t]

def _bag_sim(a: str, b: str) -> float:
    sa, sb = set(_tok(a)), set(_tok(b))
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    uni = len(sa | sb)
    return inter / uni

def harvest_aliases(hours: int, regions: str) -> Dict[str, str]:
    """
    Retorna dict {nome_da_fonte(TheOddsAPI) : nome_canonico(API-Football)}.
    """
    api_odds_key = os.environ.get("THEODDS_API_KEY","").strip()
    if not api_odds_key:
        print("[harvest][ERROR] THEODDS_API_KEY ausente", file=sys.stderr)
        return {}

    # Carrega listas
    fixtures = fetch_apifoot_fixtures(hours)  # pode ser vazio se sem chave
    odds_events = collect_odds_events(fetch_odds_upcoming(api_odds_key, regions))

    # Fallback por sport_key para aumentar cobertura
    if len(odds_events) < 10:
        keys = fetch_odds_soccer_keys(api_odds_key)
        for k in keys:
            payload = fetch_odds_by_sport(api_odds_key, k, regions)
            odds_events.extend(collect_odds_events(payload))
            time.sleep(0.2)

    # Índice por kickoff (bucket por hora) p/ acelerar
    buckets: Dict[int, List[dict]] = {}
    for f in fixtures:
        key = int(f["kickoff"].timestamp()) // 3600  # hora UTC
        buckets.setdefault(key, []).append(f)

    alias_map: Dict[str, str] = {}
    for ev in odds_events:
        h = ev["home"]; a = ev["away"]; t = ev["kickoff"]
        bh = int(t.timestamp()) // 3600
        candidates = []
        for k in (bh-1, bh, bh+1):  # +-1h de margem inicial
            candidates.extend(buckets.get(k, []))
        # amplia janela se nada achar
        if not candidates:
            for k in range(bh-3, bh+4):
                candidates.extend(buckets.get(k, []))

        best: Optional[Tuple[dict, float]] = None
        for fx in candidates:
            if not _time_close(t, fx["kickoff"], tol_hours=3.0):
                continue
            sim_home = _bag_sim(h, fx["home"])
            sim_away = _bag_sim(a, fx["away"])
            sim_rev  = _bag_sim(h, fx["away"]) + _bag_sim(a, fx["home"])  # segurança p/ inversão
            score = max(sim_home + sim_away, sim_rev)
            if best is None or score > best[1]:
                best = (fx, score)

        if best and best[1] >= 1.2:  # ~0.6+0.6 típico ou 0.7+0.5 etc
            fx = best[0]
            # mapeia AMBOS os nomes encontrados no TheOddsAPI para o canônico APIFoot
            alias_map[h] = fx["home"]
            alias_map[a] = fx["away"]

    return alias_map

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48, help="Janela futura em horas (24/48/72)")
    ap.add_argument("--regions", default="uk,eu,us,au", help="Regiões da TheOddsAPI")
    ap.add_argument("--out", default="data/aliases/auto_aliases.json", help="Arquivo de saída")
    args = ap.parse_args()

    aliases = harvest_aliases(args.hours, args.regions)
    if not aliases:
        print("[harvest] Nenhum alias coletado (pode ser falta de chaves/API ou não há jogos).")
        # ainda assim, garantimos arquivo válido
        _ensure_dir(args.out)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump({}, f, ensure_ascii=False, indent=2)
        return

    print(f"[harvest] aliases gerados: {len(aliases)} -> {args.out}")
    _ensure_dir(args.out)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(aliases, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()