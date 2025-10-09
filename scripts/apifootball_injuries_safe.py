#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
apifootball_injuries_safe.py

Coleta lesões/suspensões do API-Football (via RapidAPI) e gera:
- {OUT_DIR}/injuries_raw.csv  : granular por jogador
- {OUT_DIR}/injuries.csv      : tabela "longa", fácil de agregar por time

Principais cuidados:
- Resolve o ID do time por busca ("teams?search=") com heurísticas:
  * descarta times U17/U18/U19/U20/U21/U23, "B", "II", Feminino
  * prioriza nome igual ao da whitelist (normalizado)
  * opcionalmente prioriza país "Brazil" quando aplicável
- Tolera falhas de rede (retries) e retorna CSV vazio sem derrubar o workflow
- Normaliza colunas e datas; evita crashes por mudanças no payload

Uso:
  python scripts/apifootball_injuries_safe.py --out-dir data/out/123 --season 2025 --debug

Requer:
  - Variável de ambiente X_RAPIDAPI_KEY (se ausente, o script sai com código 0 e gera CSV vazio)
  - Arquivo {OUT_DIR}/matches_whitelist.csv (para capturar nomes de times)
"""

import argparse
import csv
import datetime as dt
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests


API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"
TIMEOUT = 12
RETRIES = 3
SLEEP_BETWEEN = 0.8


# ------------------------------ Utils ---------------------------------
def dbg(enabled: bool, *msg):
    if enabled:
        print("[injuries]", *msg, flush=True)


def read_whitelist(out_dir: str, debug: bool) -> List[Tuple[str, str, str]]:
    """
    Retorna lista de (match_id, home, away) a partir da whitelist.
    """
    path = os.path.join(out_dir, "matches_whitelist.csv")
    if not os.path.isfile(path):
        dbg(debug, f"AVISO: whitelist ausente -> {path}")
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            mid = str(r.get("match_id", "")).strip()
            h = str(r.get("team_home", r.get("home", ""))).strip()
            a = str(r.get("team_away", r.get("away", ""))).strip()
            rows.append((mid, h, a))
    return rows


def normalize_team_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[-_]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    # mapeamentos rápidos comuns no BR
    s = s.replace("atletico mg", "atletico mineiro")
    s = s.replace("américa mg", "america mineiro").replace("america mg", "america mineiro")
    s = s.replace("operario-pr", "operario pr").replace("operário pr", "operario pr")
    s = s.replace("athletic club", "athletic club mg")
    s = s.replace("avai", "avaí").replace("avai", "avaí")
    return s


EXCLUDE_PAT = re.compile(r"\b(u\d{2}|sub-\d{2}| women| feminino| fem\.?| b\b| ii\b)\b", re.IGNORECASE)


def looks_like_main_squad(name: str) -> bool:
    """
    True para times principais; False para Uxx, B, II, Feminino.
    """
    if not name:
        return False
    if EXCLUDE_PAT.search(name):
        return False
    return True


def req_json(url: str, headers: Dict[str, str], params: Dict[str, str], debug: bool) -> Optional[dict]:
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            else:
                dbg(debug, f"HTTP {r.status_code} GET {url} params={params} body={r.text[:200]}")
        except Exception as e:
            dbg(debug, f"erro request {url}: {e}")
        time.sleep(SLEEP_BETWEEN * (i + 1))
    return None


def pick_team_id_by_search(name: str, headers: Dict[str, str], prefer_country: Optional[str], debug: bool) -> Optional[Tuple[int, str]]:
    """
    Busca por nome e seleciona a opção mais provável para o 'time principal'.
    Retorna (team_id, team_name_oficial) ou None.
    """
    url = f"{BASE_URL}/teams"
    js = req_json(url, headers, {"search": name}, debug)
    if not js or "response" not in js:
        return None

    candidates = []
    for item in js.get("response", []):
        t = item.get("team", {}) or {}
        c = item.get("country", {}) or {}
        tid = t.get("id")
        tname = t.get("name") or ""
        cc_name = c.get("name") or item.get("country") or ""
        valid = tid is not None and tname
        if not valid:
            continue
        # Filtrar Uxx/B/II/Feminino
        if not looks_like_main_squad(tname.lower()):
            continue
        # Score heurístico: +2 se país preferido, +1 se nome normalizado igual
        score = 0
        if prefer_country and str(cc_name).lower().strip() == prefer_country.lower().strip():
            score += 2
        if normalize_team_name(tname) == normalize_team_name(name):
            score += 1
        candidates.append((score, tid, tname, cc_name))

    if not candidates:
        return None
    candidates.sort(key=lambda x: (-x[0], x[2]))
    best = candidates[0]
    return best[1], best[2]


def fetch_injuries(team_id: int, season: int, headers: Dict[str, str], debug: bool) -> List[dict]:
    """
    Chama /injuries?team=&season= e retorna lista padrão (pode ser vazia).
    """
    url = f"{BASE_URL}/injuries"
    js = req_json(url, headers, {"team": str(team_id), "season": str(season)}, debug)
    if not js or "response" not in js:
        return []
    return js.get("response", [])


def normalize_injury_row(raw: dict, team_id: int, team_official_name: str) -> dict:
    """
    Normaliza um item da API para colunas estáveis.
    """
    player = (raw.get("player") or {})
    team = (raw.get("team") or {})
    fixture = (raw.get("fixture") or {})
    league = (raw.get("league") or {})
    # Alguns providers devolvem "type", outros "reason"/"description"
    detail = None
    for k in ("type", "reason", "description", "injury"):
        v = (raw.get("injury") or {}).get(k) if isinstance(raw.get("injury"), dict) else raw.get(k)
        if v:
            detail = v
            break

    status = (raw.get("player") or {}).get("status") or raw.get("status")

    start = raw.get("fixture", {}).get("date") or raw.get("date")
    # Normaliza data
    try:
        if start:
            start = str(dt.datetime.fromisoformat(str(start).replace("Z", "+00:00")))
    except Exception:
        start = str(start) if start is not None else ""

    row = {
        "team_id": team_id,
        "team_name": team_official_name or team.get("name") or "",
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "player_id": player.get("id"),
        "player_name": player.get("name"),
        "player_age": player.get("age"),
        "player_position": player.get("position"),
        "status": status,
        "type": detail,
        "fixture_id": fixture.get("id"),
        "date": start,
    }
    return row


def status_severity_weight(status: Optional[str], detail: Optional[str]) -> float:
    """
    Pondera gravidade (aproximação): OUT/SUSP >>> DOUBTFUL >>> QUESTIONABLE >>> PROBABLE.
    """
    s = (status or "").lower()
    d = (detail or "").lower()
    if any(x in s for x in ("out", "absent", "excluded")) or any(x in d for x in ("rupture", "surgery", "fracture", "suspens", "knee", "acl", "mcl")):
        return 1.0
    if "susp" in s or "susp" in d:  # suspensão
        return 0.9
    if any(x in s for x in ("doubt", "doubtful", "uncertain")):
        return 0.7
    if any(x in s for x in ("question", "questionable")):
        return 0.5
    if any(x in s for x in ("probable", "minor")):
        return 0.3
    return 0.6  # default moderado


# ------------------------------ Main ---------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--season", required=True, type=int, help="Temporada, ex.: 2025")
    ap.add_argument("--country", default="Brazil", help="País preferido para resolver times (default: Brazil)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.out_dir
    season = int(args.season)
    prefer_country = args.country
    debug = args.debug

    api_key = os.environ.get("X_RAPIDAPI_KEY", "").strip()
    if not api_key:
        dbg(debug, "AVISO: X_RAPIDAPI_KEY ausente — gerando CSVs vazios e saindo (0).")
        # Gera arquivos vazios para não quebrar pipeline
        empty_headers_raw = ["team_id","team_name","league_id","league_name","player_id","player_name","player_age","player_position","status","type","fixture_id","date"]
        empty_headers = ["match_id","team_name","inj_count","inj_weight_sum"]
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "injuries_raw.csv"), "w", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=empty_headers_raw).writeheader()
        with open(os.path.join(out_dir, "injuries.csv"), "w", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=empty_headers).writeheader()
        sys.exit(0)

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": API_HOST
    }

    wl = read_whitelist(out_dir, debug)
    if not wl:
        dbg(debug, "AVISO: whitelist vazia — sem times para consultar. Gerando CSV vazio.")
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "injuries.csv"), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["match_id","team_name","inj_count","inj_weight_sum"])
            w.writeheader()
        with open(os.path.join(out_dir, "injuries_raw.csv"), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "team_id","team_name","league_id","league_name","player_id","player_name","player_age","player_position","status","type","fixture_id","date"
            ])
            w.writeheader()
        sys.exit(0)

    # Resolver IDs por time (evita repetir buscas)
    team_id_cache: Dict[str, Tuple[Optional[int], Optional[str]]] = {}
    def resolve_id(name: str) -> Tuple[Optional[int], Optional[str]]:
        key = normalize_team_name(name)
        if key in team_id_cache:
            return team_id_cache[key]
        res = pick_team_id_by_search(name, headers, prefer_country, debug)
        team_id_cache[key] = res if res else (None, None)
        return team_id_cache[key]

    # Coleta injuries por time e agrega por match
    detailed_rows: List[dict] = []
    agg_rows: List[dict] = []

    for match_id, home, away in wl:
        for side_name in (home, away):
            if not side_name:
                continue
            tid, tname = resolve_id(side_name)
            dbg(debug, f"RESOLVE team '{side_name}' -> id={tid} name='{tname}'")
            cnt = 0
            wsum = 0.0
            if tid:
                lst = fetch_injuries(tid, season, headers, debug)
                for item in lst:
                    row = normalize_injury_row(item, tid, tname or side_name)
                    detailed_rows.append(row)
                    weight = status_severity_weight(row.get("status"), row.get("type"))
                    cnt += 1
                    wsum += float(weight)
            else:
                lst = []
            # linha de agregação por jogo/time (é isso que o join usa)
            agg_rows.append({
                "match_id": str(match_id),
                "team_name": tname or side_name,
                "inj_count": cnt,
                "inj_weight_sum": round(wsum, 3)
            })

    # Salvar
    os.makedirs(out_dir, exist_ok=True)
    raw_path = os.path.join(out_dir, "injuries_raw.csv")
    with open(raw_path, "w", encoding="utf-8", newline="") as f:
        cols = ["team_id","team_name","league_id","league_name","player_id","player_name","player_age","player_position","status","type","fixture_id","date"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in detailed_rows:
            w.writerow({k: r.get(k) for k in cols})

    out_path = os.path.join(out_dir, "injuries.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        cols = ["match_id","team_name","inj_count","inj_weight_sum"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in agg_rows:
            w.writerow({k: r.get(k) for k in cols})

    dbg(debug, f"OK -> {out_path} ({len(agg_rows)} linhas); raw={len(detailed_rows)} registros")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        raise
    except Exception as e:
        print(f"[injuries][ERRO] {e}", file=sys.stderr)
        # Em caso de erro, gerar arquivos vazios para não quebrar pipeline
        try:
            # Melhor esforço: tenta descobrir OUT_DIR a partir de argv
            out_dir = None
            for i, a in enumerate(sys.argv):
                if a == "--out-dir" and i + 1 < len(sys.argv):
                    out_dir = sys.argv[i + 1]
                    break
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
                with open(os.path.join(out_dir, "injuries.csv"), "w", encoding="utf-8", newline="") as f:
                    csv.DictWriter(f, fieldnames=["match_id","team_name","inj_count","inj_weight_sum"]).writeheader()
                with open(os.path.join(out_dir, "injuries_raw.csv"), "w", encoding="utf-8", newline="") as f:
                    csv.DictWriter(f, fieldnames=[
                        "team_id","team_name","league_id","league_name","player_id","player_name","player_age","player_position","status","type","fixture_id","date"
                    ]).writeheader()
        finally:
            sys.exit(0)