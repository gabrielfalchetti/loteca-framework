#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
apifootball_injuries_safe.py
Coleta de lesões/suspensões via API-FOOTBALL (usando RapidAPI).

Uso:
  python scripts/apifootball_injuries_safe.py --out-dir data/out/<RODADA_ID> --season 2025 --debug

Requisitos de ambiente:
  - X_RAPIDAPI_KEY (Secrets) -> chave do RapidAPI
  - Arquivo data/in/matches_source.csv com cabeçalho pelo menos: match_id,home,away,source
    (lat/lon são ignorados aqui)

Saída:
  - <OUT_DIR>/injuries.csv  (colunas-chave para enriquecer o workflow)
Fail-fast:
  - Se não houver chave ou ocorrer erro de rede repetido, sai com código 6.
"""

import argparse
import csv
import json
import os
import sys
import time
import unicodedata
from typing import Dict, Optional, Tuple, List

import requests

RAPID_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{RAPID_HOST}/v3"
TIMEOUT = 25  # segundos
RETRIES = 4
BACKOFF_BASE = 2.0

# Aliases úteis para clubes brasileiros e variações sem acento / hifen
ALIASES: Dict[str, List[str]] = {
    # Série A / B / Copas comuns no nosso pipeline
    "america mineiro": ["américa mg", "america-mg", "américa-mg", "america mg", "america (mg)"],
    "atletico mineiro": ["atlético mg", "atletico-mg", "atlético-mg"],
    "botafogo sp": ["botafogo-sp", "botafogo ribeirao", "botafogo ribeirão preto", "botafogo (sp)"],
    "criciuma": ["criciúma", "criciuma ec"],
    "cuiaba": ["cuiabá", "cuiaba ec"],
    "goias": ["goiás"],
    "avai": ["avaí", "avai fc"],
    "america mg": ["america mineiro"],  # fallback cruzado
    "atletico-mg": ["atletico mineiro"],  # caso venha interno
    "athletic club": ["athletic mg", "athletic (mg)"],  # clube de são joão del-rei
    "fluminense": ["fluminense fc"],
    "vilanova": ["vila nova", "vila nova fc"],
    "operario pr": ["operário pr", "operario-ferroviario", "operário-ferroviário", "operario (pr)"],
    "paysandu": ["paysandu sc"],
    "novorizontino": ["gremio novorizontino", "grêmio novorizontino", "novorizontino sp"],
}

# Colunas de saída
OUT_COLS = [
    "team_id",
    "team_name",
    "player_name",
    "player_age",
    "player_position",
    "type",          # tipo de lesão/suspensão
    "reason",        # descrição/observação
    "fixture_id",
    "fixture_date",
    "status",        # provável/duvida/fora, quando presente
    "last_update",
]


def norm(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    # remove caracteres não alfanum/hifen/espaco
    clean = []
    for ch in s:
        if ch.isalnum() or ch in (" ", "-", "_", "/"):
            clean.append(ch)
    return "".join(clean).strip()


def add_aliases(canonical: str) -> List[str]:
    out = {canonical}
    n = norm(canonical)
    out.add(n)
    for k, vs in ALIASES.items():
        if n == k or n in vs:
            out.add(k)
            for v in vs:
                out.add(v)
    # Também quebra por hifen/espaco para tentar variações
    parts = n.replace("-", " ").split()
    if parts:
        out.add(" ".join(parts))
        out.add("".join(parts))
    return list(out)


def rapid_headers() -> Dict[str, str]:
    key = os.getenv("X_RAPIDAPI_KEY", "").strip()
    if not key:
        print("##[error]X_RAPIDAPI_KEY ausente em Secrets.", file=sys.stderr)
        sys.exit(6)
    return {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": RAPID_HOST,
    }


def http_get(url: str, params: Dict[str, str]) -> Dict:
    headers = rapid_headers()
    err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            err = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:
            err = str(e)
        sleep_s = BACKOFF_BASE ** (attempt - 1)
        print(f"[injuries][WARN] GET {url} params={params} falhou (tentativa {attempt}/{RETRIES}): {err}. Retentando em {sleep_s:.1f}s...")
        time.sleep(sleep_s)
    print(f"##[error][injuries] Falha definitiva em {url}: {err}", file=sys.stderr)
    sys.exit(6)


def search_team_id(name: str, country_hint: Optional[str] = None) -> Tuple[Optional[int], Optional[str]]:
    """
    Busca o ID do time por /teams?search=... usando variações/aliases.
    Se country_hint for passado (e.g. 'Brazil'), tenta priorizar.
    """
    url = f"{BASE_URL}/teams"
    tried = set()
    for q in add_aliases(name):
        qn = q.strip()
        if not qn or qn in tried:
            continue
        tried.add(qn)
        params = {"search": qn}
        data = http_get(url, params)
        resp = data.get("response") or []
        if not resp:
            continue
        # Se veio mais de 1, filtra por país e/ou melhor match por normalização
        best = None
        best_score = -1
        for item in resp:
            team = item.get("team") or {}
            tname = team.get("name") or ""
            tid = team.get("id")
            country = (item.get("team") or {}).get("country") or (item.get("country") or {}).get("name") or (item.get("venue") or {}).get("country") or ""
            n_t = norm(tname)
            n_q = norm(qn)
            score = 0
            if n_t == n_q:
                score += 5
            if country_hint and norm(country) == norm(country_hint):
                score += 3
            # bônus para “sp” quando o nome do time menciona “sp”
            if " sp" in n_q or "sp" == n_q[-2:]:
                if " sp" in n_t:
                    score += 2
            if score > best_score:
                best_score = score
                best = (tid, tname)
        if best and best[0]:
            return best[0], best[1]
    return None, None


def fetch_injuries(team_id: int, season: str) -> List[Dict]:
    """
    Endpoint: /injuries?team=<id>&season=<season>
    Retorna lista de dicts normalizados conforme OUT_COLS.
    """
    url = f"{BASE_URL}/injuries"
    data = http_get(url, {"team": str(team_id), "season": str(season)})
    items = data.get("response") or []
    out = []
    for it in items:
        player = (it.get("player") or {})
        team = (it.get("team") or {})
        fixture = (it.get("fixture") or {})
        # Alguns retornos colocam info no 'player'->'type'/'reason'/'status'
        rec = {
            "team_id": team.get("id"),
            "team_name": (team.get("name") or "").strip(),
            "player_name": (player.get("name") or "").strip(),
            "player_age": player.get("age"),
            "player_position": (player.get("position") or "").strip(),
            "type": (player.get("type") or it.get("type") or "").strip(),
            "reason": (player.get("reason") or it.get("reason") or "").strip(),
            "fixture_id": fixture.get("id"),
            "fixture_date": (fixture.get("date") or "").strip(),
            "status": (player.get("status") or it.get("status") or "").strip(),
            "last_update": (it.get("update") or "").strip(),
        }
        out.append(rec)
    return out


def read_matches(path: str) -> List[Tuple[str, str]]:
    """
    Lê data/in/matches_source.csv e retorna pares (home, away).
    Cabeçalhos exigidos: match_id,home,away,source (lat/lon ignorados aqui)
    """
    if not os.path.isfile(path):
        print(f"##[error]Arquivo de entrada não encontrado: {path}", file=sys.stderr)
        sys.exit(6)
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for need in ("match_id", "home", "away", "source"):
            if need not in [c.strip().lower() for c in r.fieldnames or []]:
                print(f"##[error]Cabeçalho obrigatório ausente em {path}: {need}", file=sys.stderr)
                sys.exit(6)
        teams = []
        for row in r:
            home = (row.get("home") or "").strip()
            away = (row.get("away") or "").strip()
            if home and away:
                teams.append((home, away))
        if not teams:
            print(f"##[error]Nenhum jogo válido em {path}", file=sys.stderr)
            sys.exit(6)
        return teams


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, help="Diretório de saída (ex.: data/out/<RODADA_ID>)")
    ap.add_argument("--season", required=True, help="Temporada (ex.: 2025)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.out_dir
    season = str(args.season)
    os.makedirs(out_dir, exist_ok=True)

    matches_csv = "data/in/matches_source.csv"
    pairs = read_matches(matches_csv)

    # Monta o conjunto de times únicos a buscar
    unique_teams = {}
    for h, a in pairs:
        for t in (h, a):
            t_n = norm(t)
            unique_teams[t_n] = t  # guarda forma original

    # País preferencial para reduzir ambiguidades (ajuste se necessário por etapa)
    country_hint = "Brazil"

    # Resolve IDs
    id_map: Dict[str, Tuple[int, str]] = {}
    for key, original in unique_teams.items():
        tid, tname = search_team_id(original, country_hint=country_hint)
        if args.debug:
            print(f"[injuries][DEBUG] RESOLVE team '{original}' -> id={tid} name='{tname}'")
        if tid:
            id_map[key] = (tid, tname or original)
        else:
            # Última tentativa sem country_hint
            tid2, tname2 = search_team_id(original, country_hint=None)
            if args.debug:
                print(f"[injuries][DEBUG] FALLBACK team '{original}' -> id={tid2} name='{tname2}'")
            if tid2:
                id_map[key] = (tid2, tname2 or original)

    if not id_map:
        print("##[error][injuries] Nenhum ID de time encontrado para os jogos informados.", file=sys.stderr)
        sys.exit(6)

    # Coleta injuries
    all_rows = []
    for key, (tid, tname) in id_map.items():
        try:
            rows = fetch_injuries(tid, season)
            if args.debug:
                print(f"[injuries] team_id={tid} '{tname}' -> {len(rows)} registros de lesões/suspensões")
            if not rows:
                # Mesmo sem registros, registramos uma linha “vazia” para sinalizar que buscou e não há dados
                all_rows.append({
                    "team_id": tid,
                    "team_name": tname,
                    "player_name": "",
                    "player_age": "",
                    "player_position": "",
                    "type": "",
                    "reason": "",
                    "fixture_id": "",
                    "fixture_date": "",
                    "status": "none",
                    "last_update": "",
                })
            else:
                all_rows.extend(rows)
        except SystemExit:
            raise
        except Exception as e:
            print(f"##[error][injuries] Falha ao coletar para time_id={tid} '{tname}': {e}", file=sys.stderr)
            sys.exit(6)

    # Salva CSV
    out_path = os.path.join(out_dir, "injuries.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS)
        w.writeheader()
        for row in all_rows:
            # garante todas as colunas
            safe = {k: row.get(k, "") for k in OUT_COLS}
            w.writerow(safe)

    # Verificação final
    if not os.path.isfile(out_path) or os.path.getsize(out_path) == 0:
        print("##[error]injuries.csv não gerado", file=sys.stderr)
        sys.exit(6)

    if args.debug:
        print(f"[injuries] OK -> {out_path} ({len(all_rows)} linhas)")


if __name__ == "__main__":
    main()