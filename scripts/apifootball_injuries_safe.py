#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coleta contusões/suspensões via API-Football (RapidAPI) e escreve OUT_DIR/injuries.csv.

Modo endurecido: falha (exit != 0) quando uma condição essencial não é atendida.
- Sem X_RAPIDAPI_KEY -> ERRO e sai
- Sem data/in/matches_source.csv -> ERRO e sai
- Erro de requisição / API fora -> ERRO e sai
- Nenhuma resposta útil vinda da API (api_hits == 0) -> ERRO e sai

Parâmetros:
  --out-dir <pasta>   (ex.: data/out/<id>)
  --season <YYYY>     (ex.: 2025)
  --debug             (opcional – log verboso)
"""

import argparse
import csv
import os
import sys
import time
import unicodedata
from datetime import datetime
from typing import Dict, Optional, Tuple

try:
    import requests
except Exception as e:
    print(f"[injuries] ERRO: 'requests' não está disponível: {e}", file=sys.stderr)
    sys.exit(6)

# ---------------- Utils ----------------

def norm(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return (
        s.strip()
         .lower()
         .replace(".", "")
         .replace("-", " ")
         .replace("_", " ")
    )

def best_match_id(name: str, candidates: Dict[int, str]) -> Optional[int]:
    target = norm(name)
    best_id, best_score = None, -1
    for tid, tname in candidates.items():
        n = norm(tname)
        score = 0
        if target == n:
            score = 100
        else:
            if n.startswith(target) or target.startswith(n):
                score = 80
            elif target in n or n in target:
                score = 70
            t_words = set(n.split())
            s_words = set(target.split())
            score += 10 * len(t_words & s_words)
        if score > best_score:
            best_id, best_score = tid, score
    return best_id

def rapid_get(url: str, headers: dict, params: dict, debug=False) -> dict:
    if debug:
        print(f"[injuries][DEBUG] GET {url} params={params}")
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

# -------------- Core -------------------

def fetch_team_id(api_host: str, headers: dict, team_name: str, cache: dict, debug=False) -> Optional[int]:
    if team_name in cache:
        return cache[team_name]
    data = rapid_get(f"https://{api_host}/v3/teams", headers, {"search": team_name}, debug)
    candidates = {}
    for item in data.get("response", []):
        tid = item.get("team", {}).get("id")
        tname = item.get("team", {}).get("name")
        if tid and tname:
            candidates[int(tid)] = str(tname)
    tid = best_match_id(team_name, candidates) if candidates else None
    cache[team_name] = tid
    if debug:
        print(f"[injuries][DEBUG] team '{team_name}' -> id={tid}")
    # rate-limit gentle
    time.sleep(0.35)
    return tid

def fetch_injuries(api_host: str, headers: dict, team_id: int, season: str, debug=False) -> Tuple[int, str]:
    data = rapid_get(f"https://{api_host}/v3/injuries", headers, {"team": team_id, "season": season}, debug)
    players = []
    for item in data.get("response", []):
        p = item.get("player") or {}
        reason = item.get("information") or item.get("type") or ""
        name = p.get("name") or "N/A"
        players.append(f"{name}({reason})")
    top = ", ".join(players[:5]) if players else ""
    count = len(players)
    time.sleep(0.35)
    return count, top

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, help="Diretório de saída (ex.: data/out/<id>)")
    ap.add_argument("--season", required=True, help="Ano da temporada (YYYY)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "injuries.csv")

    # 1) Pré-checagens endurecidas
    src_matches = "data/in/matches_source.csv"
    if not os.path.isfile(src_matches):
        print(f"::error::[injuries] Entrada obrigatória ausente: {src_matches}", file=sys.stderr)
        sys.exit(6)

    rapid_key = os.getenv("X_RAPIDAPI_KEY", "").strip()
    if not rapid_key:
        print("::error::[injuries] X_RAPIDAPI_KEY não definido. Configure o secret para usar API-Football.", file=sys.stderr)
        sys.exit(6)

    api_host = "v3.football.api-sports.io"
    headers = {
        "x-rapidapi-key": rapid_key,
        "x-rapidapi-host": api_host,
    }

    # 2) Carrega jogos (sem inventar nada)
    rows = []
    with open(src_matches, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        # Checagem de cabeçalho mínimo
        for col in ("match_id", "home", "away"):
            if col not in rd.fieldnames:
                print(f"::error::[injuries] Cabeçalho obrigatório '{col}' ausente em {src_matches}", file=sys.stderr)
                sys.exit(6)
        for r in rd:
            if not r.get("home") or not r.get("away"):
                continue
            rows.append({
                "match_id": r.get("match_id", "").strip(),
                "home": r["home"].strip(),
                "away": r["away"].strip(),
                "source": (r.get("source") or "").strip()
            })
    if not rows:
        print("::error::[injuries] Nenhuma linha válida em data/in/matches_source.csv", file=sys.stderr)
        sys.exit(6)

    # 3) Consulta API (endurecida)
    team_cache: Dict[str, Optional[int]] = {}
    generated_at = datetime.utcnow().isoformat()
    out_rows = []
    api_hits = 0  # será >0 somente se recebermos respostas da API (teams/injuries OK)

    try:
        for r in rows:
            home = r["home"]; away = r["away"]

            tid_home = fetch_team_id(api_host, headers, home, team_cache, args.debug)
            tid_away = fetch_team_id(api_host, headers, away, team_cache, args.debug)

            inj_count_home = inj_count_away = 0
            key_home = key_away = ""

            # Consideramos "hit" quando a chamada retorna 200 OK com JSON plausível
            if tid_home:
                c, k = fetch_injuries(api_host, headers, tid_home, args.season, args.debug)
                inj_count_home, key_home = c, k
                api_hits += 1
            if tid_away:
                c, k = fetch_injuries(api_host, headers, tid_away, args.season, args.debug)
                inj_count_away, key_away = c, k
                api_hits += 1

            out_rows.append({
                "match_id": r["match_id"],
                "home": home,
                "away": away,
                "inj_count_home": inj_count_home,
                "inj_count_away": inj_count_away,
                "key_out_home": key_home,
                "key_out_away": key_away,
                "api_hits": api_hits,
                "generated_at": generated_at,
                "source": "api-football"
            })
    except requests.HTTPError as e:
        print(f"::error::[injuries] HTTPError ao consultar API-Football: {e}", file=sys.stderr)
        sys.exit(6)
    except requests.RequestException as e:
        print(f"::error::[injuries] Erro de rede ao consultar API-Football: {e}", file=sys.stderr)
        sys.exit(6)
    except Exception as e:
        print(f"::error::[injuries] Falha inesperada: {e}", file=sys.stderr)
        sys.exit(6)

    # 4) Garante que tivemos ao menos 1 “hit” real da API
    if api_hits == 0:
        print("::error::[injuries] Nenhuma resposta útil obtida da API-Football (api_hits=0). Abortando.", file=sys.stderr)
        sys.exit(6)

    # 5) Escreve CSV final (somente se tudo OK)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "match_id","home","away",
            "inj_count_home","inj_count_away",
            "key_out_home","key_out_away",
            "api_hits","generated_at","source"
        ])
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"[injuries] OK -> {out_csv} linhas={len(out_rows)} hits={api_hits}")

if __name__ == "__main__":
    main()