#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera data/in/matches_whitelist.csv (ou caminho passado em --out) a partir da TheOddsAPI,
filtrando EXCLUSIVAMENTE eventos de futebol (soccer_*).

Saída mínima exigida pelo pipeline:
- match_id,home,away

Uso típico:
  python scripts/build_whitelist_from_apis.py \
      --out data/in/matches_whitelist.csv \
      --regions "uk,eu,us,au" \
      --lookahead-days 3 \
      --sports soccer \
      --debug

Requisitos:
- Variável de ambiente THEODDS_API_KEY definida.
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

try:
    import requests
except ImportError:
    print("[whitelist][ERRO] 'requests' não está instalado. Adicione 'pip install requests' na etapa de setup.", file=sys.stderr)
    sys.exit(2)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# --------------------------
# Utilidades
# --------------------------
def to_commence_time_to(days_a_frente: int) -> str:
    """
    A TheOddsAPI requer 'YYYY-MM-DDTHH:MM:SSZ' (sempre Z no fim, sem offset tipo +00:00).
    """
    dt = datetime.now(timezone.utc) + timedelta(days=days_a_frente)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def mk_match_id(home: str, away: str) -> str:
    base = f"{home}__{away}".lower().encode("utf-8")
    return hashlib.md5(base).hexdigest()[:12]


def load_aliases(path: str) -> Dict[str, str]:
    """
    aliases.json opcional no formato:
    {
      "teams": {
        "Man Utd": "Manchester United",
        "CR Flamengo": "Flamengo"
      }
    }
    """
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        teams = data.get("teams", {}) or {}
        # normaliza chaves para comparação case-insensitive:
        return {k.strip().lower(): v.strip() for k, v in teams.items() if isinstance(k, str) and isinstance(v, str)}
    except Exception as ex:
        eprint(f"[whitelist][WARN] Não consegui ler aliases de {path} ({ex}); seguindo sem.")
        return {}


def apply_alias(name: str, aliases: Dict[str, str]) -> str:
    if not name:
        return name
    return aliases.get(name.strip().lower(), name.strip())


def pick_home_away(ev: Dict[str, Any]) -> Tuple[str, str]:
    """
    Eventos da TheOddsAPI (v4) trazem:
      - 'home_team'
      - 'away_team'
      - 'teams' (lista)
    Preferimos home_team/away_team; se faltarem, tentamos inferir com 'teams'.
    """
    home = ev.get("home_team") or ""
    away = ev.get("away_team") or ""
    if home and away:
        return str(home), str(away)

    teams = ev.get("teams") or []
    if isinstance(teams, list) and len(teams) == 2:
        # Sem garantia de ordem, mas tentamos manter consistência:
        return str(teams[0]), str(teams[1])

    return "", ""


# --------------------------
# Coleta TheOddsAPI
# --------------------------
def fetch_upcoming_odds(api_key: str, regions: str, lookahead_days: int, debug: bool) -> List[Dict[str, Any]]:
    """
    Consulta /v4/sports/upcoming/odds com filtros de mercado h2h e janela de tempo.
    """
    base = "https://api.the-odds-api.com/v4/sports/upcoming/odds"
    params = {
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
        "apiKey": api_key,
        "commenceTimeTo": to_commence_time_to(lookahead_days),
    }
    if debug:
        print(f"[whitelist][theodds] GET {base} {params}")

    resp = requests.get(base, params=params, timeout=30)
    if resp.status_code != 200:
        eprint(f"[whitelist][theodds][ERRO] HTTP {resp.status_code}: {resp.text}")
        return []

    try:
        data = resp.json()
    except Exception as ex:
        eprint(f"[whitelist][theodds][ERRO] JSON inválido: {ex}")
        return []

    if debug:
        print(f"[whitelist][theodds] Eventos recebidos: {len(data)}")
    return data if isinstance(data, list) else []


# --------------------------
# Filtro exclusivo de futebol
# --------------------------
def filter_soccer_only(events: List[Dict[str, Any]], debug: bool) -> List[Dict[str, Any]]:
    """
    Mantém apenas eventos com sport_key inicando em 'soccer_'.
    """
    out = []
    for ev in events:
        skey = str(ev.get("sport_key", ""))
        if skey.startswith("soccer_"):
            out.append(ev)
    if debug:
        print(f"[whitelist] Pós-filtro soccer_: {len(out)} eventos (de {len(events)})")
    return out


# --------------------------
# Construção da whitelist
# --------------------------
def build_whitelist_rows(events: List[Dict[str, Any]], aliases: Dict[str, str], debug: bool) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for ev in events:
        home, away = pick_home_away(ev)
        home, away = home.strip(), away.strip()

        if not home or not away:
            if debug:
                eprint(f"[whitelist][SKIP] evento sem times: {ev.get('id') or ev.get('sport_key')}")
            continue

        # aplica aliases
        home = apply_alias(home, aliases)
        away = apply_alias(away, aliases)

        # Garante que há odds H2H disponíveis por pelo menos uma casa (opcional, mas útil)
        bookmakers = ev.get("bookmakers") or []
        if not bookmakers:
            if debug:
                eprint(f"[whitelist][SKIP] sem bookmakers: {home} x {away}")
            continue

        mid = mk_match_id(home, away)
        rows.append({"match_id": mid, "home": home, "away": away})

    # Dedup (se vierem duplicados do endpoint)
    uniq = {}
    for r in rows:
        uniq[(r["match_id"])] = r
    final = list(uniq.values())

    if debug:
        print(f"[whitelist] linhas finais (soccer): {len(final)}")
    return final


# --------------------------
# Main
# --------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Caminho do CSV de saída (ex.: data/in/matches_whitelist.csv)")
    parser.add_argument("--regions", required=True, help="Regiões TheOddsAPI (ex.: uk,eu,us,au)")
    parser.add_argument("--lookahead-days", type=int, default=3, help="Janela em dias à frente para buscar jogos (padrão: 3)")
    parser.add_argument("--sports", default="soccer", help="Sempre 'soccer' aqui; mantido como flag para compatibilidade")
    parser.add_argument("--aliases", default="data/in/aliases.json", help="Arquivo de aliases opcional (padrão: data/in/aliases.json)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    api_key = os.getenv("THEODDS_API_KEY", "").strip()
    if not api_key:
        eprint("[whitelist][ERRO] THEODDS_API_KEY não definido no ambiente.")
        sys.exit(3)

    if args.sports.strip().lower() != "soccer":
        # Mesmo que passem outra coisa, forçamos soccer:
        if args.debug:
            eprint(f"[whitelist][WARN] --sports='{args.sports}' ignorado. Usando 'soccer' (apenas futebol).")

    aliases = load_aliases(args.aliases)

    # 1) Coleta bruta
    events = fetch_upcoming_odds(api_key, args.regions, args.lookahead_days, args.debug)

    # 2) Aplica filtro exclusivo de futebol
    events_soccer = filter_soccer_only(events, args.debug)

    # 3) Constrói linhas da whitelist
    rows = build_whitelist_rows(events_soccer, aliases, args.debug)

    # 4) Valida e grava
    out_path = args.out
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    if not rows:
        eprint("[whitelist][ERRO] Nenhum jogo de futebol encontrado na janela/regions fornecidas.")
        # Mesmo assim, escrevemos um cabeçalho mínimo para depuração (o step chamador fará o teste -s e falhará)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["match_id", "home", "away"])
            w.writeheader()
        sys.exit(4)

    # grava CSV
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["match_id", "home", "away"])
        w.writeheader()
        w.writerows(rows)

    if args.debug:
        print("===== Preview whitelist (até 20 linhas) =====")
        for i, r in enumerate(rows[:20], start=1):
            print(f"{i:02d}. {r['match_id']},{r['home']},{r['away']}")

    print(f"[whitelist] OK -> {out_path}  linhas={len(rows)}")


if __name__ == "__main__":
    main()