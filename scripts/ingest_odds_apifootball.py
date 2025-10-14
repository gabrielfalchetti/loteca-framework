#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestão de "odds" via API-Football (API-Sports).
Observação: muitas contas/planos não têm endpoint de odds habilitado.
Este script então foca em:
  - Resolver times/fixtures a partir de nomes BR (com aliases),
  - E sempre gerar um CSV com cabeçalho, mesmo se 0 linhas (evita falha no consenso).

Saída: {rodada}/odds_apifootball.csv com colunas:
  team_home,team_away,odds_home,odds_draw,odds_away

Quando não houver odds, só o cabeçalho é gravado.
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta
import time
import json
import re
from typing import Optional, Tuple, Dict, Any, List

import requests
from unidecode import unidecode


API_BASE = "https://v3.football.api-sports.io"
API_KEY_ENV = "API_FOOTBALL_KEY"


# -------- Normalização BR (aliases enxutos que cobrem seu caso atual) --------
ALIAS_MAP = {
    # clubes nacionais com sufixos de estado/siglas
    "athletico-pr": "athletico paranaense",
    "atletico-pr": "athletico paranaense",
    "atlético-pr": "athletico paranaense",

    "atletico-go": "atletico goianiense",
    "atlético-go": "atletico goianiense",

    "botafogo-sp": "botafogo ribeirao preto",
    "botafogo ribeirão preto": "botafogo ribeirao preto",

    "ferroviaria": "ferroviaria",
    "ferroviária": "ferroviaria",

    "avai": "avai",
    "avaí": "avai",

    "chapecoense": "chapecoense sc",
    "chapecoense-sc": "chapecoense sc",

    "volta redonda": "volta redonda",

    "paysandu": "paysandu",
    "paysandu sc": "paysandu",

    "remo": "remo",
    "clube do remo": "remo",

    # mantenha alguns comuns sem acento
    "crb": "crb",
}


def norm_team_br(name: str) -> str:
    if not name:
        return ""
    t = unidecode(name).lower().strip()
    # padroniza separadores
    t = t.replace(" - ", "-")
    t = re.sub(r"[^\w\s\-]", " ", t)  # remove pontuação
    t = re.sub(r"\s+", " ", t).strip()
    return ALIAS_MAP.get(t, t)


def read_matches_csv(path: str) -> List[Dict[str, str]]:
    """
    Aceita cabeçalhos: match_id,home,away (preferido)
    Também tolera: match_id,team_home,team_away
    """
    rows = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        hdr = [c.strip().lower() for c in reader.fieldnames or []]
        # mapeia nomes
        col_home = "home" if "home" in hdr else "team_home" if "team_home" in hdr else None
        col_away = "away" if "away" in hdr else "team_away" if "team_away" in hdr else None
        col_id = "match_id" if "match_id" in hdr else None
        if not (col_home and col_away and col_id):
            raise SystemExit(f"[apifootball][CRITICAL] CSV {path} precisa de colunas match_id,home,away (ou team_home/team_away).")

        for r in reader:
            rows.append({
                "match_id": r.get(col_id, "").strip(),
                "home": r.get(col_home, "").strip(),
                "away": r.get(col_away, "").strip(),
            })
    return rows


def api_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    key = os.environ.get(API_KEY_ENV, "").strip()
    if not key:
        raise SystemExit("[apifootball][CRITICAL] API_FOOTBALL_KEY ausente no ambiente.")
    headers = {
        "x-apisports-key": key,
        "Accept": "application/json",
    }
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=25)
    if r.status_code == 429:
        # rate limit → aguarda e tenta 1x
        time.sleep(2.5)
        r = requests.get(url, headers=headers, params=params, timeout=25)
    r.raise_for_status()
    return r.json()


def find_team_id_by_name(name: str) -> Optional[int]:
    if not name:
        return None
    q = norm_team_br(name)
    try:
        data = api_get("/teams", {"search": q})
        for it in data.get("response", []):
            tname = it.get("team", {}).get("name") or ""
            # Valida pelo normalizado
            if norm_team_br(tname) == q:
                return it.get("team", {}).get("id")
        # fallback: primeiro resultado
        if data.get("response"):
            return data["response"][0].get("team", {}).get("id")
    except Exception:
        return None
    return None


def fixtures_next_by_team(team_id: int, season: Optional[int]) -> List[Dict[str, Any]]:
    params = {"team": team_id, "next": 20}
    if season:
        params["season"] = season
    try:
        data = api_get("/fixtures", params)
        return data.get("response", []) or []
    except Exception:
        return []


def find_fixture_by_window(home_id: int, away_id: int, since: datetime, until: datetime, season: Optional[int]) -> Optional[Dict[str, Any]]:
    # tenta por intervalo
    params = {
        "season": season or datetime.utcnow().year,
        "from": since.strftime("%Y-%m-%d"),
        "to": until.strftime("%Y-%m-%d"),
        "team": home_id,
    }
    try:
        data = api_get("/fixtures", params)
        for fx in data.get("response", []):
            th = fx.get("teams", {}).get("home", {}).get("id")
            ta = fx.get("teams", {}).get("away", {}).get("id")
            if th == home_id and ta == away_id:
                return fx
    except Exception:
        pass
    return None


def find_fixture_h2h(home_id: int, away_id: int, season: Optional[int]) -> Optional[Dict[str, Any]]:
    # procura nos "próximos 20" de cada lado
    for tid, oid in ((home_id, away_id), (away_id, home_id)):
        for fx in fixtures_next_by_team(tid, season):
            th = fx.get("teams", {}).get("home", {}).get("id")
            ta = fx.get("teams", {}).get("away", {}).get("id")
            if th == home_id and ta == away_id:
                return fx
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída (ex: data/out/12345)")
    ap.add_argument("--source_csv", required=True, help="CSV de entrada com match_id,home,away (normalizado)")
    ap.add_argument("--season", type=int, default=int(os.environ.get("SEASON", "0")) or None)
    args = ap.parse_args()

    os.makedirs(args.rodada, exist_ok=True)
    out_csv = os.path.join(args.rodada, "odds_apifootball.csv")

    matches = read_matches_csv(args.source_csv)
    print(f"[apifootball]Iniciando busca direcionada para {len(matches)} jogos do arquivo de origem.")

    # Cabeçalho + acumula linhas (se odds existirem)
    rows_out: List[Dict[str, Any]] = []

    # âncora de tempo
    now = datetime.utcnow()
    lookahead = int(os.environ.get("LOOKAHEAD_DAYS", "3") or 3)

    for m in matches:
        home_raw, away_raw = m["home"], m["away"]
        home = norm_team_br(home_raw)
        away = norm_team_br(away_raw)

        home_id = find_team_id_by_name(home)
        away_id = find_team_id_by_name(away)

        if not home_id or not away_id:
            print(f"[apifootball][WARN] Sem team_id para: {home_raw} vs {away_raw}")
            continue

        # 1) tenta por janela curta
        since = now
        until = now + timedelta(days=lookahead)
        fx = find_fixture_by_window(home_id, away_id, since, until, args.season)

        # 2) fallback: janela maior
        if not fx:
            fx = find_fixture_by_window(home_id, away_id, since, now + timedelta(days=14), args.season)

        # 3) fallback: próximos 20 (H2H)
        if not fx:
            fx = find_fixture_h2h(home_id, away_id, args.season)

        if not fx:
            print(f"[apifootball][WARN] Sem fixture_id para: {home_raw} vs {away_raw}")
            continue

        # Odds pelo API-Football exigem plano/endpoint específico (nem sempre disponível).
        # Para manter compatibilidade com o consenso, não vamos falhar —
        # apenas NÃO adicionamos linhas (mas escreveremos cabeçalho no CSV final).
        # Se quiser tentar odds no seu plano, descomente / adapte abaixo:
        #
        # fixture_id = fx.get("fixture", {}).get("id")
        # try:
        #     data_odds = api_get("/odds", {"fixture": fixture_id})
        #     # ... transformar em odds_home/odds_draw/odds_away e append em rows_out
        # except Exception:
        #     pass

    # Garante arquivo com cabeçalho SEMPRE
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["team_home", "team_away", "odds_home", "odds_draw", "odds_away"])
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    print(f"[apifootball]Arquivo odds_apifootball.csv gerado com {len(rows_out)} jogos encontrados.")


if __name__ == "__main__":
    main()