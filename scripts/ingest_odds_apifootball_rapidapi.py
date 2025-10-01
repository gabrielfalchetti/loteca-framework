# scripts/ingest_odds_apifootball_rapidapi.py
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List, Iterable, Tuple

import requests

from scripts.csv_utils import ensure_dir, write_csv_rows


BASE_URL = "https://api-football-v1.p.rapidapi.com/v3"
HOST = "api-football-v1.p.rapidapi.com"


def _headers() -> Dict[str, str]:
    key = os.environ.get("RAPIDAPI_KEY", "").strip()
    if not key:
        print("[apifootball] ERRO: variável de ambiente RAPIDAPI_KEY ausente.", file=sys.stderr)
        sys.exit(7)
    # Somente os 2 cabeçalhos exigidos
    return {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": HOST,
    }


def rapid_get(endpoint: str, params: Dict[str, Any] | None = None, timeout: int = 30) -> Dict[str, Any]:
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    r = requests.get(url, headers=_headers(), params=params or {}, timeout=timeout)
    # Levanta erros explícitos para 4xx/5xx (facilita depuração no CI)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        # Tenta imprimir corpo para diagnósticos (limites/401/etc.)
        body = None
        try:
            body = r.json()
        except Exception:
            body = r.text[:400]
        print(f"[apifootball] HTTP {r.status_code} em {url} params={params} body={body}", file=sys.stderr)
        raise
    return r.json()


def parse_rodada_date(rodada: str) -> str:
    """
    Extrai a data YYYY-MM-DD do prefixo da rodada, ex.: 2025-09-27_1213 -> 2025-09-27
    """
    try:
        return rodada.split("_", 1)[0]
    except Exception:
        return rodada  # fallback


def collect_fixtures(date_str: str, season: int, leagues: Iterable[int], debug: bool = False) -> List[Dict[str, Any]]:
    fixtures: List[Dict[str, Any]] = []
    for league_id in leagues:
        params = {"date": date_str, "league": league_id, "season": season}
        if debug:
            print(f"[apifootball] buscando fixtures: {params}")
        data = rapid_get("fixtures", params=params)
        r = data.get("response", [])
        if debug:
            print(f"[apifootball] fixtures liga {league_id}: {len(r)}")
        fixtures.extend(r)
        time.sleep(0.5)  # evita throttling agressivo
    return fixtures


def extract_match_winner_odds(odds_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    A resposta /odds traz bookmakers -> bets (ex.: 'Match Winner'/'1X2').
    Extraímos home/draw/away quando disponível.
    """
    out: List[Dict[str, Any]] = []
    resp = odds_payload.get("response", [])
    for item in resp:
        fixture = item.get("fixture", {}) or {}
        fixture_id = fixture.get("id")
        league = item.get("league", {}) or {}
        league_id = league.get("id")
        league_name = league.get("name")
        update_ts = item.get("update")

        for bm in item.get("bookmakers", []) or []:
            bookmaker_name = bm.get("name")
            for bet in bm.get("bets", []) or []:
                bet_name = (bet.get("name") or "").lower()
                if bet_name not in {"match winner", "1x2", "match_winner"}:
                    continue
                home_odd = draw_odd = away_odd = None
                for v in bet.get("values", []) or []:
                    label = (v.get("value") or "").lower()
                    odd = v.get("odd")
                    if label in {"home", "1"}:
                        home_odd = odd
                    elif label in {"draw", "x"}:
                        draw_odd = odd
                    elif label in {"away", "2"}:
                        away_odd = odd

                row = {
                    "fixture_id": fixture_id,
                    "league_id": league_id,
                    "league_name": league_name,
                    "bookmaker": bookmaker_name,
                    "market": "1x2",
                    "home_odds": home_odd,
                    "draw_odds": draw_odd,
                    "away_odds": away_odd,
                    "updated": update_ts,
                    "source": "apifootball",
                }
                out.append(row)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingesta de odds via API-FOOTBALL (RapidAPI)")
    ap.add_argument("--rodada", required=True, help="ex.: 2025-09-27_1213")
    ap.add_argument("--season", type=int, default=int(os.getenv("SEASON", "2025")))
    ap.add_argument("--debug", action="store_true")

    # Flags legadas (mantidas só para compatibilidade com o workflow atual)
    ap.add_argument("--window", type=int, default=2)
    ap.add_argument("--fuzzy", type=float, default=0.90)
    ap.add_argument("--aliases", default="data/aliases_br.json")

    args = ap.parse_args()

    # Saídas
    out_dir = os.path.join("data", "out", args.rodada)
    out_path = os.path.join(out_dir, "odds_apifootball.csv")
    unmatched_path = os.path.join(out_dir, "unmatched_apifootball.csv")
    debug_dir = os.path.join(out_dir, "debug", "apifootball")
    ensure_dir(out_path)
    ensure_dir(unmatched_path)
    ensure_dir(os.path.join(debug_dir, "x.json"))

    # Data alvo (da RODADA) e ligas de interesse: Série A (71) e Série B (72)
    date_str = parse_rodada_date(args.rodada)
    leagues = [71, 72]

    if args.debug:
        print(f"[apifootball] rodando com RODADA={args.rodada} SEASON={args.season} DATA={date_str}")

    # 1) Coleta fixtures do dia nas ligas alvo
    fixtures = collect_fixtures(date_str, args.season, leagues, debug=args.debug)

    # Dump de diagnóstico
    with open(os.path.join(debug_dir, "fixtures.json"), "w", encoding="utf-8") as f:
        json.dump(fixtures, f, ensure_ascii=False, indent=2)

    # 2) Para cada fixture, consulta odds
    all_rows: List[Dict[str, Any]] = []
    for fx in fixtures:
        fixture = fx.get("fixture", {}) or {}
        fixture_id = fixture.get("id")
        if not fixture_id:
            continue

        try:
            od = rapid_get("odds", params={"fixture": fixture_id})
        except requests.HTTPError:
            # segue para próxima — pode ser limite/indisponível p/ fixture
            continue

        if args.debug:
            print(f"[apifootball] odds fixture={fixture_id} ok")

        # guarda bruto para depuração
        with open(os.path.join(debug_dir, f"odds_{fixture_id}.json"), "w", encoding="utf-8") as f:
            json.dump(od, f, ensure_ascii=False, indent=2)

        rows = extract_match_winner_odds(od)
        all_rows.extend(rows)

        time.sleep(0.4)  # evita rate limit

    # 3) Escreve CSV de saída (mesmo se vazio, para o workflow ter artefato)
    fields = [
        "fixture_id",
        "league_id",
        "league_name",
        "bookmaker",
        "market",
        "home_odds",
        "draw_odds",
        "away_odds",
        "updated",
        "source",
    ]
    n = write_csv_rows(out_path, all_rows, fieldnames=fields)

    # 4) Cria um unmatched vazio (compatibilidade com upload de artefatos)
    write_csv_rows(unmatched_path, [], fieldnames=["reason", "context"])

    print(f"[apifootball] OK -> {out_path} ({n} linhas)")
    sys.exit(0)


if __name__ == "__main__":
    main()
