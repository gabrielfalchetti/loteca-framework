# scripts/ingest_odds_theoddsapi_safe.py
# -*- coding: utf-8 -*-
"""
Coleta odds H2H do TheOddsAPI com parsing defensivo.
Gera: {OUT_DIR}/odds_theoddsapi.csv
Colunas: match_id,home,away,region,sport,odds_home,odds_draw,odds_away,last_update,source

Uso:
  python scripts/ingest_odds_theoddsapi_safe.py --rodada data/out/1234567890 --regions "uk,eu,us,au" --debug
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any

import pandas as pd
import requests


API_URL = "https://api.the-odds-api.com/v4/sports/soccer/odds"
SOURCE_TAG = "theoddsapi"


def log(msg: str, debug: bool = False) -> None:
    if debug:
        print(msg, flush=True)


def fetch_one_region(api_key: str, region: str, debug: bool) -> List[Dict[str, Any]]:
    params = {
        "apiKey": api_key,
        "regions": region,
        "markets": "h2h",
    }
    log(f"[theoddsapi][DEBUG] GET {API_URL} {params}", debug)
    r = requests.get(API_URL, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    return data


def safe_get_h2h_prices(event: Dict[str, Any], debug: bool) -> Tuple[float, float, float]:
    """
    Retorna (odds_home, odds_draw, odds_away) com parsing defensivo.
    Caso não encontre algum preço, retorna None naquele campo.
    """
    odds_home = odds_draw = odds_away = None

    bookmakers = event.get("bookmakers") or []
    if not bookmakers:
        return odds_home, odds_draw, odds_away

    # Varre todos os bookmakers/markets e pega o 1º conjunto completo que achar
    for bk in bookmakers:
        markets = bk.get("markets") or []
        for mk in markets:
            # markets[i].key deve ser "h2h" — garantimos por query, mas seguimos defensivos
            if (mk.get("key") or "").lower() != "h2h":
                continue
            outcomes = mk.get("outcomes") or []
            # outcomes pode vir com nomes variados: "Home", "Draw", "Away" OU nomes dos clubes
            # Estratégia: se houver Home/Draw/Away usamos; senão, tentamos mapear por home_team/away_team.
            names_lower = {str(o.get("name", "")).strip().lower(): o for o in outcomes}

            # 1) Tentativa por rótulos Home/Draw/Away
            if "home" in names_lower or "away" in names_lower or "draw" in names_lower:
                if "home" in names_lower:
                    odds_home = names_lower["home"].get("price")
                if "draw" in names_lower:
                    odds_draw = names_lower["draw"].get("price")
                if "away" in names_lower:
                    odds_away = names_lower["away"].get("price")

            # 2) Tentativa por nomes dos times
            if (odds_home is None or odds_away is None) and outcomes:
                home_team = str(event.get("home_team", "")).strip().lower()
                away_team = str(event.get("away_team", "")).strip().lower()
                # mapeia exato por nome
                for o in outcomes:
                    name = str(o.get("name", "")).strip().lower()
                    if not name:
                        continue
                    if odds_home is None and name == home_team:
                        odds_home = o.get("price")
                    if odds_away is None and name == away_team:
                        odds_away = o.get("price")
                # empates às vezes aparecem como "Tie" ou "Empate"
                if odds_draw is None:
                    for o in outcomes:
                        name = str(o.get("name", "")).strip().lower()
                        if name in {"draw", "tie", "empate"}:
                            odds_draw = o.get("price")
                            break

            # Se coletamos ao menos um preço, já retornamos (não bloqueia se faltou algum — workflow lida)
            if odds_home is not None or odds_draw is not None or odds_away is not None:
                return odds_home, odds_draw, odds_away

    return odds_home, odds_draw, odds_away


def normalize_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    parser.add_argument("--regions", required=False, default="uk,eu,us,au", help="Regiões do TheOddsAPI separadas por vírgula")
    parser.add_argument("--debug", action="store_true", help="Logs verbosos")
    args = parser.parse_args()

    api_key = os.environ.get("THEODDS_API_KEY", "")
    if not api_key:
        print("::error::THEODDS_API_KEY não definido em Secrets", flush=True)
        sys.exit(4)

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    regions = [r.strip() for r in (args.regions or "").split(",") if r.strip()]
    if not regions:
        regions = ["uk"]

    rows: List[Dict[str, Any]] = []
    hits = 0
    for region in regions:
        try:
            events = fetch_one_region(api_key, region, args.debug)
        except requests.RequestException as e:
            print(f"::warning::[theoddsapi] Falha ao buscar região '{region}': {e}", flush=True)
            continue

        for ev in events:
            home = normalize_str(ev.get("home_team"))
            away = normalize_str(ev.get("away_team"))
            sport = normalize_str(ev.get("sport_key") or ev.get("sport_title") or "soccer")

            # Tenta odds
            oh, od, oa = safe_get_h2h_prices(ev, args.debug)

            # Se todas None, ignora esse evento
            if oh is None and od is None and oa is None:
                continue

            # last_update preferindo commence_time do evento, se existir
            commence = normalize_str(ev.get("commence_time"))
            if commence:
                # normaliza para ISO curta se possível
                try:
                    dt = datetime.fromisoformat(commence.replace("Z", "+00:00")).astimezone(timezone.utc)
                    last_update = dt.replace(microsecond=0).isoformat()
                except Exception:
                    last_update = commence
            else:
                last_update = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

            # match_id amigável e estável por casa__fora
            match_id = f"{home}__{away}" if home and away else normalize_str(ev.get("id") or ev.get("event_id") or f"{int(time.time())}")

            rows.append({
                "match_id": match_id,
                "home": home,
                "away": away,
                "region": region,
                "sport": sport,
                "odds_home": oh,
                "odds_draw": od,
                "odds_away": oa,
                "last_update": last_update,
                "source": SOURCE_TAG,
            })
            hits += 1

    if not rows:
        # Gera CSV vazio porém com cabeçalho, para evitar falhas em etapas posteriores.
        out_path = os.path.join(out_dir, "odds_theoddsapi.csv")
        empty_df = pd.DataFrame(columns=[
            "match_id", "home", "away", "region", "sport",
            "odds_home", "odds_draw", "odds_away",
            "last_update", "source",
        ])
        empty_df.to_csv(out_path, index=False)
        print(f"[theoddsapi] AVISO: nenhum dado retornado. CSV vazio criado -> {out_path}")
        sys.exit(0)

    # Dedup básica (último por match_id+region, mantendo a linha mais recente por last_update)
    df = pd.DataFrame(rows)
    # Tenta converter odds para float
    for col in ("odds_home", "odds_draw", "odds_away"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ordena por last_update (quando possível) e faz drop_duplicates
    def _to_dt(s):
        try:
            return pd.to_datetime(s, utc=True)
        except Exception:
            return pd.NaT

    df["_dt"] = df["last_update"].apply(_to_dt)
    df = df.sort_values(["match_id", "region", "_dt"], ascending=[True, True, False])
    df = df.drop(columns=["_dt"]).drop_duplicates(subset=["match_id", "region"], keep="first")

    out_path = os.path.join(out_dir, "odds_theoddsapi.csv")
    df.to_csv(out_path, index=False)

    print(f"[theoddsapi] OK -> {out_path} (hits={len(df)})")
    # pequena prévia
    try:
        print(df.head(10).to_csv(index=False))
    except Exception:
        pass


if __name__ == "__main__":
    main()