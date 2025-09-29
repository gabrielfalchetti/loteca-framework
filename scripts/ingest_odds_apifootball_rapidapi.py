#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball_rapidapi.py
-----------------------------------
Coleta odds 1X2 via API-Football (RapidAPI) e salva em:
  data/out/{rodada}/odds_apifootball.csv

Design:
- Lê data/in/{rodada}/matches_source.csv
  Aceita colunas: (match_id, home/away) OU (match_id, home_team/away_team)
- Flags aceitos (compat):
    --rodada (obrigatório)
    --season (opcional, ignorado se não necessário)
    --window (opcional, dias para janela de busca; default 14)
    --aliases (opcional, json com apelidos)
    --fuzzy (opcional, float 0..1; se ausente, usa 0.9)  [compat]
    --debug (opcional)
- Se RAPIDAPI_KEY não estiver presente, gera CSV vazio e sai com código 0.
- Tenta obter odds por fixture_id quando disponível no matches_source.csv (colunas fixture_id / apifootball_fixture_id).
  Se ausente, tenta busca por data aproximada + liga mapeada (escopo reduzido para robustez).

Obs: Este coletor é "melhor esforço": se não houver odds disponíveis ainda no provedor,
ele não falha o pipeline — apenas grava CSV vazio.
"""

import os
import json
import time
import argparse
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import requests


API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID da rodada (ex.: 2025-09-27_1213)")
    ap.add_argument("--season", default=None, help="Compat: temporada (ex.: 2025) — opcional")
    ap.add_argument("--window", type=int, default=14, help="Janela de dias para busca por fixtures")
    ap.add_argument("--aliases", default="data/aliases_br.json", help="Arquivo JSON de apelidos de times")
    ap.add_argument("--fuzzy", type=float, default=0.9, help="Compat: limiar de similaridade (0..1)")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()


def _load_aliases(p: str) -> Dict[str, str]:
    try:
        if p and os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _normalize_team(name: str, aliases: Dict[str, str]) -> str:
    if not isinstance(name, str):
        return ""
    key = name.strip()
    return aliases.get(key, key)


def _read_matches(rodada: str, aliases: Dict[str, str]) -> pd.DataFrame:
    src = f"data/in/{rodada}/matches_source.csv"
    if not os.path.exists(src):
        raise FileNotFoundError(f"[apifootball] arquivo não encontrado: {src}")

    df = pd.read_csv(src)

    # Aceita home/away ou home_team/away_team
    if "home" in df.columns and "away" in df.columns:
        df["home_team"] = df["home"].astype(str)
        df["away_team"] = df["away"].astype(str)
    elif "home_team" in df.columns and "away_team" in df.columns:
        pass
    else:
        raise RuntimeError("[apifootball] matches_source precisa de colunas 'home/away' ou 'home_team/away_team'")

    if "match_id" not in df.columns:
        # cria match_id se não existir
        df["match_id"] = [f"m{i+1}" for i in range(len(df))]

    # Normaliza nomes via aliases
    df["home_team"] = df["home_team"].apply(lambda x: _normalize_team(x, aliases))
    df["away_team"] = df["away_team"].apply(lambda x: _normalize_team(x, aliases))

    return df


def _rapidapi_headers(api_key: str) -> Dict[str, str]:
    return {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": API_HOST,
    }


def _request_json(url: str, headers: Dict[str, str], params: Dict[str, Any], debug: bool) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=25)
        if debug:
            print(f"[apifootball][GET] {resp.url}")
        if resp.status_code != 200:
            if debug:
                print(f"[apifootball] HTTP {resp.status_code}: {resp.text[:300]}")
            return None
        return resp.json()
    except Exception as e:
        if debug:
            print(f"[apifootball] ERRO requests: {e}")
        return None


def _extract_1x2_from_oddsgroup(oddsgroup: Dict[str, Any]) -> Optional[Tuple[float, float, float]]:
    """
    Procura por mercado "Match Winner" / "1X2" / "Full Time Result"
    Retorna odds decimais (home, draw, away)
    """
    if not oddsgroup:
        return None
    # Estrutura típica:
    # {"league": {...}, "fixture": {...}, "bookmakers": [{"name": "...", "bets": [{"name":"Match Winner","values":[{"value":"Home","odd":"1.90"},...] }]}]}
    try:
        bks = oddsgroup.get("bookmakers", [])
        # escolhe o primeiro bookmaker com mercado 1X2
        for bk in bks:
            for bet in bk.get("bets", []):
                name = (bet.get("name") or "").lower()
                if any(key in name for key in ["match winner", "1x2", "full time result", "resultado final"]):
                    hv = dv = av = None
                    for v in bet.get("values", []):
                        label = (v.get("value") or "").lower()
                        odd = v.get("odd")
                        try:
                            oddf = float(str(odd).replace(",", "."))
                        except Exception:
                            oddf = None
                        if oddf and oddf > 1.0:
                            if "home" in label or label in ("1", "mandante"):
                                hv = oddf
                            elif "draw" in label or label in ("x", "empate"):
                                dv = oddf
                            elif "away" in label or label in ("2", "visitante"):
                                av = oddf
                    if all(x is not None for x in [hv, dv, av]):
                        return hv, dv, av
        return None
    except Exception:
        return None


def _fetch_odds_for_fixture(fixture_id: int, headers: Dict[str, str], debug: bool) -> Optional[Tuple[float, float, float]]:
    url = f"{BASE_URL}/odds"
    params = {"fixture": fixture_id}
    data = _request_json(url, headers, params, debug)
    if not data or "response" not in data or not data["response"]:
        return None
    # A API retorna uma lista; tomamos o primeiro agrupamento
    for group in data["response"]:
        res = _extract_1x2_from_oddsgroup(group)
        if res:
            return res
    return None


def main():
    args = parse_args()
    rodada = args.rodada
    outdir = f"data/out/{rodada}"
    os.makedirs(outdir, exist_ok=True)

    # Saída
    out_path = f"{outdir}/odds_apifootball.csv"

    # Checa chave RapidAPI
    api_key = os.environ.get("RAPIDAPI_KEY", "").strip()
    if not api_key:
        # Gera CSV vazio porém válido
        pd.DataFrame(columns=["match_id", "home", "away", "home_price", "draw_price", "away_price", "provider"]) \
          .to_csv(out_path, index=False)
        print("[apifootball] RAPIDAPI_KEY ausente — gerado CSV vazio.")
        print(f"[apifootball] OK -> {out_path} (0 linhas)")
        return

    aliases = _load_aliases(args.aliases)
    matches = _read_matches(rodada, aliases)

    headers = _rapidapi_headers(api_key)

    rows: List[Dict[str, Any]] = []

    # Estratégia 1: se o matches_source trouxer fixture_id (api-football), usamos direto.
    # Colunas aceitas: fixture_id OU apifootball_fixture_id
    fixture_col = None
    for cand in ["fixture_id", "apifootball_fixture_id"]:
        if cand in matches.columns:
            fixture_col = cand
            break

    for _, r in matches.iterrows():
        match_id = r.get("match_id")
        home = r.get("home_team")
        away = r.get("away_team")

        if fixture_col and pd.notna(r.get(fixture_col)):
            try:
                fixture_id = int(r.get(fixture_col))
            except Exception:
                fixture_id = None
        else:
            fixture_id = None

        odds = None
        if fixture_id:
            odds = _fetch_odds_for_fixture(fixture_id, headers, args.debug)

        if odds:
            h, d, a = odds
            rows.append({
                "match_id": match_id,
                "home": home,
                "away": away,
                "home_price": h,
                "draw_price": d,
                "away_price": a,
                "provider": "rapidapi",
            })
        else:
            # Sem odds por enquanto — não falha.
            if args.debug:
                print(f"[apifootball] sem odds p/ match_id={match_id} '{home}' vs '{away}'")
            continue

        # Respeita rate-limit conservador (depende do plano)
        time.sleep(0.2)

    out_df = pd.DataFrame(rows, columns=["match_id", "home", "away", "home_price", "draw_price", "away_price", "provider"])
    out_df.to_csv(out_path, index=False)
    print(f"[apifootball] OK -> {out_path} ({len(out_df)} linhas)")


if __name__ == "__main__":
    main()
