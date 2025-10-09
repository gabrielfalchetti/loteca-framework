#!/usr/bin/env python3
# scripts/ingest_odds_theoddsapi.py
# Coleta odds H2H da TheOddsAPI e casa com a whitelist de jogos,
# gerando odds_theoddsapi.csv com schema obrigatório.

from __future__ import annotations
import argparse
import csv
import json
import os
import sys
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
from rapidfuzz import process, fuzz
from unidecode import unidecode

SOURCE_NAME = "theoddsapi"
REQUIRED_COLUMNS = ["match_id", "home", "away", "odds_home", "odds_draw", "odds_away", "bookmaker", "market", "source"]

def log(msg: str):
    print(msg, flush=True)

def err(msg: str):
    print(f"[{SOURCE_NAME}] ERRO {msg}", flush=True)

def dbg(enabled: bool, msg: str):
    if enabled:
        print(f"[{SOURCE_NAME}][DEBUG] {msg}", flush=True)

def read_json(path: Optional[str], default: Any) -> Any:
    """
    Lê JSON com tolerância: se path não existir, for vazio, ou inválido -> retorna default.
    """
    if not path:
        return default
    try:
        if not os.path.exists(path):
            return default
        if os.path.getsize(path) == 0:
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def normalize_team(s: str) -> str:
    return unidecode(str(s or "")).lower().strip()

def load_whitelist(rodada_dir: str, debug: bool=False) -> pd.DataFrame:
    """
    Tenta localizar whitelist/matches_source em possíveis locais.
    Ordem de preferência:
      1) {rodada}/matches_whitelist.csv
      2) {rodada}/matches_source.csv
      3) data/in/matches_whitelist.csv
      4) data/in/matches_source.csv
    """
    candidates = [
        os.path.join(rodada_dir, "matches_whitelist.csv"),
        os.path.join(rodada_dir, "matches_source.csv"),
        "data/in/matches_whitelist.csv",
        "data/in/matches_source.csv",
    ]
    for p in candidates:
        if os.path.exists(p) and os.path.getsize(p) > 0:
            df = pd.read_csv(p)
            dbg(debug, f"whitelist encontrada: {p} (linhas={len(df)})")
            break
    else:
        raise FileNotFoundError(f"Whitelist inexistente nas localizações padrão: {candidates}")

    # Validar colunas mínimas
    needed = {"match_id", "home", "away"}
    missing = needed - set(map(str.lower, df.columns))
    # Tentar normalizar cabeçalho (case-insensitive)
    cols = {c.lower(): c for c in df.columns}
    if missing:
        # checar se a planilha tem as colunas com case diferente
        if needed - set(cols.keys()):
            raise ValueError(f"Whitelist precisa conter colunas {sorted(list(needed))}, encontrado={list(df.columns)}")
    # Renomear para o padrão
    ren = {}
    for k in ["match_id", "home", "away"]:
        if k in cols:
            ren[cols[k]] = k
    if ren:
        df = df.rename(columns=ren)

    # Garantir tipos/cortes
    df["match_id"] = df["match_id"].astype(str)
    df["home_norm"] = df["home"].map(normalize_team)
    df["away_norm"] = df["away"].map(normalize_team)
    return df

def theodds_fetch(api_key: str, regions: str, debug: bool=False) -> List[Dict[str, Any]]:
    """
    Busca odds H2H em todos os esportes 'upcoming'. Mantemos a query simples (sem commenceTimeTo)
    para evitar 422 por formato de data.
    """
    base = "https://api.the-odds-api.com/v4/sports/upcoming/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    dbg(debug, f"GET {base} params={params}")
    r = requests.get(base, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    try:
        data = r.json()
    except Exception as e:
        raise RuntimeError(f"Falha ao decodificar JSON: {e}")
    if not isinstance(data, list):
        raise RuntimeError("Resposta inesperada da TheOddsAPI (não é lista).")
    dbg(debug, f"Registros retornados: {len(data)}")
    return data

def best_match_team(candidate: str, choices: List[str]) -> Tuple[str, float]:
    """
    Encontra o melhor match por fuzzy matching (token_sort_ratio) entre 'candidate' e 'choices'.
    Retorna (melhor_escolha, score).
    """
    if not choices:
        return ("", 0.0)
    res = process.extractOne(
        normalize_team(candidate),
        [normalize_team(x) for x in choices],
        scorer=fuzz.token_sort_ratio
    )
    if not res:
        return ("", 0.0)
    best_norm, score, idx = res
    return choices[idx], float(score)

def map_odds_to_whitelist(whitelist: pd.DataFrame, odds_raw: List[Dict[str, Any]], debug: bool=False) -> pd.DataFrame:
    """
    Casa os eventos da TheOddsAPI com a whitelist pelo par (home, away) via fuzzy.
    Se não achar mapeamento com score >= 80 para ambos, ignora o evento.
    """
    wl_rows: List[Dict[str, Any]] = []
    wl_home_choices = whitelist["home"].tolist()
    wl_away_choices = whitelist["away"].tolist()

    total = 0
    mapeados = 0

    for ev in odds_raw:
        total += 1
        try:
            home = ev.get("home_team") or ""
            away = ev.get("away_team") or ""
            if not home or not away:
                continue

            # fuzzy por time da whitelist
            best_home, s1 = best_match_team(home, wl_home_choices)
            best_away, s2 = best_match_team(away, wl_away_choices)

            if s1 < 80 or s2 < 80:
                dbg(debug, f"descartado por score baixo: '{home}'->{best_home}({s1}), '{away}'->{best_away}({s2})")
                continue

            # localizar match_id correspondente (home==best_home & away==best_away)
            match_row = whitelist[(whitelist["home"] == best_home) & (whitelist["away"] == best_away)]
            if match_row.empty:
                # tentar na ordem invertida (algumas fontes podem inverter home/away)
                match_row = whitelist[(whitelist["home"] == best_away) & (whitelist["away"] == best_home)]
                if match_row.empty:
                    dbg(debug, f"sem linha correspondente na whitelist: {best_home} vs {best_away}")
                    continue

            match_id = str(match_row.iloc[0]["match_id"])

            # bookmakers -> markets (h2h)
            odds_home = odds_draw = odds_away = None
            bookmaker_name = ""
            market_name = "h2h"

            for bm in ev.get("bookmakers", []):
                markets = bm.get("markets", [])
                for mk in markets:
                    if mk.get("key") != "h2h":
                        continue
                    outcomes = mk.get("outcomes", [])
                    # outcomes: [{name: Team A, price: 1.80}, {name: Team B, price: 2.0}, {name: Draw, price:3.2}]
                    # coletar melhor preço por outcome
                    local_home = local_draw = local_away = None
                    for oc in outcomes:
                        name = (oc.get("name") or "").lower().strip()
                        price = oc.get("price")
                        if price is None:
                            continue
                        if "draw" in name or "empate" in name:
                            local_draw = max(local_draw, price) if local_draw else price
                        elif normalize_team(name) == normalize_team(home) or normalize_team(name) == normalize_team(best_home):
                            local_home = max(local_home, price) if local_home else price
                        else:
                            # assume away
                            local_away = max(local_away, price) if local_away else price
                    # atualizar com o melhor encontrado entre casas
                    if local_home and (not odds_home or local_home > odds_home):
                        odds_home = local_home
                        bookmaker_name = bm.get("title") or bm.get("key") or bookmaker_name
                    if local_draw and (not odds_draw or local_draw > odds_draw):
                        odds_draw = local_draw
                        bookmaker_name = bm.get("title") or bm.get("key") or bookmaker_name
                    if local_away and (not odds_away or local_away > odds_away):
                        odds_away = local_away
                        bookmaker_name = bm.get("title") or bm.get("key") or bookmaker_name

            if not (odds_home and odds_away):
                # precisamos ao menos odds_home e odds_away; draw pode faltar em alguns mercados
                dbg(debug, f"evento sem odds suficientes: {best_home} vs {best_away}")
                continue

            wl_rows.append({
                "match_id": match_id,
                "home": best_home,
                "away": best_away,
                "odds_home": float(odds_home),
                "odds_draw": float(odds_draw) if odds_draw else "",
                "odds_away": float(odds_away),
                "bookmaker": bookmaker_name,
                "market": market_name,
                "source": SOURCE_NAME
            })
            mapeados += 1

        except Exception as e:
            dbg(debug, f"erro ao processar evento: {e}")

    dbg(debug, f"mapeados={mapeados}/{total}")
    if not wl_rows:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return pd.DataFrame(wl_rows, columns=REQUIRED_COLUMNS)

def save_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)

def main():
    ap = argparse.ArgumentParser(description="Ingeste odds H2H da TheOddsAPI e casa com whitelist.")
    ap.add_argument("--rodada", required=True, help="Diretório de trabalho/rodada para saída (e possíveis insumos).")
    ap.add_argument("--regions", default=os.environ.get("REGIONS", "uk,eu,us,au"), help="Regiões da TheOddsAPI (ex: uk,eu,us,au).")
    ap.add_argument("--aliases", default="data/in/team_aliases.json", help="Arquivo de aliases de times (opcional).")
    ap.add_argument("--league_map", default="data/in/league_map.json", help="Mapeamento de ligas (opcional).")
    ap.add_argument("--debug", action="store_true", help="Logs detalhados.")
    args = ap.parse_args()

    out_dir = args.rodada
    api_key = os.environ.get("THEODDS_API_KEY", "").strip()

    if not api_key:
        err("THEODDS_API_KEY não definido no ambiente.")
        sys.exit(4)

    try:
        wl = load_whitelist(out_dir, debug=args.debug)
    except Exception as e:
        err(f"Whitelist não encontrada/válida: {e}")
        sys.exit(4)

    aliases = read_json(args.aliases, {"teams": {}})
    league_map = read_json(args.league_map, {})
    if args.debug:
        dbg(True, f"aliases carregado? {'sim' if aliases and isinstance(aliases, dict) else 'não (usando default)'}")
        dbg(True, f"league_map carregado? {'sim' if league_map and isinstance(league_map, dict) else 'não (usando default)'}")

    try:
        raw = theodds_fetch(api_key, args.regions, debug=args.debug)
    except Exception as e:
        err(f"Falha na chamada TheOddsAPI: {e}")
        sys.exit(4)

    df = map_odds_to_whitelist(wl, raw, debug=args.debug)

    out_csv = os.path.join(out_dir, "odds_theoddsapi.csv")
    if df.empty:
        err("Nenhum evento mapeado à whitelist (odds vazias).")
        # ainda salva CSV com cabeçalho para facilitar debug downstream
        save_csv(pd.DataFrame(columns=REQUIRED_COLUMNS), out_csv)
        sys.exit(4)

    # Garantir colunas e ordem
    for c in REQUIRED_COLUMNS:
        if c not in df.columns:
            df[c] = "" if c not in ("odds_home", "odds_draw", "odds_away") else None
    df = df[REQUIRED_COLUMNS]

    save_csv(df, out_csv)
    log(f"[{SOURCE_NAME}] OK -> {out_csv} ({len(df)} linhas)")

if __name__ == "__main__":
    main()