#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ingestor de odds reais do TheOddsAPI (mercado H2H) com:
- validação automática de sport_keys (consulta /v4/sports);
- matching robusto (fuzzy) entre eventos retornados e matches_source.csv;
- saída padronizada: data/out/<rodada>/odds_theoddsapi.csv com colunas:
  match_id, k1, kx, k2, provider, fetched_at

Requisitos:
- env THEODDS_API_KEY
- arquivo data/in/<rodada>/matches_source.csv com colunas: match_id, home, away, date (YYYY-MM-DD), sport_key (opcional)
- se sport_key vier vazio, o script tenta inferir pela coluna "league" usando data/theoddsapi_league_map.json (opcional)

Observações:
- Odds em formato DECIMAL.
- Se a data do jogo estiver muito distante (±30 dias), o TheOddsAPI pode não retornar o evento ainda.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from rapidfuzz import fuzz, process
from unidecode import unidecode

API = "https://api.the-odds-api.com/v4"
PROVIDER = "theoddsapi"
REQ_TIMEOUT = 25
FUZZY_MIN = 90  # limiar p/ matching de nomes

def norm(s: str) -> str:
    return unidecode(str(s or "")).strip().lower()

def load_league_map(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def infer_sport_key(league: str, league_map: dict) -> str | None:
    if not league:
        return None
    target = norm(league)
    for _, info in league_map.items():
        skey = info.get("sport_key")
        aliases = set(info.get("aliases", []))
        # inclua também a chave "liga" como alias
        if skey:
            for a in list(aliases):
                pass
        # match em aliases normalizados
        for cand in set([league] + list(aliases)):
            if norm(cand) == target:
                return skey
    return None

def fetch_sports_list(api_key: str) -> set[str]:
    """Retorna o conjunto de sport_keys suportados (para evitar UNKNOWN_SPORT)."""
    url = f"{API}/sports?apiKey={api_key}"
    r = requests.get(url, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    valid = set()
    for it in data:
        key = it.get("key")
        if key:
            valid.add(key)
    return valid

def fetch_odds_for_sport(api_key: str, sport_key: str, regions: str) -> list[dict]:
    """
    Busca todos os eventos de um sport_key com mercado H2H (home/draw/away).
    Retorna a lista crua de eventos (cada um com bookmakers/markets/outcomes).
    """
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal"
    }
    url = f"{API}/sports/{sport_key}/odds"
    r = requests.get(url, params=params, timeout=REQ_TIMEOUT)
    if r.status_code == 404 and "UNKNOWN_SPORT" in r.text:
        # sport_key inválido
        return []
    r.raise_for_status()
    return r.json()

def best_prices_from_event(ev: dict) -> tuple[float|None, float|None, float|None]:
    """Extrai melhor odd decimal (max) para home/draw/away no mercado H2H."""
    books = ev.get("bookmakers", []) or []
    best_home = best_draw = best_away = None
    for bk in books:
        for mk in bk.get("markets", []) or []:
            if mk.get("key") != "h2h":
                continue
            for out in mk.get("outcomes", []) or []:
                name = norm(out.get("name"))
                price = out.get("price")
                if not isinstance(price, (int, float)):
                    continue
                if name in ("home",):
                    best_home = max(best_home, price) if best_home else price
                elif name in ("draw", "empate"):
                    best_draw = max(best_draw, price) if best_draw else price
                elif name in ("away",):
                    best_away = max(best_away, price) if best_away else price
    return best_home, best_draw, best_away

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ex.: 2025-09-27_1213")
    ap.add_argument("--regions", default="uk,eu,us,au", help="regiões TheOddsAPI")
    ap.add_argument("--leaguemap", default="data/theoddsapi_league_map.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    api_key = os.environ.get("THEODDS_API_KEY", "").strip()
    if not api_key:
        print("[theoddsapi] ERRO: THEODDS_API_KEY não definido.", file=sys.stderr)
        sys.exit(1)

    rodada = args.rodada
    in_dir = Path(f"data/in/{rodada}")
    out_dir = Path(f"data/out/{rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    ms_path = in_dir / "matches_source.csv"
    out_path = out_dir / "odds_theoddsapi.csv"

    if not ms_path.exists():
        print(f"[theoddsapi] ERRO: arquivo ausente {ms_path}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(ms_path)
    # valida colunas
    need = ["match_id", "home", "away"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        print(f"[theoddsapi] ERRO: matches_source faltando colunas {miss}", file=sys.stderr)
        sys.exit(2)

    # normalização auxiliar
    df["home_n"] = df["home"].map(norm)
    df["away_n"] = df["away"].map(norm)
    if "date" in df.columns:
        # padronizar para date sem hora
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # carrega mapa de ligas (plano B)
    league_map = load_league_map(Path(args.leaguemap))

    # resolve sport_key por linha
    if "sport_key" not in df.columns:
        df["sport_key"] = None
    df["sport_key"] = df.apply(
        lambda r: r["sport_key"] if pd.notna(r.get("sport_key")) and str(r["sport_key"]).strip()
        else infer_sport_key(str(r.get("league", "")), league_map),
        axis=1
    )

    # consulta lista de esportes válidos
    try:
        valid_sports = fetch_sports_list(api_key)
    except Exception as e:
        print(f"[theoddsapi] ERRO ao consultar /sports: {e}", file=sys.stderr)
        sys.exit(3)

    # agrupa por sport_key válido
    df["sport_key"] = df["sport_key"].astype(str).str.strip()
    grouped = df.groupby("sport_key", dropna=False)

    rows_out = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for skey, g in grouped:
        if not skey or skey not in valid_sports:
            # reporta e segue
            for _, r in g.iterrows():
                print(f"[theoddsapi] AVISO {r['match_id']}: sport_key inválido/indisponível -> '{skey}'")
            continue

        # busca eventos do sport_key
        try:
            events = fetch_odds_for_sport(api_key, skey, args.regions)
        except Exception as e:
            print(f"[theoddsapi] AVISO {skey}: falha ao buscar odds: {e}", file=sys.stderr)
            continue

        if not events:
            # nada retornado para esse sport_key (janela/mercado indisponível)
            for _, r in g.iterrows():
                print(f"[theoddsapi] AVISO {r['match_id']}: nenhum evento retornado para sport_key={skey}")
            continue

        # prepara índice de busca por (data, nome home/away)
        # criamos lista de “candidatos” com tupla normalizada para fuzzy
        cand = []
        for ev in events:
            # times
            home_team = norm(ev.get("home_team"))
            away_team = norm(ev.get("away_team"))
            # data
            ct = ev.get("commence_time")
            try:
                ev_date = datetime.fromisoformat(ct.replace("Z", "+00:00")).date() if ct else None
            except Exception:
                ev_date = None

            # chave de matching
            key = f"{home_team}__{away_team}"
            cand.append((key, ev_date, ev))

        # para cada match, encontrar melhor candidato (por nomes e, se existir, por data)
        for _, r in g.iterrows():
            mid = r["match_id"]
            h = r["home_n"]
            a = r["away_n"]
            want_date = r.get("date")

            target_key = f"{h}__{a}"

            # filtra por data (se fornecida) ± 3 dias (para mitigar fusos)
            pool = []
            for key, ev_date, ev in cand:
                if want_date and ev_date:
                    if abs((ev_date - want_date).days) > 3:
                        continue
                pool.append((key, ev))

            if not pool:
                pool = [(k, ev) for (k, _, ev) in cand]  # sem filtro de data

            # fuzzy match no key
            choices = [k for k, _ in pool]
            if not choices:
                print(f"[theoddsapi] AVISO {mid}: nenhum candidato p/ matching")
                continue

            best = process.extractOne(target_key, choices, scorer=fuzz.token_set_ratio)
            if not best or best[1] < FUZZY_MIN:
                print(f"[theoddsapi] AVISO {mid}: matching fraco ({best[1] if best else 'NA'}) - '{r['home']}' x '{r['away']}'")
                continue

            # pega o evento correspondente
            idx = choices.index(best[0])
            ev = pool[idx][1]

            k1, kx, k2 = best_prices_from_event(ev)
            if not (k1 and kx and k2):
                print(f"[theoddsapi] AVISO {mid}: odds H2H incompletas (k1={k1}, kx={kx}, k2={k2})")
                continue

            rows_out.append({
                "match_id": mid,
                "k1": float(k1),
                "kx": float(kx),
                "k2": float(k2),
                "provider": PROVIDER,
                "fetched_at": now_iso
            })

    if not rows_out:
        # ainda assim escrevemos um CSV vazio para o consenso enxergar que tentamos
        pd.DataFrame(columns=["match_id","k1","kx","k2","provider","fetched_at"]).to_csv(out_path, index=False)
        print(f"[theoddsapi] AVISO: nenhum par de odds casou com os jogos (arquivo vazio salvo em {out_path})")
        sys.exit(0)

    out_df = pd.DataFrame(rows_out).drop_duplicates(subset=["match_id"])
    out_df.to_csv(out_path, index=False)
    print(f"[theoddsapi] OK -> {out_path} ({len(out_df)} linhas)")

if __name__ == "__main__":
    main()
