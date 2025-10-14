#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import re
import sys
from typing import Dict, List, Tuple, Optional

import requests
import pandas as pd
from unidecode import unidecode

API_BASE = "https://api.the-odds-api.com/v4"


# ========================= Helpers de string =========================

def _slug(s: str) -> str:
    if s is None:
        return ""
    s = unidecode(str(s)).lower()
    s = (
        s.replace("(", " ").replace(")", " ")
         .replace("/", " ").replace("-", " ")
         .replace("_", " ").replace(".", " ")
         .replace(",", " ").replace("'", " ")
         .replace("’", " ").replace("&", " and ")
    )
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _aliased_names(team: str) -> List[str]:
    """
    Retorna uma lista de possíveis nomes (aliases) para casar outcomes/bookmakers.
    Focado em clubes BR comuns no seu fluxo. Expanda à vontade.
    """
    base = unidecode(team).strip()
    s = _slug(team)

    # mapeamentos por slug
    BR_ALIASES: Dict[str, List[str]] = {
        # Paranaense
        "athletico pr": ["athletico pr", "athletico paranaense", "atletico pr", "athletico-pr", "athletico paranaense pr"],
        # Goianiense
        "atletico go": ["atletico go", "atletico goianiense", "atletico-go", "atletico goianiense go"],
        # Botafogo SP
        "botafogo sp": ["botafogo sp", "botafogo-sp", "botafogo ribeirao", "botafogo ribeirao preto", "botafogo rp"],
        # Ferroviária
        "ferroviaria": ["ferroviaria", "ferroviaria sp", "a e ferroviaria"],
        # Paysandu
        "paysandu": ["paysandu", "paysandu pa"],
        # Remo
        "remo": ["remo", "remo pa", "clube do remo"],
        # CRB
        "crb": ["crb", "crb al", "clube de regatas brasil"],
        # Chapecoense
        "chapecoense": ["chapecoense", "chapecoense sc", "a chapecoense"],
        # Avai
        "avai": ["avai", "avai sc"],
        # Volta Redonda
        "volta redonda": ["volta redonda", "volta redonda rj", "volta redonda fc"],
        # Atlético-GO grafia com acento
        "atlético go": ["atletico go", "atlético go", "atletico goianiense", "atlético goianiense"],
        # Athletico-PR com hífen
        "athletico pr": ["athletico pr", "athletico-pr", "athletico paranaense", "atletico pr"],
        # Botafogo-SP com hífen
        "botafogo sp": ["botafogo sp", "botafogo-sp", "botafogo ribeirao preto"],
        # Ferroviária com acento
        "ferroviária": ["ferroviaria", "ferroviária", "ferroviaria sp"],
        # Avaí com acento
        "avai": ["avai", "avaí", "avai sc"],
    }

    # também tratar "paisandu/remo (pa)" que pode aparecer no csv original por engano
    if "paysandu" in s and "remo" in s:
        BR_ALIASES.setdefault("paysandu", ["paysandu", "paysandu pa"])
        BR_ALIASES.setdefault("remo", ["remo", "remo pa"])

    # fallback genérico: variações óbvias
    generic = {base, team, team.replace("-", " "), team.replace("/", " "), team.replace("(", " ").replace(")", " ")}
    generic.update({unidecode(x) for x in list(generic)})
    generic_slugs = { _slug(x) for x in generic }

    aliases = set()
    aliases.update(generic)
    for key, vals in BR_ALIASES.items():
        if _slug(team) == key or _slug(base) == key or (_slug(team) in key) or (key in _slug(team)):
            aliases.update(vals)

    # garantir versões sem acento e com/sem UF
    more = set()
    for a in list(aliases):
        a0 = unidecode(a)
        more.add(a0)
        more.add(a0.replace("-", " "))
        more.add(a0.replace("/", " "))
    aliases.update(more)

    # remover vazios, normalizar espaços
    aliases = { re.sub(r"\s+", " ", x).strip() for x in aliases if x and str(x).strip() }

    return sorted(aliases)


def _draw_aliases() -> List[str]:
    return ["Draw", "Empate", "Tie"]


# ========================= Helpers CSV =========================

def pick_home_away_cols(df: pd.DataFrame) -> Tuple[str, str]:
    lc = {c.lower(): c for c in df.columns}
    home = lc.get("home_norm") or lc.get("home") or lc.get("team_home") or lc.get("mandante")
    away = lc.get("away_norm") or lc.get("away") or lc.get("team_away") or lc.get("visitante")
    if not home or not away:
        print("::error::CSV precisa conter colunas home/home_norm e away/away_norm", file=sys.stderr)
        sys.exit(5)
    return home, away


def pick_id_col(df: pd.DataFrame) -> str:
    lc = {c.lower(): c for c in df.columns}
    mid = lc.get("match_id") or lc.get("id") or lc.get("jogo") or lc.get("game_id")
    if not mid:
        print("::error::CSV precisa conter coluna match_id (ou id/jogo/game_id)", file=sys.stderr)
        sys.exit(5)
    return mid


# ========================= Clientes TheOddsAPI =========================

def _get(api_key: str, path: str, params: Dict[str, str], timeout: int = 25) -> Optional[requests.Response]:
    url = f"{API_BASE}{path}"
    try:
        r = requests.get(url, params={"apiKey": api_key, **params}, timeout=timeout)
        if r.status_code == 401:
            print("[theoddsapi][ERROR] 401 Unauthorized — verifique THEODDS_API_KEY/limites.", file=sys.stderr)
            return None
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        print(f"[theoddsapi][ERROR] Falha HTTP: {e}", file=sys.stderr)
        return None


def list_sports(api_key: str) -> List[dict]:
    r = _get(api_key, "/sports", params={"all": "true"})
    if not r:
        return []
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def fetch_odds_for_sport(api_key: str, sport_key: str, regions: str, markets: str = "h2h",
                         odds_format: str = "decimal", date_format: str = "iso") -> List[dict]:
    r = _get(api_key, f"/sports/{sport_key}/odds", params={
        "regions": regions, "markets": markets, "oddsFormat": odds_format, "dateFormat": date_format
    })
    if not r:
        return []
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


# ========================= Extração/Matching =========================

def extract_h2h_prices(event: dict) -> Tuple[Optional[str], Optional[str], List[Tuple[str, float]], int]:
    home = event.get("home_team")
    away = event.get("away_team")

    prices: Dict[str, List[float]] = {}
    books = event.get("bookmakers") or []
    for bk in books:
        markets = bk.get("markets") or []
        for mk in markets:
            if (mk.get("key") or "").lower() != "h2h":
                continue
            outcomes = mk.get("outcomes") or []
            for o in outcomes:
                name = o.get("name")
                price = o.get("price")
                if name is None or price is None:
                    continue
                key = _slug(name)
                prices.setdefault(key, []).append(float(price))

    averaged: List[Tuple[str, float]] = []
    for k, vals in prices.items():
        if vals:
            averaged.append((k, sum(vals) / len(vals)))

    books_count = len(books)
    return home, away, averaged, books_count


def names_match(a: str, b: str) -> bool:
    return _slug(a) == _slug(b)


def event_matches_fixture(ehome: str, eaway: str, our_home: str, our_away: str) -> Tuple[bool, bool]:
    eh, ea = _slug(ehome), _slug(eaway)
    oh, oa = _slug(our_home), _slug(our_away)
    if eh == oh and ea == oa:
        return True, False
    if eh == oa and ea == oh:
        return True, True
    # tenta por aliases (ex.: athletico pr vs athletico paranaense)
    for ah in _aliased_names(our_home):
        for aa in _aliased_names(our_away):
            if names_match(ehome, ah) and names_match(eaway, aa):
                return True, False
            if names_match(ehome, aa) and names_match(eaway, ah):
                return True, True
    return False, False


def pick_triplet(prices: List[Tuple[str, float]], home_aliases: List[str], away_aliases: List[str]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    price_map = {k: v for k, v in prices}
    # chaves candidatas
    home_keys = {_slug(x) for x in home_aliases}
    away_keys = {_slug(x) for x in away_aliases}
    draw_keys = {_slug(x) for x in _draw_aliases()}

    def find_first(keys: set) -> Optional[float]:
        for k in keys:
            if k in price_map:
                return price_map[k]
        return None

    o_home = find_first(home_keys)
    o_away = find_first(away_keys)
    o_draw = find_first(draw_keys)
    return o_home, o_draw, o_away


# ========================= Main =========================

def main():
    ap = argparse.ArgumentParser(description="Ingest odds from TheOddsAPI and match with provided fixtures.")
    ap.add_argument("--rodada", required=True, help="Diretório de saída desta rodada (ex: data/out/<run_id>)")
    ap.add_argument("--regions", required=True, help="Regiões da TheOddsAPI (ex: uk,eu,us,au)")
    ap.add_argument("--source_csv", required=True, help="CSV com match_id/home/away (ou home_norm/away_norm)")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "odds_theoddsapi.csv")

    api_key = os.environ.get("THEODDS_API_KEY", "").strip()
    if not api_key:
        print("::error::THEODDS_API_KEY não configurada no ambiente.", file=sys.stderr)
        sys.exit(5)

    # Carrega fixtures
    try:
        df = pd.read_csv(args.source_csv)
    except Exception as e:
        print(f"::error::Falha ao ler {args.source_csv}: {e}", file=sys.stderr)
        sys.exit(5)

    mid_col = pick_id_col(df)
    home_col, away_col = pick_home_away_cols(df)
    sel = df[[mid_col, home_col, away_col]].copy()
    sel.columns = ["match_id", "home", "away"]
    sel["home"] = sel["home"].astype(str).str.strip()
    sel["away"] = sel["away"].astype(str).str.strip()

    # 1) Tenta 'upcoming' mas filtra só soccer
    events = fetch_odds_for_sport(api_key, "upcoming", args.regions, markets="h2h")
    soccer_events = [e for e in events if str(e.get("sport_key", "")).startswith("soccer_")]

    # 2) Se pouca coisa, tenta ligar específicas do Brasil
    if len(soccer_events) < len(sel):
        sports = list_sports(api_key)
        br_keys = [s.get("key") for s in sports if isinstance(s, dict) and str(s.get("key", "")).startswith("soccer_brazil_")]
        for key in br_keys:
            extra = fetch_odds_for_sport(api_key, key, args.regions, markets="h2h")
            soccer_events.extend(extra)

    # Dedup simples por id
    seen = set()
    filtered_events = []
    for e in soccer_events:
        eid = e.get("id") or (e.get("home_team"), e.get("away_team"), e.get("commence_time"))
        if eid in seen:
            continue
        seen.add(eid)
        filtered_events.append(e)

    # Pré-processa
    processed = []
    for ev in filtered_events:
        ehome, eaway, prices, books_count = extract_h2h_prices(ev)
        if not ehome or not eaway:
            continue
        processed.append((ehome, eaway, prices, books_count))

    rows_out: List[List] = []
    matched = 0

    for _, r in sel.iterrows():
        mid = r["match_id"]
        our_home = r["home"]
        our_away = r["away"]

        best = None

        for (ehome, eaway, prices, books_count) in processed:
            ok, reversed_flag = event_matches_fixture(ehome, eaway, our_home, our_away)
            if not ok:
                continue

            # aliases para outcomes
            if not reversed_flag:
                home_aliases = [ehome, our_home] + _aliased_names(our_home)
                away_aliases = [eaway, our_away] + _aliased_names(our_away)
            else:
                home_aliases = [eaway, our_home] + _aliased_names(our_home)
                away_aliases = [ehome, our_away] + _aliased_names(our_away)

            o_home, o_draw, o_away = pick_triplet(prices, home_aliases, away_aliases)

            # Requer pelo menos home e away
            if o_home is not None and o_away is not None:
                best = [mid, our_home, our_away, o_home, o_draw, o_away, books_count]
                break

        if best:
            rows_out.append(best)
            matched += 1

    # Escreve saída (mesmo vazia, com header, para o workflow seguir a checagem)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "books_count"])
        w.writerows(rows_out)

    print(f"[theoddsapi]Eventos={len(filtered_events)} | jogoselecionados={len(sel)} | pareados={matched} — salvo em {out_csv}")


if __name__ == "__main__":
    main()