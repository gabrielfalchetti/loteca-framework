#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import math
import argparse
import unicodedata
from typing import Dict, List, Tuple
import requests
import pandas as pd
from rapidfuzz import fuzz, process

THEODDS_ENDPOINTS = [
    # Brasileirão A
    "https://api.the-odds-api.com/v4/sports/soccer_brazil_campeonato/odds",
    # Série B
    "https://api.the-odds-api.com/v4/sports/soccer_brazil_serie_b/odds",
]

def norm(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    # normalizações comuns BR
    s = s.replace("atletico", "atletico").replace("athletico", "atletico")
    s = s.replace("botafogo sp", "botafogo-sp").replace("botafogo rj", "botafogo")
    s = s.replace("avai", "avai").replace("goias", "goias")
    # múltiplos espaços/traços
    while "  " in s: s = s.replace("  ", " ")
    s = s.replace(" - ", " ").replace("--", "-")
    return s

def make_key(home: str, away: str) -> str:
    return f"{norm(home)}__vs__{norm(away)}"

def load_aliases(path: str) -> Dict[str, List[str]]:
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # normaliza tudo
    ali = {}
    for k, arr in data.items():
        base = norm(k)
        ali[base] = list({norm(x) for x in ([k] + arr)})
    return ali

def best_price(markets: List[Dict]) -> Tuple[float, float, float]:
    """
    Recebe a lista de bookmakers->markets do TheOdds e retorna (home, draw, away)
    usando o melhor (maior) preço H2H encontrado entre todos os bookmakers.
    """
    best_h = None
    best_d = None
    best_a = None
    for b in markets:
        # cada bookmaker tem "markets": [{"key": "h2h", "outcomes": [{"name": "Draw"|"Team A"|"Team B","price":1.9}, ...]}]
        for m in b.get("markets", []):
            if m.get("key") != "h2h":
                continue
            for outcome in m.get("outcomes", []):
                name = outcome.get("name", "")
                price = outcome.get("price")
                if price is None:
                    continue
                # Normaliza outcome: "Draw" -> draw; senão é time
                if name.lower() == "draw":
                    if best_d is None or price > best_d:
                        best_d = price
                else:
                    # Não sabemos a orientação (home/away) aqui; quem chama decide
                    pass
    # Não conseguimos separar home/away só olhando outcome name acima,
    # vamos extrair de uma chamada mais rica que sabe quem é home/away.
    # O TheOdds traz outcomes no mesmo order do evento (home/away) por bookmaker.
    # Então tratamos isso no loop principal (ver abaixo).
    return best_h, best_d, best_a  # placeholder (não usado diretamente)

def fetch_events(api_key: str, regions: str, debug: bool=False) -> List[Dict]:
    all_events = []
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": "h2h",
    }
    for url in THEODDS_ENDPOINTS:
        if debug:
            print(f"[theoddsapi-safe][DEBUG] GET {url} {params}")
        r = requests.get(url, params=params, timeout=25)
        r.raise_for_status()
        part = r.json()
        if isinstance(part, list):
            all_events.extend(part)
    if debug:
        print(f"[theoddsapi-safe][DEBUG] eventos coletados: {len(all_events)}")
    return all_events

def event_to_row(ev: Dict) -> Dict:
    """
    Transforma um evento do TheOdds em um dicionário com:
    team_home, team_away, match_key, odds_home, odds_draw, odds_away
    tomando o melhor preço H2H entre os bookmakers.
    """
    home = ev.get("home_team") or ""
    away = ev.get("away_team") or ""
    key = make_key(home, away)

    best_home = None
    best_draw = None
    best_away = None

    for bm in ev.get("bookmakers", []):
        for m in bm.get("markets", []):
            if m.get("key") != "h2h":
                continue
            # outcomes costuma vir em 2 ou 3 entradas: [home, away] ou [home, draw, away]
            # Precisamos mapear pelo *nome* que bate com home/away do evento.
            local_home = None
            local_draw = None
            local_away = None
            for out in m.get("outcomes", []):
                nm = out.get("name", "")
                price = out.get("price")
                if price is None:
                    continue
                if nm.lower() == "draw":
                    local_draw = price
                elif norm(nm) == norm(home):
                    local_home = price
                elif norm(nm) == norm(away):
                    local_away = price
                else:
                    # às vezes “outcome.name” vem com variação pequena (ex: “Atletico MG” vs “Athletic Club (MG)”)
                    # tentamos um match “contains” bem tolerante:
                    if norm(home) in norm(nm):
                        local_home = price
                    elif norm(away) in norm(nm):
                        local_away = price
            # atualiza melhores preços globais
            if local_home and (best_home is None or local_home > best_home):
                best_home = local_home
            if local_draw and (best_draw is None or local_draw > best_draw):
                best_draw = local_draw
            if local_away and (best_away is None or local_away > best_away):
                best_away = local_away

    return {
        "team_home": home,
        "team_away": away,
        "match_key": key,
        "odds_home": best_home,
        "odds_draw": best_draw,
        "odds_away": best_away,
    }

def read_matches(rodada: str) -> pd.DataFrame:
    path = os.path.join("data", "in", rodada, "matches_source.csv")
    df = pd.read_csv(path)
    # garante colunas
    if "team_home" not in df.columns or "team_away" not in df.columns:
        raise RuntimeError("matches_source.csv precisa ter colunas team_home, team_away")
    if "match_key" not in df.columns:
        df["match_key"] = df.apply(lambda r: make_key(r["team_home"], r["team_away"]), axis=1)
    # normalizados auxiliares
    df["__home_norm"] = df["team_home"].astype(str).map(norm)
    df["__away_norm"] = df["team_away"].astype(str).map(norm)
    df["__key_norm"]  = df["match_key"].astype(str).map(str.lower)
    return df

def match_events_to_source(events_df: pd.DataFrame, src_df: pd.DataFrame, aliases: Dict[str, List[str]], debug=False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Faz o join por:
    1) match_key exato
    2) se falhar, fuzzy: casa e fora precisam dar bom (>= 90) quando comparados contra aliases.
    """
    ev = events_df.copy()
    ev["__home_norm"] = ev["team_home"].astype(str).map(norm)
    ev["__away_norm"] = ev["team_away"].astype(str).map(norm)
    ev["__key_norm"]  = ev["match_key"].astype(str).map(str.lower)

    # 1) match_key direto
    merged = pd.merge(src_df, ev, on="match_key", how="left", suffixes=("", "_odds"))
    direct_ok = ~merged["odds_draw"].isna() | ~merged["odds_home"].isna() | ~merged["odds_away"].isna()

    # 2) para as que ficaram sem odds, fazer fuzzy por nome dos times
    need = merged[~direct_ok].copy()
    if not need.empty:
        # prepara dicionários de aliases para casa e fora
        def all_aliases(s: str) -> List[str]:
            base = norm(s)
            arr = [base]
            if base in aliases:
                arr.extend(aliases[base])
            return list(set(arr))

        def fuzzy_pick(target: str, choices: List[str]) -> Tuple[str, int]:
            if not choices:
                return ("", 0)
            res = process.extractOne(target, choices, scorer=fuzz.token_set_ratio)
            if res is None:
                return ("", 0)
            return (res[0], int(res[1]))

        rows = []
        for _, r in need.iterrows():
            home_choices = all_aliases(r["team_home"])
            away_choices = all_aliases(r["team_away"])

            # procurar no dataframe de eventos (lado odds)
            best_row = None
            best_score = -1

            for _, e in ev.iterrows():
                h_score = max(
                    (fuzz.token_set_ratio(norm(e["team_home"]), c) for c in home_choices),
                    default=0
                )
                a_score = max(
                    (fuzz.token_set_ratio(norm(e["team_away"]), c) for c in away_choices),
                    default=0
                )
                score = min(h_score, a_score)  # exigente: os dois lados têm que bater
                if score > best_score:
                    best_score = score
                    best_row = e

            if best_row is not None and best_score >= 90:
                new_r = r.copy()
                for col in ["odds_home","odds_draw","odds_away","team_home_odds","team_away_odds","match_key_odds"]:
                    new_r[col] = best_row.get(col.replace("_odds",""), None)
                rows.append(new_r)

        if rows:
            patched = pd.DataFrame(rows, columns=merged.columns)
            # atualiza as linhas faltantes
            merged.loc[patched.index, :] = patched

    ok_mask = (~merged["odds_home"].isna()) | (~merged["odds_draw"].isna()) | (~merged["odds_away"].isna())
    matched = merged[ok_mask].copy()

    # “válido” para o nosso pipeline = ter pelo menos duas odds > 1.0
    def valid_row(r):
        vals = [r.get("odds_home"), r.get("odds_draw"), r.get("odds_away")]
        cnt = sum(1 for x in vals if isinstance(x,(int,float)) and x and x>1.0 and not math.isnan(x))
        return cnt >= 2

    matched["__valid"] = matched.apply(valid_row, axis=1)
    valid = matched[matched["__valid"]].copy()

    unmatched = src_df[~src_df["match_key"].isin(valid["match_key"])].copy()
    return valid[["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]], unmatched

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    api_key = os.environ.get("THEODDS_API_KEY", "")
    if not api_key:
        print("[theoddsapi-safe] SKIP: THEODDS_API_KEY ausente.")
        sys.exit(0)

    out_dir = os.path.join("data","out",rodada)
    os.makedirs(out_dir, exist_ok=True)

    # 1) ler matches de entrada
    src_df = read_matches(rodada)
    # 2) chamar API
    events = fetch_events(api_key, args.regions, debug=args.debug)
    # 3) mapear para linhas com odds
    rows = [event_to_row(e) for e in events]
    events_df = pd.DataFrame(rows)
    # 4) join + fuzzy
    aliases = load_aliases(args.aliases)
    valid_df, unmatched_df = match_events_to_source(events_df, src_df, aliases, debug=args.debug)

    # 5) salvar
    odds_path = os.path.join(out_dir,"odds_theoddsapi.csv")
    unmatched_path = os.path.join(out_dir,"unmatched_theoddsapi.csv")

    valid_df.to_csv(odds_path, index=False)
    unmatched_df.to_csv(unmatched_path, index=False)

    print(f'[theoddsapi-safe] linhas -> {json.dumps({os.path.basename(odds_path): len(valid_df), os.path.basename(unmatched_path): len(unmatched_df)})}')

if __name__ == "__main__":
    main()