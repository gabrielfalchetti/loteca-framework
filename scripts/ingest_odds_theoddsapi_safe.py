#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coleta odds 3-way (home/draw/away) do TheOddsAPI para os jogos listados em
data/in/<RODADA>/matches_source.csv e grava em data/out/<RODADA>/odds_theoddsapi.csv

Compatível com:
- THEODDS_API_KEY (env)
- --rodada RODADA (obrigatório)
- --regions "uk,eu,us,au" (opcional; default "uk,eu,us,au")
- --debug (opcional)

Regras:
- Faz match de outcomes usando nomes de time (com aliases opcionais em data/aliases_br.json)
- Aceita odds em decimal ou americano (converte p/ decimal)
- Só grava uma linha por jogo (consenso simples entre bookies: melhor preço por outcome)
"""

import os, sys, json, argparse, math, time
from typing import Dict, Any, List, Tuple
import pandas as pd
import requests
from unidecode import unidecode

THEODDS_BASE = "https://api.the-odds-api.com/v4"
DEFAULT_REGIONS = "uk,eu,us,au"

def log(msg: str):
    print(f"[theoddsapi-safe] {msg}")

def ddbg(debug: bool, msg: str):
    if debug:
        print(f"[theoddsapi-safe][DEBUG] {msg}")

def load_aliases(path: str, debug: bool) -> Dict[str, List[str]]:
    if not os.path.isfile(path):
        ddbg(debug, f"aliases não encontrado: {path} (seguindo sem)")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)  # precisa ser JSON bem-formado
        if not isinstance(data, dict):
            raise ValueError("aliases não é um objeto JSON")
        return data
    except Exception as e:
        print(f"[theoddsapi] AVISO: falha ao ler aliases {path}: {e}")
        return {}

def norm(s: str) -> str:
    if s is None:
        return ""
    return unidecode(str(s)).strip().lower()

def join_key(home: str, away: str) -> str:
    return f"{norm(home)}__vs__{norm(away)}"

def american_to_decimal(x: Any) -> float:
    if x is None:
        return float('nan')
    if isinstance(x, (int, float)):
        try:
            v = float(x)
        except Exception:
            return float('nan')
    else:
        s = str(x).strip()
        if not s or s in ("[]", "null", "None", "nan", "NaN"):
            return float('nan')
        try:
            v = float(s)
        except Exception:
            return float('nan')
    # heurística: se parece decimal já (entre 1.01 e 100), retorna
    if 1.01 <= v <= 100.0:
        return v
    # odds americanas
    if v > 0:
        return 1.0 + (v / 100.0)
    elif v < 0:
        return 1.0 + (100.0 / abs(v))
    return float('nan')

def pick_best_price(prices: List[float]) -> float:
    prices = [p for p in prices if isinstance(p, (int, float)) and p > 1.0 and math.isfinite(p)]
    return max(prices) if prices else float('nan')

def name_matches(team: str, outcome: str, aliases: Dict[str, List[str]]) -> bool:
    t = norm(team)
    o = norm(outcome)
    if o == "draw" or o == "empate":
        return False
    if t == o:
        return True
    # tenta via aliases
    for k, arr in aliases.items():
        nk = norm(k)
        if nk == t:
            for alt in arr or []:
                if norm(alt) == o:
                    return True
    return False

def collect_theodds_for_keys(api_key: str, sport_keys: List[str], regions: str, debug: bool) -> List[Dict[str, Any]]:
    headers = {"Accept": "application/json"}
    all_events: List[Dict[str, Any]] = []
    for sk in sport_keys:
        url = f"{THEODDS_BASE}/sports/{sk}/odds"
        params = {
            "apiKey": api_key,
            "regions": regions,
            "markets": "h2h",
            # oddsFormat sem setar: TheOdds costuma retornar decimal; se vier americano, convertemos
        }
        ddbg(debug, f"GET {url} {params}")
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code != 200:
            print(f"[theoddsapi] AVISO: {url} HTTP {r.status_code} body={r.text[:200]}")
            continue
        try:
            data = r.json()
        except Exception as e:
            print(f"[theoddsapi] AVISO: JSON inválido em {url}: {e}")
            continue
        if isinstance(data, list):
            all_events.extend(data)
        time.sleep(0.5)  # gentil com a API
    return all_events

def determine_sport_keys(matches_df: pd.DataFrame) -> List[str]:
    # chuta chaves relevantes para BR (A e B). Inclua mais se precisar.
    keys = {
        "soccer_brazil_campeonato",   # Série A
        "soccer_brazil_serie_b",      # Série B
    }
    # se quiser: derivar por time/ligas no futuro
    return sorted(keys)

def extract_prices(event: Dict[str, Any]) -> Tuple[str, str, Dict[str, float]]:
    # Retorna home_name, away_name e dict outcome->preço decimal
    home = event.get("home_team") or ""
    away = event.get("away_team") or ""
    out_prices: Dict[str, float] = {}

    bmk = event.get("bookmakers") or []
    for b in bmk:
        mkts = b.get("markets") or []
        for m in mkts:
            if m.get("key") != "h2h":
                continue
            outs = m.get("outcomes") or []
            for o in outs:
                nm = o.get("name", "")
                price = o.get("price")
                # se vier como americano em field "price", converter; se decimal, a função respeita
                dec = american_to_decimal(price)
                # guarda o melhor preço por outcome
                prior = out_prices.get(nm)
                if prior is None or (isinstance(dec, float) and dec > prior):
                    out_prices[nm] = dec
    return home, away, out_prices

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default=DEFAULT_REGIONS)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    debug = bool(args.debug or os.getenv("DEBUG", "").lower() == "true")
    api_key = os.getenv("THEODDS_API_KEY", "")
    if not api_key:
        print("[theoddsapi-safe] SKIP: THEODDS_API_KEY ausente.")
        sys.exit(0)

    in_matches = os.path.join("data", "in", args.rodada, "matches_source.csv")
    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "odds_theoddsapi.csv")
    unmatched_csv = os.path.join(out_dir, "unmatched_theoddsapi.csv")

    if not os.path.isfile(in_matches):
        print(f"[theoddsapi-safe] ERRO: arquivo não encontrado: {in_matches}")
        sys.exit(2)

    matches = pd.read_csv(in_matches)
    # espera colunas: team_home, team_away (case-insensitive)
    col_home = next((c for c in matches.columns if c.lower() == "team_home"), None)
    col_away = next((c for c in matches.columns if c.lower() == "team_away"), None)
    if not col_home or not col_away:
        print("[theoddsapi-safe] ERRO: matches_source.csv precisa das colunas team_home e team_away")
        sys.exit(2)
    matches["__join_key"] = matches.apply(lambda r: join_key(r[col_home], r[col_away]), axis=1)

    aliases = load_aliases(os.path.join("data", "aliases_br.json"), debug)

    sport_keys = determine_sport_keys(matches)
    events = collect_theodds_for_keys(api_key, sport_keys, args.regions, debug)
    ddbg(debug, f"eventos coletados: {len(events)}")

    rows, unmatched = [], []
    for ev in events:
        home_raw, away_raw, prices = extract_prices(ev)
        if not home_raw or not away_raw:
            continue

        jk = join_key(home_raw, away_raw)
        # tenta casar com a lista de jogos de entrada (join exato por chave normalizada)
        # se não achar, ainda podemos salvar como "unmatched" pra depuração
        if jk not in set(matches["__join_key"]):
            # tenta outra ordem (alguns fornecedores invertem)
            jk_rev = join_key(away_raw, home_raw)
            if jk_rev not in set(matches["__join_key"]):
                unmatched.append({
                    "home_raw": home_raw,
                    "away_raw": away_raw,
                    "prices": prices,
                })
                continue
            else:
                # troca
                home_raw, away_raw = away_raw, home_raw
                jk = jk_rev

        # agora precisamos mapear outcomes para home/draw/away
        # Outcomes podem vir como "Draw" e nomes de times
        # Coletar todos preços por outcome e escolher o melhor por coluna
        home_prices, draw_prices, away_prices = [], [], []
        for nm, pr in prices.items():
            nn = norm(nm)
            if nn in ("draw", "empate"):
                draw_prices.append(pr)
            elif name_matches(home_raw, nm, aliases):
                home_prices.append(pr)
            elif name_matches(away_raw, nm, aliases):
                away_prices.append(pr)
            else:
                # fallback: se outcome == home_raw/away_raw normalizados
                if norm(home_raw) == nn:
                    home_prices.append(pr)
                elif norm(away_raw) == nn:
                    away_prices.append(pr)
                # senão ignora

        oh = pick_best_price(home_prices)
        od = pick_best_price(draw_prices)
        oa = pick_best_price(away_prices)

        rows.append({
            "team_home": home_raw,
            "team_away": away_raw,
            "match_key": join_key(home_raw, away_raw),
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
        })

    df = pd.DataFrame(rows, columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"])
    df.to_csv(out_csv, index=False)
    um = pd.DataFrame(unmatched)
    um.to_csv(unmatched_csv, index=False)
    print('9:Marcador requerido pelo workflow: "theoddsapi-safe"')
    print(f"[theoddsapi-safe] linhas -> {{\"odds_theoddsapi.csv\": {len(df)}, \"unmatched_theoddsapi.csv\": {len(um)}}}")

if __name__ == "__main__":
    main()