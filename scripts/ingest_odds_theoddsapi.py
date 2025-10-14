#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestão de odds via TheOddsAPI focada em ligas brasileiras.
- Consulta múltiplas ligas (Série A/B, Copa do Brasil e Estaduais),
- Normaliza nomes BR para casar com matches_norm.csv,
- Faz média simples entre casas para odds_home/draw/away.

Entrada: --source_csv {rodada}/matches_norm.csv (match_id,home,away)
Saída:   {rodada}/odds_theoddsapi.csv (team_home,team_away,odds_home,odds_draw,odds_away)
"""

import argparse
import csv
import os
import sys
import re
from typing import Any, Dict, List, Tuple
from unidecode import unidecode
import requests


API_KEY_ENV = "THEODDS_API_KEY"

TARGET_SPORTS = [
    "soccer_brazil_serie_a",
    "soccer_brazil_serie_b",
    "soccer_brazil_cup",
    "soccer_brazil_carioca",
    "soccer_brazil_paulista",
    "soccer_brazil_mineiro",
    "soccer_brazil_gaucho",
    "soccer_brazil_paranaense",
    # amistosos como fallback
    "soccer_international_friendly",
]

# -------- Normalização BR (mesma base do apifootball) --------
ALIAS_MAP = {
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

    "crb": "crb",
}


def norm_team_br(name: str) -> str:
    if not name:
        return ""
    t = unidecode(name).lower().strip()
    t = t.replace(" - ", "-")
    t = re.sub(r"[^\w\s\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return ALIAS_MAP.get(t, t)


def read_matches_csv(path: str) -> List[Dict[str, str]]:
    rows = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        hdr = [c.strip().lower() for c in reader.fieldnames or []]
        col_home = "home" if "home" in hdr else "team_home" if "team_home" in hdr else None
        col_away = "away" if "away" in hdr else "team_away" if "team_away" in hdr else None
        col_id = "match_id" if "match_id" in hdr else None
        if not (col_home and col_away and col_id):
            raise SystemExit(f"[theoddsapi][CRITICAL] CSV {path} precisa de colunas match_id,home,away (ou team_home/team_away).")
        for r in reader:
            rows.append({
                "match_id": r.get(col_id, "").strip(),
                "home": r.get(col_home, "").strip(),
                "away": r.get(col_away, "").strip(),
            })
    return rows


def fetch_odds_all(api_key: str, regions: str) -> List[Dict[str, Any]]:
    all_events: List[Dict[str, Any]] = []
    for sport in TARGET_SPORTS:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
        params = dict(apiKey=api_key, regions=regions, markets="h2h", oddsFormat="decimal")
        r = requests.get(url, params=params, timeout=25)
        if r.status_code == 401:
            raise SystemExit("[theoddsapi][CRITICAL] 401 Unauthorized — verifique THEODDS_API_KEY e plano.")
        if r.status_code == 429:
            print("[theoddsapi][WARN] Rate limit atingido — interrompendo coleta cedo.")
            break
        if r.ok:
            try:
                js = r.json()
                if isinstance(js, list):
                    all_events.extend(js)
            except Exception:
                pass
    return all_events


def extract_three_way_prices(event: Dict[str, Any]) -> Tuple[float, float, float]:
    """
    Procura o mercado 'h2h' e retorna médias (home, draw, away).
    Se não houver draw (duas vias), retorna (home, None, away).
    """
    home_prices, draw_prices, away_prices = [], [], []
    for bk in event.get("bookmakers", []):
        for m in bk.get("markets", []):
            if (m.get("key") or "").lower() != "h2h":
                continue
            for o in m.get("outcomes", []):
                nm = (o.get("name") or "").strip()
                price = o.get("price")
                if price is None:
                    continue
                nmn = norm_team_br(nm)
                # outcomes podem vir como nome do time ou "Draw"
                if nmn in (norm_team_br(event.get("home_team", "")),):
                    home_prices.append(float(price))
                elif nmn in (norm_team_br(event.get("away_team", "")),):
                    away_prices.append(float(price))
                elif nmn in ("draw", "empate"):
                    draw_prices.append(float(price))
    def avg(x: List[float]) -> float:
        return sum(x)/len(x) if x else float("nan")
    return avg(home_prices), avg(draw_prices), avg(away_prices)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída (ex: data/out/12345)")
    ap.add_argument("--regions", default=os.environ.get("REGIONS", "uk,eu,us,au"))
    ap.add_argument("--source_csv", required=True, help="CSV de entrada com match_id,home,away (normalizado)")
    args = ap.parse_args()

    os.makedirs(args.rodada, exist_ok=True)
    out_csv = os.path.join(args.rodada, "odds_theoddsapi.csv")

    api_key = os.environ.get(API_KEY_ENV, "").strip()
    if not api_key:
        raise SystemExit("[theoddsapi][CRITICAL] THEODDS_API_KEY ausente no ambiente.")

    matches = read_matches_csv(args.source_csv)

    # coleta de odds por ligas
    all_events = fetch_odds_all(api_key, args.regions)
    # index por par normalizado (home, away)
    idx: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for ev in all_events:
        h = norm_team_br(ev.get("home_team", ""))
        a = norm_team_br(ev.get("away_team", ""))
        idx.setdefault((h, a), []).append(ev)

    # saída
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["team_home", "team_away", "odds_home", "odds_draw", "odds_away"])
        w.writeheader()

        paired = 0
        for m in matches:
            home = norm_team_br(m["home"])
            away = norm_team_br(m["away"])
            cand = idx.get((home, away), [])
            if not cand:
                # tenta o inverso (casos raros de mandante invertido pela API)
                cand = idx.get((away, home), [])
                if cand:
                    # inverte odds se achou como (away,home)
                    # mas como as odds são associadas a quem é home/away no evento,
                    # é mais seguro pular esse match do que inverter automaticamente.
                    cand = []

            if not cand:
                continue

            # agregação simples: pega média entre todas as casas/mercados dos eventos casados
            hs, ds, as_ = [], [], []
            for ev in cand:
                h, d, a = extract_three_way_prices(ev)
                if h == h: hs.append(h)  # h==h filtra nan
                if d == d: ds.append(d)
                if a == a: as_.append(a)

            if not hs or not as_:
                continue

            def avg(x: List[float]) -> float:
                return sum(x)/len(x) if x else float("nan")

            row = {
                "team_home": m["home"],
                "team_away": m["away"],
                "odds_home": f"{avg(hs):.3f}" if hs else "",
                "odds_draw": f"{avg(ds):.3f}" if ds else "",
                "odds_away": f"{avg(as_):.3f}" if as_ else "",
            }
            w.writerow(row)
            paired += 1

    print(f"[theoddsapi]Eventos soccer={len(all_events)} | jogoselecionados={len(matches)} — iniciando matching...")
    print(f"[theoddsapi]Arquivo odds_theoddsapi.csv gerado com {paired} jogos pareados.")


if __name__ == "__main__":
    main()