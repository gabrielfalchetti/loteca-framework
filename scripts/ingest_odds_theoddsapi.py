# scripts/ingest_odds_theoddsapi.py
# Coleta odds H2H (1X2) da TheOddsAPI e faz matching fuzzy com a lista de jogos.
# Compatível com matches_norm.csv (colunas: match_id,home,away,home_norm,away_norm).
#
# Saída: {rodada}/odds_theoddsapi.csv com colunas:
#   match_id,team_home,team_away,odds_home,odds_draw,odds_away

import os
import sys
import re
import csv
import argparse
import requests
from statistics import median
from typing import Dict, List, Tuple, Optional
from unidecode import unidecode
from rapidfuzz import fuzz

API_UPCOMING = "https://api.the-odds-api.com/v4/sports/upcoming/odds"


def _norm(s: str) -> str:
    s = unidecode((s or "").strip().lower())
    s = re.sub(r"[^a-z0-9\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _read_whitelist(path: str) -> List[Dict[str, str]]:
    assert os.path.exists(path), f"{path} not found"
    rows = []
    with open(path, encoding="utf-8") as f:
        rd = csv.DictReader(f)
        # Aceita tanto matches_source.csv quanto matches_norm.csv
        for r in rd:
            home = r.get("home", "")
            away = r.get("away", "")
            home_norm = r.get("home_norm") or _norm(home)
            away_norm = r.get("away_norm") or _norm(away)
            rows.append({
                "match_id": r["match_id"],
                "home": home,
                "away": away,
                "home_norm": _norm(home_norm),
                "away_norm": _norm(away_norm),
            })
    return rows


def _fetch_events(regions: str, api_key: str) -> List[Dict]:
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
    }
    r = requests.get(API_UPCOMING, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # Filtra só futebol (sport_key começa com 'soccer_')
    events = [e for e in data if str(e.get("sport_key", "")).startswith("soccer_")]
    return events


def _event_home_away(e: Dict) -> Tuple[str, str]:
    home = e.get("home_team", "") or ""
    teams = e.get("teams") or []
    away = ""
    if len(teams) == 2:
        away = teams[1] if teams[0] == home else teams[0]
    return home, away


def _best_match(home_norm: str, away_norm: str, events: List[Dict]) -> Tuple[Optional[Dict], float]:
    """Retorna (evento, score) usando token_sort_ratio na dupla (home,away)."""
    top = (None, 0.0)
    for e in events:
        eh, ea = _event_home_away(e)
        eh_n, ea_n = _norm(eh), _norm(ea)
        # Soma das similaridades home e away (ordem importa).
        s1 = (fuzz.token_sort_ratio(home_norm, eh_n) + fuzz.token_sort_ratio(away_norm, ea_n)) / 2
        # Também tenta invertido; por via das dúvidas, pega o melhor.
        s2 = (fuzz.token_sort_ratio(home_norm, ea_n) + fuzz.token_sort_ratio(away_norm, eh_n)) / 2
        s = max(s1, s2)
        if s > top[1]:
            top = (e, s)
    return top


def _collect_prices(e: Dict) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Consolida odds por mediana das casas para (home, draw, away)."""
    eh, ea = _event_home_away(e)
    eh_n, ea_n = _norm(eh), _norm(ea)

    prices = {"home": [], "draw": [], "away": []}

    for bk in e.get("bookmakers", []) or []:
        for mk in bk.get("markets", []) or []:
            if mk.get("key") != "h2h":
                continue
            outcomes = mk.get("outcomes", []) or []
            # outcomes: [{name:'Team A', price:1.8}, {name:'Team B', price:2.0}, {name:'Draw', price:3.2}]
            for oc in outcomes:
                nm = _norm(oc.get("name", ""))
                pr = oc.get("price", None)
                if pr is None:
                    continue
                try:
                    p = float(pr)
                except Exception:
                    continue

                if "draw" in nm or "empate" in nm:
                    prices["draw"].append(p)
                    continue

                # Mapeia time→home/away por nome normalizado (com fallback fuzzy)
                if nm == eh_n or fuzz.token_sort_ratio(nm, eh_n) >= 92:
                    prices["home"].append(p)
                elif nm == ea_n or fuzz.token_sort_ratio(nm, ea_n) >= 92:
                    prices["away"].append(p)
                # else: ignora nomes estranhos

    def _med(lst):
        return round(median(lst), 4) if lst else None

    return _med(prices["home"]), _med(prices["draw"]), _med(prices["away"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída (ex: data/out/123456)")
    ap.add_argument("--regions", required=True, help="Regiões TheOddsAPI (ex: uk,eu,us,au)")
    ap.add_argument("--source_csv", required=True, help="Arquivo de jogos (matches_norm.csv ou matches_source.csv)")
    args = ap.parse_args()

    api_key = os.environ.get("THEODDS_API_KEY", "")
    if not api_key:
        print("[theoddsapi][ERROR] THEODDS_API_KEY vazio.", file=sys.stderr)
        return 2

    wl = _read_whitelist(args.source_csv)
    try:
        events = _fetch_events(args.regions, api_key)
    except Exception as e:
        print(f"[theoddsapi][ERROR] Falha ao consultar TheOddsAPI: {e}", file=sys.stderr)
        events = []

    print(f"[theoddsapi]Eventos soccer={len(events)} | jogoselecionados={len(wl)} — iniciando matching...")

    rows = []
    for r in wl:
        ev, score = _best_match(r["home_norm"], r["away_norm"], events)
        if not ev or score < 80.0:  # limiar conservador
            continue
        oh, od, oa = _collect_prices(ev)
        if oh is None or od is None or oa is None:
            continue
        rows.append({
            "match_id": r["match_id"],
            "team_home": r["home"],
            "team_away": r["away"],
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
        })

    os.makedirs(args.rodada, exist_ok=True)
    outp = os.path.join(args.rodada, "odds_theoddsapi.csv")
    with open(outp, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","team_home","team_away","odds_home","odds_draw","odds_away"])
        wr.writeheader()
        wr.writerows(rows)

    print(f"[theoddsapi]Arquivo odds_theoddsapi.csv gerado com {len(rows)} jogos pareados.")
    return 0


if __name__ == "__main__":
    sys.exit(main())