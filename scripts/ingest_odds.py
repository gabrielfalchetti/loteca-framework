# scripts/ingest_odds.py
# Coleta odds REAIS via TheOddsAPI e gera consenso multi-bookmakers com devig.
# - Sem mocks/simulações.
# - Falha por padrão se não conseguir odds para algum jogo (use --allow-partial se quiser seguir).
#
# Saída: data/out/<RODADA>/odds.csv com colunas:
#   match_id, odd_home, odd_draw, odd_away, n_bookmakers, overround_mean, providers
#
# Uso típico (workflow):
#   export ODDS_API_KEY=... (como secret)
#   python scripts/ingest_odds.py --rodada 2025-09-20_21 \
#       --sport soccer_brazil_campeonato --regions uk,eu --market h2h
#
# Notas:
# - O script lê data/out/<RODADA>/matches.csv para casar (home/away).
# - Matching usa normalização + similaridade (rapidfuzz) p/ tolerar pequenas variações de nomes.
# - Consenso: converte odds->probs, remove vigorish (devig proporcional por mercado/bookmaker),
#   depois faz média ponderada de probabilidades (peso maior p/ Pinnacle e Betfair quando existirem).
# - Se algum jogo NÃO obtiver odds, por padrão lança erro; com --allow-partial, preenche só os que tiver.

from __future__ import annotations
import argparse
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np
import requests
from rapidfuzz import fuzz, process

# ---------- Config peso de bookmakers no consenso ----------
# Maior peso para Pinnacle e Betfair se presentes; demais = 1.0
BOOK_WEIGHTS_DEFAULT = {
    "pinnacle": 2.0,
    "betfair":  1.8,   # a API retorna 'Betfair' como bookmaker em alguns agregadores
    # Demais não listados assumem 1.0
}

# ---------- Normalização de nomes ----------
def norm_team(s: str) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    s = s.lower().strip()
    repl = [
        (" futebol clube", ""), (" futebol", ""), (" clube", ""), (" club", ""),
        (" fc", ""), (" afc", ""), (" sc", ""), (" ac", ""), (" de futebol", ""),
        ("  ", " "),
    ]
    for a, b in repl:
        s = s.replace(a, b)
    return s

# ---------- Leitura de matches ----------
def read_matches(rodada: str, matches_path: Optional[str] = None) -> pd.DataFrame:
    base = Path(f"data/out/{rodada}")
    mp = Path(matches_path) if matches_path else base / "matches.csv"
    if not mp.exists() or mp.stat().st_size == 0:
        raise RuntimeError(f"[ingest_odds] matches.csv ausente/vazio: {mp}")
    m = pd.read_csv(mp)
    m = m.rename(columns={c: c.lower() for c in m.columns})
    maps = {
        "mandante":"home", "visitante":"away",
        "time_casa":"home", "time_fora":"away",
        "casa":"home", "fora":"away",
        "home_team":"home", "away_team":"away",
        "data_jogo":"date", "data":"date", "matchdate":"date",
        "id":"match_id",
    }
    m = m.rename(columns={k:v for k,v in maps.items() if k in m.columns})
    if "match_id" not in m.columns:
        m = m.reset_index(drop=True)
        m["match_id"] = m.index + 1
    for col in ("home","away"):
        if col in m.columns:
            m[col] = m[col].astype(str).str.strip()
    return m[["match_id","home","away"]].copy()

# ---------- Chamada TheOddsAPI ----------
def fetch_theoddsapi(sport: str, regions: List[str], market: str, api_key: str) -> List[dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {
        "apiKey": api_key,
        "regions": ",".join(regions),
        "markets": market,             # "h2h" = 1x2 pré-jogo
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"[ingest_odds] TheOddsAPI HTTP {r.status_code}: {r.text[:250]}")
    return r.json()

# ---------- Parse de odds da API para estrutura { (home,away) -> {bookmaker: (oh,od,oa)} } ----------
def parse_api_events(data: List[dict]) -> Dict[Tuple[str,str], Dict[str, Tuple[float,float,float]]]:
    # Alguns eventos fornecem 'home_team' e outcomes por bookmaker/market
    events: Dict[Tuple[str,str], Dict[str, Tuple[float,float,float]]] = {}
    for ev in data:
        # Tentativa de determinar home/away
        home_team_raw = ev.get("home_team") or ""
        home_norm = norm_team(home_team_raw)

        # Descobrir away a partir dos outcomes (nome mais frequente != home)
        away_candidates: List[str] = []
        for bk in ev.get("bookmakers", []):
            for mk in bk.get("markets", []):
                for outc in mk.get("outcomes", []):
                    name = norm_team(outc.get("name",""))
                    if name and name != home_norm:
                        away_candidates.append(name)
        away_norm = None
        if away_candidates:
            # pega o mais frequente
            counts: Dict[str,int] = {}
            for t in away_candidates:
                counts[t] = counts.get(t, 0) + 1
            away_norm = max(counts, key=counts.get)

        if not (home_norm and away_norm and home_norm != away_norm):
            # não conseguimos identificar um par consistente home/away — pula
            continue

        key = (home_norm, away_norm)
        if key not in events:
            events[key] = {}

        for bk in ev.get("bookmakers", []):
            bk_key = bk.get("key","").lower() or bk.get("title","").lower()
            # outcomes por market "h2h"
            oh = od = oa = None
            for mk in bk.get("markets", []):
                if mk.get("key") != "h2h":
                    continue
                for outc in mk.get("outcomes", []):
                    nm = norm_team(outc.get("name",""))
                    pr = outc.get("price", None)
                    try:
                        pr = float(pr)
                    except Exception:
                        pr = None
                    if pr is None:
                        continue
                    if nm == home_norm:
                        oh = pr
                    elif nm in ("draw","empate","x"):
                        od = pr
                    elif nm == away_norm:
                        oa = pr
            if oh and od and oa:
                events[key][bk_key] = (oh, od, oa)
    return events

# ---------- Devig proporcional por mercado ----------
def devig_proportional(oh: float, od: float, oa: float) -> Tuple[float,float,float,float]:
    # odds -> prob imp
    ph, pd, pa = 1.0/oh, 1.0/od, 1.0/oa
    s = ph + pd + pa
    overround = s
    if s <= 0:
        return ph, pd, pa, overround
    # normalização proporcional
    ph_f = ph / s
    pd_f = pd / s
    pa_f = pa / s
    return ph_f, pd_f, pa_f, overround

# ---------- Consenso multi-bookmakers (probabilidades) ----------
def consensus_probs(book_odds: Dict[str, Tuple[float,float,float]]) -> Tuple[float,float,float,float,int]:
    if not book_odds:
        raise ValueError("sem bookmakers para consenso")
    probs = []
    overrounds = []
    weights = []
    for bk, (oh, od, oa) in book_odds.items():
        ph, pd, pa, over = devig_proportional(oh, od, oa)
        probs.append((ph, pd, pa))
        overrounds.append(over)
        w = BOOK_WEIGHTS_DEFAULT.get(bk.lower(), 1.0)
        weights.append(w)
    probs = np.array(probs)  # shape (n, 3)
    weights = np.array(weights).reshape(-1,1)
    # média ponderada das probabilidades já devigadas
    p_mean = np.sum(probs * weights, axis=0) / np.sum(weights)
    over_mean = float(np.mean(overrounds))
    n_books = len(book_odds)
    return float(p_mean[0]), float(p_mean[1]), float(p_mean[2]), over_mean, n_books

# ---------- Matching (home/away) entre matches.csv e API ----------
def match_events(matches: pd.DataFrame,
                 api_events: Dict[Tuple[str,str], Dict[str,Tuple[float,float,float]]],
                 min_ratio: int = 90) -> List[dict]:
    # Índice rápido de chaves API
    api_keys = list(api_events.keys())
    api_join = []
    for _, row in matches.iterrows():
        mid = row["match_id"]
        h = norm_team(row["home"])
        a = norm_team(row["away"])

        # 1) Tentativa de match exato
        if (h,a) in api_events:
            api_join.append({"match_id": mid, "key": (h,a)})
            continue

        # 2) Fuzzy match: procura par com maior escore combinado
        best_key = None
        best_score = -1
        for (hh, aa) in api_keys:
            s1 = fuzz.token_set_ratio(h, hh)
            s2 = fuzz.token_set_ratio(a, aa)
            s = (s1 + s2) / 2
            if s > best_score:
                best_score = s
                best_key = (hh, aa)
        if best_key and best_score >= min_ratio:
            api_join.append({"match_id": mid, "key": best_key})
        # else: sem match → ficará sem odds (e tratar depois)

    return api_join

# ---------- Principal ----------
def main():
    ap = argparse.ArgumentParser(description="Ingest odds reais (TheOddsAPI) -> data/out/<rodada>/odds.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--matches", default=None, help="Caminho alternativo para matches.csv")
    ap.add_argument("--sport", default="soccer_brazil_campeonato", help="TheOddsAPI sport key (ex.: soccer_brazil_campeonato)")
    ap.add_argument("--regions", default="uk,eu", help="Regiões (comma-separated): uk,eu,us,au")
    ap.add_argument("--market", default="h2h", help="Mercado (h2h = 1x2)")
    ap.add_argument("--min-match", type=int, default=90, help="mín. similaridade fuzzy para casar times (0-100)")
    ap.add_argument("--allow-partial", action="store_true", help="não falha se algum jogo ficar sem odds")
    args = ap.parse_args()

    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("[ingest_odds] ODDS_API_KEY não definido (coloque como secret no CI).")

    matches = read_matches(args.rodada, args.matches)
    data = fetch_theoddsapi(args.sport, [r.strip() for r in args.regions.split(",") if r.strip()], args.market, api_key)
    ev_map = parse_api_events(data)

    # casamento eventos
    join_map = match_events(matches, ev_map, min_ratio=args.min_match)

    rows = []
    missing = []
    for _, r in matches.iterrows():
        mid = r["match_id"]
        # acha key mapeada
        rec = next((x for x in join_map if x["match_id"] == mid), None)
        if not rec:
            missing.append(int(mid))
            continue
        key = rec["key"]
        book_odds = ev_map.get(key, {})
        if not book_odds:
            missing.append(int(mid))
            continue

        ph, pd, pa, over_mean, n_books = consensus_probs(book_odds)
        # volta para odds "justas" (sem vig) — 1/p
        oh = round(1.0 / ph, 4)
        od = round(1.0 / pd, 4)
        oa = round(1.0 / pa, 4)
        providers = ",".join(sorted(book_odds.keys()))

        rows.append({
            "match_id": mid,
            "odd_home": oh,
            "odd_draw": od,
            "odd_away": oa,
            "n_bookmakers": n_books,
            "overround_mean": round(over_mean, 4),
            "providers": providers,
        })

    if missing and not args.allow_partial:
        raise RuntimeError(f"[ingest_odds] Sem odds para match_id: {sorted(missing)} (use --allow-partial para prosseguir).")

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("[ingest_odds] Nenhuma odd coletada (verifique sport/regions/nomes dos times e a API key).")

    out_path = Path(f"data/out/{args.rodada}/odds.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"[ingest_odds] OK: {len(out)} linhas -> {out_path}")
    if missing:
        print(f"[ingest_odds] Aviso: {len(missing)} jogos sem odds (ids={sorted(missing)})")

if __name__ == "__main__":
    main()
