# scripts/ingest_odds_apifootball_rapidapi.py
from __future__ import annotations
import argparse, os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd
import numpy as np
from rapidfuzz import fuzz

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

# ---------- HTTP ----------
def _headers():
    key = os.getenv("RAPIDAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("[apifootball] RAPIDAPI_KEY não definido.")
    return {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": API_HOST,
    }

def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{API_BASE}{path}", headers=_headers(), params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"[apifootball] GET {path} HTTP {r.status_code}: {r.text[:300]}")
    j = r.json()
    if isinstance(j, dict) and j.get("errors"):
        raise RuntimeError(f"[apifootball] API error: {j.get('errors')}")
    return j

# ---------- Normalização ----------
def norm_team(s: str) -> str:
    s = (s or "").lower().strip()
    rep = [(" futebol clube",""),(" futebol",""),(" clube",""),(" club",""),
           (" fc",""),(" afc",""),(" sc",""),(" ac",""),(" de futebol",""),
           ("-sp",""),("-rj",""),("-mg",""),("-rs",""),("-pr",""),
           ("/sp",""),("/rj",""),("/mg",""),("/rs",""),("/pr",""),
           ("  "," ")]
    for a,b in rep: s = s.replace(a,b)
    return s

# ---------- Busca de time por nome ----------
def find_team_id(name: str, country_hint: Optional[str]=None) -> Tuple[int,str]:
    """Procura ID do time por nome (fuzzy). Retorna (team_id, nome_oficial)."""
    data = _get("/teams", {"search": name.strip()})
    res = data.get("response", [])
    if not res:
        raise RuntimeError(f"[apifootball] time não encontrado: {name}")
    # ranking fuzzy
    choices = []
    for it in res:
        tname = it["team"]["name"]
        country = it.get("team", {}).get("country") or it.get("country")
        score = fuzz.token_set_ratio(norm_team(name), norm_team(tname))
        if country_hint and country and country_hint.lower() in str(country).lower():
            score += 3
        choices.append((score, it["team"]["id"], tname, country))
    choices.sort(key=lambda x: x[0], reverse=True)
    best = choices[0]
    return int(best[1]), str(best[2])

# ---------- Localiza fixture do dia (pelo time) ----------
def find_fixture_id_for_match(date_iso: str, home_id: int, away_id: int, season_year: int) -> Optional[int]:
    """Procura fixture na data: filtramos fixtures do mandante e conferimos o adversário."""
    data = _get("/fixtures", {"date": date_iso, "team": home_id, "season": season_year})
    for it in data.get("response", []):
        teams = it.get("teams", {})
        h = teams.get("home", {}).get("id")
        a = teams.get("away", {}).get("id")
        if int(h)==home_id and int(a)==away_id:
            return int(it["fixture"]["id"])
    return None

# ---------- Odds por fixture ----------
def fetch_odds_fixture(fixture_id: int) -> Dict[str, Tuple[float,float,float]]:
    """Retorna dict bookmaker -> (odd_home, odd_draw, odd_away) para 1X2."""
    out: Dict[str, Tuple[float,float,float]] = {}
    data = _get("/odds", {"fixture": fixture_id})
    for it in data.get("response", []):
        for b in it.get("bookmakers", []):
            bname = (b.get("name") or b.get("id") or "").lower()
            for mv in b.get("bets", []):
                outcomes = mv.get("values", [])
                labels = { (v.get("value") or "").strip(): v.get("odd") for v in outcomes }
                def _get_price(keys):
                    for k in keys:
                        if k in labels:
                            try: return float(labels[k])
                            except: pass
                    return None
                oh = _get_price(["Home","1"])
                o_draw = _get_price(["Draw","X"])
                oa = _get_price(["Away","2"])
                if oh and o_draw and oa:
                    out[bname] = (oh, o_draw, oa)
    return out

# ---------- Probabilidades (desvigoramento simples) ----------
def devig(oh: float, o_draw: float, oa: float) -> Tuple[float,float,float,float]:
    p_home, p_draw, p_away = 1/oh, 1/o_draw, 1/oa
    s = p_home + p_draw + p_away
    if s<=0: return p_home, p_draw, p_away, s
    return p_home/s, p_draw/s, p_away/s, s

def consensus(book_odds: Dict[str,Tuple[float,float,float]]) -> Tuple[float,float,float,float,int,str]:
    if not book_odds:
        raise ValueError("sem bookmakers")
    probs=[]; overs=[]
    for _,(oh,o_draw,oa) in book_odds.items():
        p_home, p_draw, p_away, over = devig(oh, o_draw, oa)
        probs.append((p_home, p_draw, p_away)); overs.append(over)
    p = np.mean(np.array(probs), axis=0)
    providers = ",".join(sorted(book_odds.keys()))
    return float(p[0]), float(p[1]), float(p[2]), float(np.mean(overs)), len(book_odds), providers

# ---------- Principal ----------
def main():
    ap = argparse.ArgumentParser(description="Odds via API-Football (RapidAPI) -> odds_apifootball.csv")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-10-05_14")
    ap.add_argument("--allow-partial", action="store_true")
    ap.add_argument("--country-hint", default="", help="Ex.: Brazil/England/Spain (ajuda no match de times)")
    args = ap.parse_args()

    # data base da rodada (YYYY-MM-DD)
    try:
        date_iso = args.rodada.split("_", 1)[0]
        season_year = int(date_iso.split("-")[0])
    except Exception:
        raise RuntimeError(f"[apifootball] Não consegui extrair data/ano da rodada: {args.rodada}")

    base = Path(f"data/out/{args.rodada}")
    matches_path = base / "matches.csv"
    out_path = base / "odds_apifootball.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not matches_path.exists() or matches_path.stat().st_size == 0:
        raise RuntimeError(f"[apifootball] matches.csv ausente/vazio: {matches_path}")

    # >>> AQUI usamos pandas normalmente (sem variável local 'pd')
    matches = pd.read_csv(matches_path)
    matches = matches.rename(columns=str.lower)
    for col in ("match_id","home","away"):
        if col not in matches.columns:
            raise RuntimeError(f"[apifootball] matches.csv faltando coluna: {col}")

    rows = []; missing = []
    for _, r in matches.iterrows():
        mid = int(r["match_id"])
        home_name = str(r["home"])
        away_name = str(r["away"])
        try:
            hid, hname = find_team_id(home_name, country_hint=args.country_hint or None)
            aid, aname = find_team_id(away_name, country_hint=args.country_hint or None)
        except Exception:
            missing.append(mid); continue

        fix_id = find_fixture_id_for_match(date_iso, hid, aid, season_year)
        if not fix_id:
            missing.append(mid); continue

        book = fetch_odds_fixture(fix_id)
        if not book:
            missing.append(mid); continue

        p_home, p_draw, p_away, over, n, prov = consensus(book)
        row = {
            "match_id": mid,
            "odd_home": round(1.0/max(p_home,1e-9), 4),
            "odd_draw": round(1.0/max(p_draw,1e-9), 4),
            "odd_away": round(1.0/max(p_away,1e-9), 4),
            "n_bookmakers": n,
            "overround_mean": round(over, 4),
            "providers": prov
        }
        rows.append(row)

    if missing and not args.allow_partial:
        raise RuntimeError(f"[apifootball] Sem odds para match_id: {sorted(missing)}")
    if not rows and missing:
        raise RuntimeError("[apifootball] Nenhuma odd coletada.")

    df_out = pd.DataFrame(rows)
    df_out.to_csv(out_path, index=False)
    print(f"[apifootball] OK: {len(df_out)} linhas -> {out_path}")
    if missing:
        print(f"[apifootball] Aviso: {len(missing)} jogos sem odds (ids={sorted(missing)})")

if __name__ == "__main__":
    main()
