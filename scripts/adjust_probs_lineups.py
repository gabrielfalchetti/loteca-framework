# scripts/adjust_probs_lineups.py
# Enriquecimento pré-jogo usando API-Football (RapidAPI):
# - mapeia fixture por H2H/data (±janela)
# - coleta contagem de lesões por time
# - ajusta probabilidades p_home/p_draw/p_away penalizando time com +lesões
# - gera data/out/<rodada>/joined_enriched.csv
from __future__ import annotations
import argparse, os
from pathlib import Path
from typing import Optional, Tuple
from datetime import date, timedelta

import requests
import pandas as pd
import numpy as np
from rapidfuzz import fuzz

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

def _headers():
    key = os.getenv("RAPIDAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("[adjust] RAPIDAPI_KEY não definido.")
    return {"X-RapidAPI-Key": key, "X-RapidAPI-Host": API_HOST}

def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{API_BASE}{path}", headers=_headers(), params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict) and j.get("errors"):
        raise RuntimeError(f"[adjust] API error: {j.get('errors')}")
    return j

def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    rep = [(" futebol clube",""),(" futebol",""),(" clube",""),(" club",""),
           (" fc",""),(" afc",""),(" sc",""),(" ac",""),(" de futebol",""),
           ("-sp",""),("-rj",""),("-mg",""),("-rs",""),("-pr",""),
           ("/sp",""),("/rj",""),("/mg",""),("/rs",""),("/pr",""),
           ("  "," ")]
    for a,b in rep: s = s.replace(a,b)
    return s

def _find_team_id(name: str, country_hint: Optional[str]=None) -> Tuple[int,str,str]:
    data = _get("/teams", {"search": name})
    res = data.get("response", [])
    if not res:
        raise RuntimeError(f"[adjust] time não encontrado: {name}")
    best=(0,None,"","")
    for it in res:
        tname = it["team"]["name"]
        country = (it.get("team",{}) or {}).get("country") or ""
        score = fuzz.token_set_ratio(_norm(name), _norm(tname))
        if country_hint and country and country_hint.lower() in country.lower():
            score += 3
        if score > best[0]:
            best=(score, it["team"]["id"], tname, country)
    return int(best[1]), str(best[2]), str(best[3])

def _fixtures_by_date(d: str) -> list:
    return _get("/fixtures", {"date": d}).get("response", [])

def _find_fixture_id(date_iso: str, home: str, away: str, season_year: int,
                     country_hint: Optional[str], days_window: int, min_match: int) -> Optional[int]:
    # tenta por H2H (ids)
    try:
        hid,_,_ = _find_team_id(home, country_hint)
        aid,_,_ = _find_team_id(away, country_hint)
        h2h = f"{hid}-{aid}"
        resp = _get("/fixtures", {"h2h": h2h, "season": season_year}).get("response", [])
        def sc(d: str) -> int:
            try:
                d0 = date.fromisoformat(date_iso); d1 = date.fromisoformat(d[:10])
                diff = abs((d1-d0).days); return 10 - diff
            except: return -999
        best=None; sbest=-999
        for it in resp:
            d = (it["fixture"].get("date") or "")[:10]
            s = sc(d)
            if s > sbest:
                best=it; sbest=s
        if best and sbest >= 8:  # data exata/±1/±2
            return int(best["fixture"]["id"])
    except Exception:
        pass
    # varredura por data ± janela, casando nomes
    d0 = date.fromisoformat(date_iso)
    best_id=None; best_score=-1
    for off in range(-days_window, days_window+1):
        dstr = (d0 + timedelta(days=off)).isoformat()
        resp = _fixtures_by_date(dstr)
        for it in resp:
            hn = (it.get("teams",{}).get("home",{}) or {}).get("name","")
            an = (it.get("teams",{}).get("away",{}) or {}).get("name","")
            s1 = fuzz.token_set_ratio(_norm(home), _norm(hn))
            s2 = fuzz.token_set_ratio(_norm(away), _norm(an))
            sc = (s1+s2)//2
            if sc > best_score:
                best_score = sc; best_id = int(it["fixture"]["id"])
    if best_score >= min_match:
        return best_id
    return None

def _fetch_injuries(team_id: int, season_year: int) -> int:
    try:
        j = _get("/injuries", {"team": team_id, "season": season_year})
        return int(len(j.get("response", [])))
    except Exception:
        return 0

def _probs_from_odds(oh, od, oa):
    arr = np.array([oh,od,oa], dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0/arr
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s <= 0: return np.array([1/3,1/3,1/3], dtype=float)
    return inv/s

def _apply_injury_adjust(p: np.ndarray, inj_home: int, inj_away: int, alpha=0.05, cap=6) -> np.ndarray:
    """penaliza lado com mais lesões; alpha ~ 5% por lesão até cap; preserva proporção de empate"""
    ih = min(max(inj_home,0), cap); ia = min(max(inj_away,0), cap)
    diff = ih - ia
    if diff == 0:
        return p
    factor_home = 1.0 - alpha*max(diff,0)   # se home tem mais lesões, reduz
    factor_away = 1.0 - alpha*max(-diff,0)  # se away tem mais lesões, reduz
    ph = max(p[0]*factor_home, 1e-9)
    pa = max(p[2]*factor_away, 1e-9)
    pd = max(p[1], 1e-9)
    s = ph+pd+pa
    return np.array([ph/s, pd/s, pa/s], dtype=float)

def main():
    ap = argparse.ArgumentParser(description="Ajuste de probabilidades por lesões (API-Football)")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-10-05_14")
    ap.add_argument("--country-hint", default="Brazil")
    ap.add_argument("--days-window", type=int, default=2)
    ap.add_argument("--min-match", type=int, default=85)
    ap.add_argument("--alpha", type=float, default=0.05, help="penalização por lesão (5% default)")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    joined_path = base/"joined.csv"
    out_path = base/"joined_enriched.csv"
    if not joined_path.exists() or joined_path.stat().st_size==0:
        raise RuntimeError(f"[adjust] joined.csv ausente/vazio: {joined_path}")

    df = pd.read_csv(joined_path)
    need_cols = ["match_id","home","away","odd_home","odd_draw","odd_away"]
    miss = [c for c in need_cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"[adjust] joined.csv faltando colunas: {miss}")

    date_iso = args.rodada.split("_",1)[0]
    season_year = int(date_iso.split("-")[0])

    rows=[]
    for _, r in df.iterrows():
        home = str(r["home"]); away = str(r["away"])
        oh, od, oa = r["odd_home"], r["odd_draw"], r["odd_away"]

        if pd.isna(oh) or pd.isna(od) or pd.isna(oa):
            rows.append({**r, "p_home": "", "p_draw": "", "p_away": "",
                         "inj_home": "", "inj_away": "", "source_adjust": "none"})
            continue

        p = _probs_from_odds(float(oh), float(od), float(oa))
        try:
            fid = _find_fixture_id(date_iso, home, away, season_year,
                                   country_hint=args.country_hint, days_window=args.days_window,
                                   min_match=args.min_match)
            if not fid:
                rows.append({**r, "p_home": p[0], "p_draw": p[1], "p_away": p[2],
                             "inj_home": "", "inj_away": "", "source_adjust": "none"})
                continue
            fx = _get("/fixtures", {"id": fid}).get("response", [])
            if not fx:
                rows.append({**r, "p_home": p[0], "p_draw": p[1], "p_away": p[2],
                             "inj_home": "", "inj_away": "", "source_adjust": "none"})
                continue
            t_home = fx[0].get("teams",{}).get("home",{}).get("id")
            t_away = fx[0].get("teams",{}).get("away",{}).get("id")
            inj_h = _fetch_injuries(int(t_home), season_year) if t_home else 0
            inj_a = _fetch_injuries(int(t_away), season_year) if t_away else 0
            p_adj = _apply_injury_adjust(p, inj_h, inj_a, alpha=args.alpha)
            rows.append({**r,
                         "p_home": p_adj[0], "p_draw": p_adj[1], "p_away": p_adj[2],
                         "inj_home": inj_h, "inj_away": inj_a, "source_adjust": "injuries"})
        except Exception:
            rows.append({**r, "p_home": p[0], "p_draw": p[1], "p_away": p[2],
                         "inj_home": "", "inj_away": "", "source_adjust": "none"})

    out = pd.DataFrame(rows)

    # recalcula odds coerentes a partir das p ajustadas (se existirem)
    def inv(p): return np.where(p>1e-9, 1.0/p, np.nan)
    mask = out[["p_home","p_draw","p_away"]].notna().all(axis=1)
    out.loc[mask, ["odd_home","odd_draw","odd_away"]] = inv(out.loc[mask, ["p_home","p_draw","p_away"]].values)

    out.to_csv(out_path, index=False)
    print(f"[adjust] joined_enriched.csv salvo em {out_path}")

if __name__ == "__main__":
    main()
