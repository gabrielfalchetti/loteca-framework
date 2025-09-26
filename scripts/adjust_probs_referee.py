# scripts/adjust_probs_referee.py
# Ajuste de probabilidades por árbitro/discipla:
# - Lê joined.csv
# - Mapeia fixture (API-Football) -> extrai árbitro
# - Se existir config/referee_bias.csv, aplica ajustes (pp = pontos percentuais)
#   Colunas esperadas: referee,home_bias_pp,draw_bias_pp,away_bias_pp,cards_pg (opcional)
# - Gera joined_referee.csv com p_* ajustadas e odds recalculadas (se p_* existirem)
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
        raise RuntimeError("[referee] RAPIDAPI_KEY não definido.")
    return {"X-RapidAPI-Key": key, "X-RapidAPI-Host": API_HOST}

def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{API_BASE}{path}", headers=_headers(), params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict) and j.get("errors"):
        raise RuntimeError(f"[referee] API error: {j.get('errors')}")
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
        raise RuntimeError(f"[referee] time não encontrado: {name}")
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
    # H2H ids
    try:
        hid,_,_ = _find_team_id(home, country_hint)
        aid,_,_ = _find_team_id(away, country_hint)
        resp = _get("/fixtures", {"h2h": f"{hid}-{aid}", "season": season_year}).get("response", [])
        def sc(d: str) -> int:
            try:
                d0 = date.fromisoformat(date_iso); d1 = date.fromisoformat(d[:10])
                return 10 - abs((d1-d0).days)
            except: return -999
        best=None; sbest=-999
        for it in resp:
            s = sc((it["fixture"].get("date") or "")[:10])
            if s > sbest: best=it; sbest=s
        if best and sbest >= 8:
            return int(best["fixture"]["id"])
    except Exception:
        pass
    # varredura por nome
    d0 = date.fromisoformat(date_iso)
    best_id=None; best_score=-1
    for off in range(-(days_window), days_window+1):
        dstr = (d0 + timedelta(days=off)).isoformat()
        for it in _fixtures_by_date(dstr):
            hn = (it.get("teams",{}).get("home",{}) or {}).get("name","")
            an = (it.get("teams",{}).get("away",{}) or {}).get("name","")
            s1 = fuzz.token_set_ratio(_norm(home), _norm(hn))
            s2 = fuzz.token_set_ratio(_norm(away), _norm(an))
            sc = (s1+s2)//2
            if sc > best_score:
                best_score=sc; best_id=int(it["fixture"]["id"])
    return best_id if best_score>=min_match else None

def _probs_from_odds(oh, od, oa):
    arr = np.array([oh,od,oa], dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0/arr
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s <= 0: return np.array([1/3,1/3,1/3], dtype=float)
    return inv/s

def _renorm(p):
    p = np.maximum(p, 1e-9)
    return p / p.sum()

def _apply_bias(p: np.ndarray, bh: float, bx: float, ba: float) -> np.ndarray:
    """Aplica deslocamentos em pontos percentuais (pp) e renormaliza."""
    ph, pd, pa = p.tolist()
    ph2 = ph + bh
    pd2 = pd + bx
    pa2 = pa + ba
    # não deixa negativos
    ph2 = max(ph2, 1e-9); pd2 = max(pd2, 1e-9); pa2 = max(pa2, 1e-9)
    return _renorm(np.array([ph2,pd2,pa2], dtype=float))

def main():
    ap = argparse.ArgumentParser(description="Ajuste de probabilidades por árbitro")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--country-hint", default="Brazil")
    ap.add_argument("--days-window", type=int, default=2)
    ap.add_argument("--min-match", type=int, default=85)
    ap.add_argument("--bias-cap", type=float, default=0.04, help="cap máx por lado em pontos (ex: 0.04 = 4 pp)")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    joined = base/"joined.csv"
    out = base/"joined_referee.csv"
    if not joined.exists() or joined.stat().st_size == 0:
        raise RuntimeError(f"[referee] joined.csv ausente/vazio: {joined}")

    df = pd.read_csv(joined)
    need = ["match_id","home","away","odd_home","odd_draw","odd_away"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise RuntimeError(f"[referee] joined.csv faltando colunas: {miss}")

    # tabela opcional
    bias_map = {}
    ref_tbl = Path("config/referee_bias.csv")
    if ref_tbl.exists() and ref_tbl.stat().st_size>0:
        TB = pd.read_csv(ref_tbl)
        cols_ok = set(["referee","home_bias_pp","draw_bias_pp","away_bias_pp"])
        if cols_ok.issubset(TB.columns):
            for _, rr in TB.iterrows():
                nm = str(rr["referee"]).strip().lower()
                bh = float(rr["home_bias_pp"])
                bx = float(rr["draw_bias_pp"])
                ba = float(rr["away_bias_pp"])
                # cap de segurança
                cap = float(args.bias_cap)
                bh = max(-cap, min(cap, bh))
                bx = max(-cap, min(cap, bx))
                ba = max(-cap, min(cap, ba))
                bias_map[nm] = (bh, bx, ba)

    date_iso = args.rodada.split("_",1)[0]
    season_year = int(date_iso.split("-")[0])

    rows=[]
    for _, r in df.iterrows():
        home, away = str(r["home"]), str(r["away"])
        oh, od, oa = r["odd_home"], r["odd_draw"], r["odd_away"]

        # Se não houver odds, apenas carrega árbitro (se possível) e segue
        p = np.array([np.nan, np.nan, np.nan], dtype=float)
        if not (pd.isna(oh) or pd.isna(od) or pd.isna(oa)):
            p = _probs_from_odds(float(oh), float(od), float(oa))

        referee_name = ""
        try:
            fid = _find_fixture_id(date_iso, home, away, season_year,
                                   country_hint=args.country_hint,
                                   days_window=args.days_window, min_match=args.min_match)
            if fid:
                fx = _get("/fixtures", {"id": fid}).get("response", [])
                if fx:
                    referee_name = (fx[0].get("fixture",{}) or {}).get("referee") or ""
        except Exception:
            pass

        # aplica bias se houver mapeamento para o árbitro
        p_adj = p.copy()
        if referee_name and not np.isnan(p).any():
            key = referee_name.strip().lower()
            if key in bias_map:
                bh,bx,ba = bias_map[key]
                p_adj = _apply_bias(p, bh, bx, ba)

        rows.append({
            **r,
            "referee": referee_name,
            "p_home": p_adj[0] if not np.isnan(p_adj).any() else np.nan,
            "p_draw": p_adj[1] if not np.isnan(p_adj).any() else np.nan,
            "p_away": p_adj[2] if not np.isnan(p_adj).any() else np.nan,
        })

    outdf = pd.DataFrame(rows)

    # garantir numérico e recalcular odds onde p_* válidas
    for col in ["p_home","p_draw","p_away"]:
        outdf[col] = pd.to_numeric(outdf[col], errors="coerce")

    def inv(arr: np.ndarray) -> np.ndarray:
        with np.errstate(divide="ignore", invalid="ignore"):
            return 1.0/np.where(arr>1e-9, arr, np.nan)

    mask = outdf[["p_home","p_draw","p_away"]].notna().all(axis=1)
    outdf.loc[mask, ["odd_home","odd_draw","odd_away"]] = inv(outdf.loc[mask, ["p_home","p_draw","p_away"]].values)

    outdf.to_csv(out, index=False)
    print(f"[referee] joined_referee.csv salvo em {out}")

if __name__ == "__main__":
    main()
