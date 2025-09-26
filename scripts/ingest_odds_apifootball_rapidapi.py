# scripts/ingest_odds_apifootball_rapidapi.py
from __future__ import annotations
import argparse, os, sys
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
def find_team_id(name: str, country_hint: Optional[str]=None, debug=False) -> Tuple[int,str,str]:
    data = _get("/teams", {"search": name.strip()})
    res = data.get("response", [])
    if not res:
        raise RuntimeError(f"[apifootball] time não encontrado: {name}")
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
    if debug:
        print(f"[apifootball][map-team] '{name}' -> '{best[2]}' ({best[3]}), score={best[0]}, id={best[1]}")
    return int(best[1]), str(best[2]), str(best[3] or "")

# ---------- Localiza fixture pelo H2H e data (com tolerância ±1 dia) ----------
def find_fixture_id(date_iso: str, home_id: int, away_id: int, season_year: int, debug=False) -> Optional[int]:
    # 1) tenta H2H (lista geral de confrontos, depois filtra por data)
    h2h = f"{home_id}-{away_id}"
    data = _get("/fixtures", {"h2h": h2h, "season": season_year})
    candidates = []
    for it in data.get("response", []):
        fid = int(it["fixture"]["id"])
        dt  = (it["fixture"].get("date") or "")[:10]  # YYYY-MM-DD
        h   = it.get("teams", {}).get("home", {}).get("id")
        a   = it.get("teams", {}).get("away", {}).get("id")
        if int(h)==home_id and int(a)==away_id:
            candidates.append((fid, dt))
    # 2) escolhe por proximidade de data (exata > +-1)
    def _score(d: str) -> int:
        if d == date_iso: return 2
        # tolerância ±1 dia (sábado/domingo)
        from datetime import date, timedelta
        try:
            d0 = date.fromisoformat(date_iso)
            d1 = date.fromisoformat(d)
            if abs((d1 - d0).days) == 1: return 1
        except Exception:
            pass
        return 0
    best = None
    best_score = -1
    for fid, d in candidates:
        sc = _score(d)
        if sc > best_score:
            best = fid; best_score = sc
    if debug:
        print(f"[apifootball][find-fix] h2h={h2h} cand={len(candidates)} chosen={best} score={best_score}")
    if best: return best

    # 3) fallback: procura por data para o mandante
    data2 = _get("/fixtures", {"date": date_iso, "team": home_id, "season": season_year})
    for it in data2.get("response", []):
        h = it.get("teams", {}).get("home", {}).get("id")
        a = it.get("teams", {}).get("away", {}).get("id")
        if int(h)==home_id and int(a)==away_id:
            if debug:
                print(f"[apifootball][find-fix] fallback-date found={it['fixture']['id']}")
            return int(it["fixture"]["id"])

    return None

# ---------- Odds por fixture ----------
def fetch_odds_fixture(fixture_id: int, debug=False) -> Dict[str, Tuple[float,float,float]]:
    out: Dict[str, Tuple[float,float,float]] = {}
    data = _get("/odds", {"fixture": fixture_id})
    cnt = 0
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
                oh     = _get_price(["Home","1"])
                o_draw = _get_price(["Draw","X"])
                oa     = _get_price(["Away","2"])
                if oh and o_draw and oa:
                    out[bname] = (oh, o_draw, oa)
                    cnt += 1
    if debug:
        print(f"[apifootball][odds] fixture={fixture_id} bookies={len(out)} markets_found={cnt}")
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
    ap.add_argument("--debug", action="store_true")
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

    matches = pd.read_csv(matches_path).rename(columns=str.lower)
    for col in ("match_id","home","away"):
        if col not in matches.columns:
            raise RuntimeError(f"[apifootball] matches.csv faltando coluna: {col}")

    rows = []; missing = []
    for _, r in matches.iterrows():
        mid = int(r["match_id"])
        home_name = str(r["home"]); away_name = str(r["away"])
        try:
            hid, hname, hctry = find_team_id(home_name, country_hint=args.country_hint or None, debug=args.debug)
            aid, aname, actry = find_team_id(away_name, country_hint=args.country_hint or None, debug=args.debug)
        except Exception:
            missing.append(mid); 
            if args.debug: print(f"[apifootball][warn] team map fail mid={mid} ({home_name} vs {away_name})")
            continue

        fix_id = find_fixture_id(date_iso, hid, aid, season_year, debug=args.debug)
        if not fix_id:
            missing.append(mid);
            if args.debug: print(f"[apifootball][warn] fixture not found mid={mid} date={date_iso}")
            continue

        book = fetch_odds_fixture(fix_id, debug=args.debug)
        if not book:
            missing.append(mid);
            if args.debug: print(f"[apifootball][warn] no odds mid={mid} fixture={fix_id}")
            continue

        p_home, p_draw, p_away, over, n, prov = consensus(book)
        rows.append({
            "match_id": mid,
            "odd_home": round(1.0/max(p_home,1e-9), 4),
            "odd_draw": round(1.0/max(p_draw,1e-9), 4),
            "odd_away": round(1.0/max(p_away,1e-9), 4),
            "n_bookmakers": n,
            "overround_mean": round(over, 4),
            "providers": prov
        })

    # ---- Saída e política de erro ----
    if not rows and missing:
        # Se não achamos nada e --allow-partial, não falha: cria CSV vazio e segue
        if args.allow_partial:
            pd.DataFrame([], columns=["match_id","odd_home","odd_draw","odd_away","n_bookmakers","overround_mean","providers"]).to_csv(out_path, index=False)
            print(f"[apifootball] Nenhuma odd coletada (ok por --allow-partial). Arquivo vazio salvo em {out_path}")
            if args.debug: print(f"[apifootball] missing_ids={sorted(missing)}")
            return
        else:
            raise RuntimeError("[apifootball] Nenhuma odd coletada.")

    df_out = pd.DataFrame(rows)
    df_out.to_csv(out_path, index=False)
    print(f"[apifootball] OK: {len(df_out)} linhas -> {out_path}")
    if missing:
        print(f"[apifootball] Aviso: {len(missing)} jogos sem odds (ids={sorted(missing)})")

if __name__ == "__main__":
    main()
