# scripts/ingest_odds_apifootball_rapidapi.py
# RapidAPI (API-Football) -> odds_apifootball.csv
# - encontra fixtures via H2H e varredura por data (± window de dias)
# - casa nomes com fuzzy + aliases opcionais
# - coleta odds 1X2 e gera consenso
from __future__ import annotations
import argparse, os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import date, timedelta

import requests
import pandas as pd
import numpy as np
from rapidfuzz import fuzz

try:
    import yaml  # aliases opcionais
except Exception:
    yaml = None

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

# -------- HTTP ----------
def headers():
    key = os.getenv("RAPIDAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("[apifootball] RAPIDAPI_KEY não definido.")
    return {"X-RapidAPI-Key": key, "X-RapidAPI-Host": API_HOST}

def api_get(path: str, params: dict) -> dict:
    r = requests.get(f"{API_BASE}{path}", headers=headers(), params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"[apifootball] GET {path} HTTP {r.status_code}: {r.text[:300]}")
    j = r.json()
    if isinstance(j, dict) and j.get("errors"):
        raise RuntimeError(f"[apifootball] API error: {j.get('errors')}")
    return j

# -------- util ----------
def norm_team(s: str) -> str:
    s = (s or "").lower().strip()
    rep = [(" futebol clube",""),(" futebol",""),(" clube",""),(" club",""),
           (" fc",""),(" afc",""),(" sc",""),(" ac",""),(" de futebol",""),
           ("-sp",""),("-rj",""),("-mg",""),("-rs",""),("-pr",""),
           ("/sp",""),("/rj",""),("/mg",""),("/rs",""),("/pr",""),
           ("  "," ")]
    for a,b in rep: s = s.replace(a,b)
    return s

def load_aliases() -> Dict[str, str]:
    """Carrega aliases opcionais (mapa 'apelido'->'nome_oficial API-Football')."""
    cfg = Path("config/team_aliases.yaml")
    if yaml and cfg.exists() and cfg.stat().st_size > 0:
        try:
            data = yaml.safe_load(cfg.read_text()) or {}
            return {str(k).lower(): str(v) for k,v in data.items()}
        except Exception:
            pass
    return {}

def alias(name: str, aliases: Dict[str,str]) -> str:
    return aliases.get(name.lower(), name)

# -------- teams ----------
def find_team_id(name: str, country_hint: Optional[str]=None, debug=False) -> Tuple[int,str,str]:
    data = api_get("/teams", {"search": name.strip()})
    res = data.get("response", [])
    if not res:
        raise RuntimeError(f"[apifootball] time não encontrado: {name}")
    best=(0,None,"","")
    for it in res:
        tname = it["team"]["name"]
        country = (it.get("team",{}) or {}).get("country") or it.get("country") or ""
        score = fuzz.token_set_ratio(norm_team(name), norm_team(tname))
        if country_hint and country and country_hint.lower() in str(country).lower():
            score += 3
        if score > best[0]:
            best=(score, it["team"]["id"], tname, country)
    if debug:
        print(f"[apifootball][team] '{name}' -> '{best[2]}' ({best[3]}), score={best[0]}, id={best[1]}")
    return int(best[1]), str(best[2]), str(best[3])

# -------- fixtures ----------
def fixtures_by_date(d: str) -> List[dict]:
    # lista TUDO do dia (global) — mais custoso, porém robusto para casar por nome
    return api_get("/fixtures", {"date": d}).get("response", [])

def find_fixture_id(date_iso: str, home: str, away: str, season_year: int,
                    country_hint: Optional[str], days_window: int, min_match: int,
                    debug=False) -> Optional[int]:
    # 1) tenta H2H por IDs (mais barato)
    try:
        hid, _, _ = find_team_id(home, country_hint, debug)
        aid, _, _ = find_team_id(away, country_hint, debug)
        h2h = f"{hid}-{aid}"
        h2h_resp = api_get("/fixtures", {"h2h": h2h, "season": season_year}).get("response", [])
        # escolhe o fixture mais próximo da data (exata > ±1 > ±2)
        def sc(d: str) -> int:
            try:
                d0 = date.fromisoformat(date_iso); d1 = date.fromisoformat(d[:10])
                diff = abs((d1-d0).days)
                return 10 - diff  # maior é melhor
            except Exception:
                return -999
        best=None; best_s=-999
        for it in h2h_resp:
            d = (it["fixture"].get("date") or "")[:10]
            s = sc(d)
            if s > best_s:
                best = it; best_s = s
        if best and best_s >= 8:  # data exata/±1/±2
            return int(best["fixture"]["id"])
    except Exception:
        pass

    # 2) varre por data ± window e casa por NOME (fuzzy) — robusto
    d0 = date.fromisoformat(date_iso)
    best_id=None; best_score=-1
    for off in range(-days_window, days_window+1):
        dstr = (d0 + timedelta(days=off)).isoformat()
        resp = fixtures_by_date(dstr)
        for it in resp:
            hname = (it.get("teams",{}).get("home",{}) or {}).get("name","")
            aname = (it.get("teams",{}).get("away",{}) or {}).get("name","")
            s1 = fuzz.token_set_ratio(norm_team(home), norm_team(hname))
            s2 = fuzz.token_set_ratio(norm_team(away), norm_team(aname))
            score = (s1 + s2)//2
            if score > best_score:
                best_score = score; best_id = int(it["fixture"]["id"])
        if debug:
            print(f"[apifootball][scan] {dstr}: {len(resp)} fixtures, best_score={best_score}")
    if best_score >= min_match:
        return best_id
    return None

# -------- odds ----------
def fetch_odds_fixture(fid: int, debug=False) -> Dict[str, Tuple[float,float,float]]:
    out: Dict[str, Tuple[float,float,float]] = {}
    data = api_get("/odds", {"fixture": fid})
    cnt=0
    for it in data.get("response", []):
        for b in it.get("bookmakers", []):
            bname = (b.get("name") or b.get("id") or "").lower()
            for mv in b.get("bets", []):
                labels = { (v.get("value") or "").strip(): v.get("odd") for v in mv.get("values", []) }
                def price(keys):
                    for k in keys:
                        if k in labels:
                            try: return float(labels[k])
                            except: pass
                    return None
                oh = price(["Home","1"]); od = price(["Draw","X"]); oa = price(["Away","2"])
                if oh and od and oa:
                    out[bname]=(oh,od,oa); cnt+=1
    if debug:
        print(f"[apifootball][odds] fixture={fid} bookies={len(out)} markets_found={cnt}")
    return out

def devig(oh: float, od: float, oa: float) -> Tuple[float,float,float,float]:
    ph,pd,pa = 1/oh, 1/od, 1/oa
    s = ph+pd+pa
    if s<=0: return ph,pd,pa,s
    return ph/s, pd/s, pa/s, s

def consensus(book_odds: Dict[str,Tuple[float,float,float]]):
    probs=[]; overs=[]
    for _,(oh,od,oa) in book_odds.items():
        ph,pd,pa,over = devig(oh,od,oa)
        probs.append((ph,pd,pa)); overs.append(over)
    p = np.mean(np.array(probs), axis=0)
    providers = ",".join(sorted(book_odds.keys()))
    return float(p[0]), float(p[1]), float(p[2]), float(np.mean(overs)), len(book_odds), providers

# -------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Odds via API-Football (RapidAPI) -> odds_apifootball.csv")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-10-05_14")
    ap.add_argument("--allow-partial", action="store_true")
    ap.add_argument("--country-hint", default="", help="Brazil/England/Spain etc.")
    ap.add_argument("--days-window", type=int, default=2, help="Janela de dias para varrer fixtures (±N)")
    ap.add_argument("--min-match", type=int, default=85, help="Limite de match fuzzy para aceitar fixture")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    # extrai data/ano da rodada
    try:
        date_iso = args.rodada.split("_", 1)[0]
        season_year = int(date_iso.split("-")[0])
    except Exception:
        raise RuntimeError(f"[apifootball] Rodada inválida: {args.rodada}")

    base = Path(f"data/out/{args.rodada}")
    matches_path = base/"matches.csv"
    out_path = base/"odds_apifootball.csv"
    base.mkdir(parents=True, exist_ok=True)

    if not matches_path.exists() or matches_path.stat().st_size == 0:
        raise RuntimeError(f"[apifootball] matches.csv ausente/vazio: {matches_path}")

    aliases = load_aliases()

    dfm = pd.read_csv(matches_path).rename(columns=str.lower)
    for col in ("match_id","home","away"):
        if col not in dfm.columns:
            raise RuntimeError(f"[apifootball] matches.csv faltando coluna: {col}")

    rows=[]; missing=[]
    for _, r in dfm.iterrows():
        mid = int(r["match_id"])
        home = alias(str(r["home"]), aliases)
        away = alias(str(r["away"]), aliases)

        fid = find_fixture_id(date_iso, home, away, season_year,
                              country_hint=(args.country_hint or None),
                              days_window=args.days_window, min_match=args.min_match,
                              debug=args.debug)
        if not fid:
            missing.append(mid)
            if args.debug: print(f"[apifootball][miss] fixture {home} vs {away} não encontrado (mid={mid})")
            continue

        book = fetch_odds_fixture(fid, debug=args.debug)
        if not book:
            missing.append(mid)
            if args.debug: print(f"[apifootball][miss] sem odds fixture={fid} (mid={mid})")
            continue

        ph,pd,pa,over,n,prov = consensus(book)
        rows.append({
            "match_id": mid,
            "odd_home": round(1.0/max(ph,1e-9), 4),
            "odd_draw": round(1.0/max(pd,1e-9), 4),
            "odd_away": round(1.0/max(pa,1e-9), 4),
            "n_bookmakers": n,
            "overround_mean": round(over, 4),
            "providers": prov
        })

    # política de saída
    if not rows:
        if args.allow_partial:
            pd.DataFrame([], columns=["match_id","odd_home","odd_draw","odd_away","n_bookmakers","overround_mean","providers"]).to_csv(out_path, index=False)
            print(f"[apifootball] Nenhuma odd coletada (ok por --allow-partial). Arquivo vazio salvo em {out_path}")
            if missing: print(f"[apifootball] missing_ids={sorted(missing)}")
            return
        raise RuntimeError("[apifootball] Nenhuma odd coletada.")

    out = pd.DataFrame(rows).sort_values("match_id")
    out.to_csv(out_path, index=False)
    print(f"[apifootball] OK: {len(out)} linhas -> {out_path}")
    if missing:
        print(f"[apifootball] Aviso: {len(missing)} jogos sem odds (ids={sorted(missing)})")

if __name__ == "__main__":
    main()
