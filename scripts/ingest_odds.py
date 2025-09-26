# scripts/ingest_odds.py
# Odds reais via TheOddsAPI. Gera data/out/<RODADA>/odds.csv
# Requer secret ODDS_API_KEY no GitHub (Settings → Secrets → Actions).
# Flags úteis no CI: --allow-partial --dump-api --print-map

from __future__ import annotations
import argparse, os, json
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np
import requests
from rapidfuzz import fuzz

BOOK_WEIGHTS_DEFAULT = {"pinnacle": 2.0, "betfair": 1.8}

def norm_team(s: str) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    s = s.lower().strip()
    for a,b in [
        (" futebol clube",""),(" futebol",""),(" clube",""),(" club",""),
        (" fc",""),(" afc",""),(" sc",""),(" ac",""),(" de futebol",""),
        ("  "," ")
    ]:
        s = s.replace(a,b)
    return s

def read_matches(rodada: str, matches_path: Optional[str]=None) -> pd.DataFrame:
    base = Path(f"data/out/{rodada}")
    mp = Path(matches_path) if matches_path else base/"matches.csv"
    if not mp.exists() or mp.stat().st_size==0:
        raise RuntimeError(f"[ingest_odds] matches.csv ausente/vazio: {mp}")
    m = pd.read_csv(mp)
    m = m.rename(columns={c:c.lower() for c in m.columns})
    maps = {"mandante":"home","visitante":"away","time_casa":"home","time_fora":"away",
            "casa":"home","fora":"away","home_team":"home","away_team":"away",
            "data_jogo":"date","data":"date","matchdate":"date","id":"match_id"}
    m = m.rename(columns={k:v for k,v in maps.items() if k in m.columns})
    if "match_id" not in m.columns:
        m = m.reset_index(drop=True); m["match_id"]=m.index+1
    for c in ("home","away"):
        if c in m.columns: m[c]=m[c].astype(str).str.strip()
    return m[["match_id","home","away"]].copy()

def fetch_api(sport: str, regions: List[str], market: str, api_key: str) -> List[dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {"apiKey": api_key, "regions": ",".join(regions), "markets": market,
              "oddsFormat": "decimal", "dateFormat": "iso"}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code!=200:
        raise RuntimeError(f"[ingest_odds] TheOddsAPI HTTP {r.status_code}: {r.text[:250]}")
    return r.json()

def parse_events(data: List[dict]) -> Dict[Tuple[str,str], Dict[str,Tuple[float,float,float]]]:
    evs: Dict[Tuple[str,str], Dict[str,Tuple[float,float,float]]] = {}
    for ev in data:
        home = norm_team(ev.get("home_team",""))
        away_counts: Dict[str,int]={}
        for bk in ev.get("bookmakers",[]):
            for mk in bk.get("markets",[]):
                if mk.get("key")!="h2h": continue
                for outc in mk.get("outcomes",[]):
                    nm = norm_team(outc.get("name",""))
                    if nm and nm!=home: away_counts[nm]=away_counts.get(nm,0)+1
        if not home or not away_counts: continue
        away = max(away_counts, key=away_counts.get)
        if home==away: continue
        key=(home,away)
        if key not in evs: evs[key]={}
        for bk in ev.get("bookmakers",[]):
            bk_key = (bk.get("key") or bk.get("title") or "").lower()
            oh=od=oa=None
            for mk in bk.get("markets",[]):
                if mk.get("key")!="h2h": continue
                for outc in mk.get("outcomes",[]):
                    nm=norm_team(outc.get("name","")); pr=outc.get("price",None)
                    try: pr=float(pr)
                    except: pr=None
                    if pr is None: continue
                    if nm==home: oh=pr
                    elif nm in ("draw","empate","x"): od=pr
                    elif nm==away: oa=pr
            if oh and od and oa: evs[key][bk_key]=(oh,od,oa)
    return evs

def devig_prop(oh: float, od: float, oa: float):
    ph,pd,pa = 1/oh,1/od,1/oa
    s=ph+pd+pa; over=s
    if s<=0: return ph,pd,pa,over
    return ph/s, pd/s, pa/s, over

def consensus(book_odds: Dict[str,Tuple[float,float,float]]):
    if not book_odds: raise ValueError("sem bookmakers")
    probs=[]; overs=[]; wts=[]
    for bk,(oh,od,oa) in book_odds.items():
        ph,pd,pa,over = devig_prop(oh,od,oa)
        probs.append((ph,pd,pa)); overs.append(over)
        wts.append(BOOK_WEIGHTS_DEFAULT.get(bk.lower(),1.0))
    probs=np.array(probs); wts=np.array(wts).reshape(-1,1)
    p=np.sum(probs*wts,axis=0)/np.sum(wts)
    return float(p[0]),float(p[1]),float(p[2]), float(np.mean(overs)), len(book_odds)

def match_with_fuzzy(matches: pd.DataFrame,
                     evs: Dict[Tuple[str,str],Dict[str,Tuple[float,float,float]]],
                     min_ratio:int):
    api_keys=list(evs.keys()); out={}
    for _,r in matches.iterrows():
        mid=r["match_id"]; h=norm_team(r["home"]); a=norm_team(r["away"])
        if (h,a) in evs: out[mid]=(h,a,100,100,100); continue
        best=None; best_avg=-1; best_s1=best_s2=0
        for (hh,aa) in api_keys:
            s1=fuzz.token_set_ratio(h,hh); s2=fuzz.token_set_ratio(a,aa); avg=(s1+s2)//2
            if avg>best_avg: best_avg=avg; best=(hh,aa); best_s1, best_s2=s1,s2
        if best and best_avg>=min_ratio: out[mid]=(best[0],best[1],best_s1,best_s2,best_avg)
    return out

def main():
    ap=argparse.ArgumentParser(description="Ingest odds reais (TheOddsAPI) -> data/out/<rodada>/odds.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--matches", default=None)
    ap.add_argument("--sport", default="soccer_brazil_campeonato")
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--market", default="h2h")
    ap.add_argument("--min-match", type=int, default=80)
    ap.add_argument("--allow-partial", action="store_true")
    ap.add_argument("--dump-api", action="store_true")
    ap.add_argument("--print-map", action="store_true")
    args=ap.parse_args()

    api_key=os.getenv("ODDS_API_KEY","").strip()
    if not api_key: raise RuntimeError("[ingest_odds] ODDS_API_KEY não definido.")

    matches=read_matches(args.rodada, args.matches)
    regions=[r.strip() for r in args.regions.split(",") if r.strip()]
    data=fetch_api(args.sport, regions, args.market, api_key)

    base=Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)
    if args.dump-api:
        (base/"odds_api_raw.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))

    evs=parse_events(data)
    if args.print-map:
        print(f"[ingest_odds] API retornou {len(evs)} pares (home,away). Exemplos:")
        for i,(k,v) in enumerate(evs.items()):
            print("  -", k, "| bookies:", ",".join(sorted(v.keys())))
            if i>=9: break

    mapping=match_with_fuzzy(matches, evs, args.min_match)

    rows=[]; missing=[]
    for _,r in matches.iterrows():
        mid=r["match_id"]
        if mid not in mapping:
            missing.append(int(mid)); continue
        hh,aa, *_ = mapping[mid]
        book_odds = evs.get((hh,aa),{})
        if not book_odds:
            missing.append(int(mid)); continue
        ph,pd,pa,over,n = consensus(book_odds)
        oh,od,oa = round(1.0/ph,4), round(1.0/pd,4), round(1.0/pa,4)
        providers = ",".join(sorted(book_odds.keys()))
        rows.append({
            "match_id":mid,
            "odd_home":oh,"odd_draw":od,"odd_away":oa,
            "n_bookmakers":n,"overround_mean":round(over,4),
            "providers":providers
        })

    if missing and not args.allow_partial:
        raise RuntimeError(f"[ingest_odds] Sem odds para match_id: {sorted(missing)} (use --allow-partial para prosseguir).")

    out=pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("[ingest_odds] Nenhuma odd coletada.")

    out_path=base/"odds.csv"
    out.to_csv(out_path, index=False)
    print(f"[ingest_odds] OK: {len(out)} linhas -> {out_path}")
    if missing:
        print(f"[ingest_odds] Aviso: {len(missing)} jogos sem odds (ids={sorted(missing)})")

if __name__=="__main__":
    main()
