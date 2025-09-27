# scripts/ingest_lineups_injuries_apifootball.py
# Injuries + Lineups via API-Football (RapidAPI) -> context_injuries_lineups.csv por rodada
from __future__ import annotations
import argparse, os, time
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests
import pandas as pd
import numpy as np
from rapidfuzz import fuzz

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

def api_get(path: str, params: dict, key: str, retries: int = 3, timeout=30) -> dict:
    hdr = {"x-rapidapi-key": key, "x-rapidapi-host": API_HOST}
    last = None
    for i in range(retries):
        r = requests.get(API_BASE + path, headers=hdr, params=params, timeout=timeout)
        if r.status_code == 200: return r.json()
        last = (r.status_code, r.text[:300])
        if r.status_code in (429,500,502,503,504): time.sleep(2*(i+1))
    raise RuntimeError(f"[ctx] GET {path} falhou: HTTP {last[0]} {last[1]}")

def _norm(s: str) -> str:
    if not isinstance(s,str): return ""
    s=s.lower().strip()
    for a,b in [("ã","a"),("õ","o"),("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ç","c"),("/"," "),("-"," ")]:
        s=s.replace(a,b)
    return " ".join(s.split())

def find_fixture_id_by_date(date_iso: str, home: str, away: str, key: str, days_window=2) -> Optional[int]:
    from datetime import datetime, timedelta
    dt = datetime.fromisoformat(date_iso.replace("Z","").replace("+00:00",""))
    cand=[]
    for off in range(-days_window, days_window+1):
        d=(dt+timedelta(days=off)).date().isoformat()
        js = api_get("/fixtures", {"date": d}, key)
        for it in js.get("response", []):
            t1=_norm(it["teams"]["home"]["name"]); t2=_norm(it["teams"]["away"]["name"])
            score = fuzz.token_sort_ratio(_norm(home), t1) + fuzz.token_sort_ratio(_norm(away), t2)
            cand.append((score,it))
    if not cand: return None
    cand.sort(key=lambda x: x[0], reverse=True)
    return cand[0][1]["fixture"]["id"]

def main():
    ap = argparse.ArgumentParser(description="Ingestão de lineups e lesões via API-Football (RapidAPI)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--days-window", type=int, default=2)
    ap.add_argument("--min-match", type=int, default=85)
    args = ap.parse_args()

    key = os.environ.get("RAPIDAPI_KEY","").strip()
    if not key: raise RuntimeError("[ctx] RAPIDAPI_KEY não definido.")

    base = Path(f"data/out/{args.rodada}"); base.mkdir(parents=True, exist_ok=True)
    matches_path = base/"matches.csv"
    if not matches_path.exists() or matches_path.stat().st_size==0:
        raise RuntimeError(f"[ctx] matches.csv ausente/vazio: {matches_path}")

    dfm = pd.read_csv(matches_path).rename(columns=str.lower)
    if not {"match_id","home","away"}.issubset(dfm.columns):
        raise RuntimeError("[ctx] matches.csv inválido; precisa de match_id,home,away[,date].")

    # tenta usar fixtures_merged para ter data
    dates={}
    fmerge = base/"fixtures_merged.csv"
    if fmerge.exists() and fmerge.stat().st_size>0:
        dfx=pd.read_csv(fmerge).rename(columns=str.lower)
        if {"match_id","date"}.issubset(dfx.columns):
            dates=dict(zip(dfx["match_id"], dfx["date"]))
    elif "date" in dfm.columns:
        dates=dict(zip(dfm["match_id"], dfm["date"]))

    out_rows=[]
    for _,r in dfm.iterrows():
        mid=int(r["match_id"]); home=str(r["home"]); away=str(r["away"])
        date_iso = dates.get(mid)
        fid=None
        if date_iso:
            try:
                fid = find_fixture_id_by_date(date_iso, home, away, key, days_window=args.days_window)
            except Exception:
                fid=None

        # Injuries (pode não encontrar, seguimos em frente)
        inj_home=inj_away=0
        try:
            if fid:
                j = api_get("/injuries", {"fixture": fid}, key)
            else:
                j = {"response":[]}
            # Se fixture não resolver, tenta por time-nome (pobre, mas evita 0 crônico)
            for it in j.get("response", []):
                tname = it.get("team",{}).get("name","")
                players = it.get("players",[])
                if fuzz.partial_ratio(_norm(tname), _norm(home)) >= args.min_match:
                    inj_home += len(players)
                elif fuzz.partial_ratio(_norm(tname), _norm(away)) >= args.min_match:
                    inj_away += len(players)
        except Exception:
            pass

        # Lineups (prováveis/confirmadas)
        lineup_home=lineup_away=0
        try:
            if fid:
                lj = api_get("/fixtures/lineups", {"fixture": fid}, key)
                for it in lj.get("response", []):
                    tname = it.get("team",{}).get("name","")
                    start = it.get("startXI",[]) or []
                    if fuzz.partial_ratio(_norm(tname), _norm(home)) >= args.min_match:
                        lineup_home = len(start)
                    elif fuzz.partial_ratio(_norm(tname), _norm(away)) >= args.min_match:
                        lineup_away = len(start)
        except Exception:
            pass

        out_rows.append({
            "match_id": mid,
            "home": home, "away": away,
            "fixture_id": fid,
            "injuries_home": inj_home, "injuries_away": inj_away,
            "lineup_starters_home": lineup_home, "lineup_starters_away": lineup_away
        })

    out = pd.DataFrame(out_rows).sort_values("match_id")
    out.to_csv(base/"context_injuries_lineups.csv", index=False)
    print(f"[ctx] OK -> {base/'context_injuries_lineups.csv'}")

if __name__ == "__main__":
    main()
