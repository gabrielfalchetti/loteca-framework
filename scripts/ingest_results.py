# scripts/ingest_results.py
# Coleta resultados finais (placar e 1X2) via API-Football (RapidAPI) e salva data/out/<rodada>/results.csv
from __future__ import annotations
import argparse, os, time, json
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

def api_get(path: str, params: dict, key: str, retries: int = 3, timeout=30) -> dict:
    hdr = {"x-rapidapi-key": key, "x-rapidapi-host": API_HOST}
    last = None
    for i in range(retries):
        r = requests.get(API_BASE + path, headers=hdr, params=params, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        last = (r.status_code, r.text[:300])
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2*(i+1))
    raise RuntimeError(f"[results] GET {path} falhou: HTTP {last[0]} {last[1]}")

def _norm(s: str) -> str:
    if not isinstance(s,str): return ""
    s = s.lower().strip()
    for a,b in [("ã","a"),("õ","o"),("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ç","c"),("/"," "),("-"," ")]:
        s = s.replace(a,b)
    return " ".join(s.split())

def _winner_to_1x2(home_goals: int, away_goals: int) -> str:
    if home_goals > away_goals: return "1"
    if home_goals < away_goals: return "2"
    return "X"

def find_fixture_id(date_iso: str, home: str, away: str, key: str, days_window: int = 2) -> Optional[int]:
    # busca por data +/- days_window (lista por dia)
    from datetime import datetime, timedelta
    dt = datetime.fromisoformat(date_iso.replace("Z","").replace("+00:00",""))
    cand = []
    for off in range(-days_window, days_window+1):
        d = (dt + timedelta(days=off)).date().isoformat()
        js = api_get("/fixtures", {"date": d}, key)
        for it in js.get("response", []):
            t1 = _norm(it["teams"]["home"]["name"])
            t2 = _norm(it["teams"]["away"]["name"])
            score = fuzz.token_sort_ratio(_norm(home), t1) + fuzz.token_sort_ratio(_norm(away), t2)
            cand.append((score, it))
    if not cand: return None
    cand.sort(key=lambda x: x[0], reverse=True)
    best = cand[0][1]
    return best["fixture"]["id"]

def main():
    ap = argparse.ArgumentParser(description="Ingestão de resultados finais via API-Football (RapidAPI)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--days-window", type=int, default=2, help="Janela ±dias para casar fixture por data")
    ap.add_argument("--min-match", type=int, default=85, help="Similaridade mínima (RapidFuzz 0-100) para aceitar pareamento")
    args = ap.parse_args()

    key = os.environ.get("RAPIDAPI_KEY","").strip()
    if not key:
        raise RuntimeError("[results] RAPIDAPI_KEY não definido nos Secrets/Env.")

    base = Path(f"data/out/{args.rodada}")
    matches_path = base/"matches.csv"
    if not matches_path.exists() or matches_path.stat().st_size == 0:
        raise RuntimeError(f"[results] matches.csv ausente/vazio: {matches_path}")

    dfm = pd.read_csv(matches_path).rename(columns=str.lower)
    if not {"match_id","home","away"}.issubset(dfm.columns):
        raise RuntimeError("[results] matches.csv inválido; precisa de match_id,home,away[,date].")

    # Garantir uma data aproximada: tenta fixtures_merged.csv
    dates = {}
    fmerge = base/"fixtures_merged.csv"
    if fmerge.exists() and fmerge.stat().st_size>0:
        dfx = pd.read_csv(fmerge).rename(columns=str.lower)
        if {"match_id","date"}.issubset(dfx.columns):
            dates = dict(zip(dfx["match_id"], dfx["date"]))
    elif "date" in dfm.columns:
        dates = dict(zip(dfm["match_id"], dfm["date"]))

    rows = []
    unresolved = []
    for _, r in dfm.iterrows():
        mid  = r["match_id"]
        home = r["home"]
        away = r["away"]
        date_iso = dates.get(mid)
        fid = None
        js = None
        if date_iso:
            try:
                fid = find_fixture_id(date_iso, home, away, key, days_window=args.days_window)
            except Exception as e:
                unresolved.append(int(mid)); continue
        if fid is None:
            # fallback: busca por H2H entre os times (últimos jogos) e escolhe o mais recente finalizado
            js = api_get("/fixtures/headtohead", {"h2h": f"{home}-{away}"}, key)
            resp = js.get("response", [])
            resp = [x for x in resp if x.get("fixture",{}).get("status",{}).get("short","") in ("FT","AET","PEN")]
            if not resp:
                unresolved.append(int(mid)); continue
            resp.sort(key=lambda x: x["fixture"]["date"], reverse=True)
            fid = resp[0]["fixture"]["id"]

        # pega detalhe final
        jd = api_get("/fixtures", {"id": fid}, key)
        r0 = jd.get("response", [])
        if not r0:
            unresolved.append(int(mid)); continue
        it = r0[0]
        st = it["fixture"]["status"]["short"]
        if st not in ("FT","AET","PEN"):
            # ainda não terminou
            continue
        h = it["goals"]["home"]; a = it["goals"]["away"]
        pick = _winner_to_1x2(int(h), int(a))
        rows.append({
            "match_id": int(mid),
            "home": home,
            "away": away,
            "home_goals": int(h),
            "away_goals": int(a),
            "resultado": pick,
            "status": st,
            "fixture_id": fid
        })

    out = pd.DataFrame(rows).sort_values("match_id")
    out.to_csv(base/"results.csv", index=False)
    print(f"[results] OK -> {base/'results.csv'}")

    if unresolved:
        print(f"[results] Aviso: não foi possível resolver match_id: {sorted(set(unresolved))} (pode ser jogo futuro ou data inconsistente)")

if __name__ == "__main__":
    main()
