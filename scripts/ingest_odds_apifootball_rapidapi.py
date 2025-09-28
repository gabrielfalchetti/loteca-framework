#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball_rapidapi.py — coleta odds do API-Football (RapidAPI)
- Suporta múltiplos arquivos de aliases para normalizar nomes de times
- Mapeamento de ligas do Brasil e Europa (principais)
- Match por janela temporal e fuzzy (rapidfuzz)
- Telemetria opcional em Weights & Biases (WANDB)

Saída:
  data/out/<RODADA>/odds_apifootball.csv
Colunas mínimas: match_id,home,away,k1,kx,k2,source

Uso:
  python scripts/ingest_odds_apifootball_rapidapi.py \
    --rodada 2025-09-27_1213 --season 2025 --window 14 --fuzzy 0.9 \
    --aliases data/aliases_br.json --aliases data/aliases_europa.json --debug
"""

from __future__ import annotations
import os, sys, json, math, time, argparse, unicodedata
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Tuple

import requests
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process

# =============== Config =============== #
BR_TZ = timezone(timedelta(hours=-3))
API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

# Ligas: BR + principais da Europa (API-Football league IDs)
LEAGUES = {
    # Brasil
    "BRA_SERIE_A": 71,
    "BRA_SERIE_B": 72,
    "BRA_COPA_DO_BRASIL": 73,
    "BRA_CARIOCA": 128,
    "BRA_PAULISTA": 776,
    # Europa
    "ENG_PREMIER": 39,
    "ESP_LALIGA": 140,
    "ITA_SERIE_A": 135,
    "GER_BUNDESLIGA": 78,
    "FRA_LIGUE1": 61,
    "NED_ERE": 88,
    "POR_PRIMEIRA": 94,
    "SCO_PREMIERSHIP": 179,
    "BEL_PRO": 144,
    "TUR_SUPER": 203
}

BOOKMAKER_PREFERRED = {"bet365", "pinnacle", "william hill", "10bet"}

# =============== Util =============== #
def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip()
    s = s.replace(".", " ")
    return " ".join(s.split())

def _load_aliases(paths: List[str]) -> Dict[str, List[str]]:
    ali: Dict[str, List[str]] = {}
    for p in paths:
        if not p or not os.path.isfile(p):
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                raw = json.load(f)
            for k, v in raw.items():
                canon = _norm(k)
                vals = list({_norm(x) for x in (v or [])})
                ali.setdefault(canon, [])
                for it in vals:
                    if it and it not in ali[canon]:
                        ali[canon].append(it)
        except Exception as e:
            print(f"[apifootball] AVISO: falha aliases '{p}': {e}", file=sys.stderr)
    return ali

def _canon(name: str, aliases: Dict[str, List[str]]) -> str:
    n = _norm(name)
    if n in aliases:
        return n
    for k, vs in aliases.items():
        if n == k:
            return k
        for v in vs:
            if n == v:
                return k
    return n

def _wandb_init_safe(project: str | None, job_name: str, config: dict) -> Any:
    try:
        import wandb  # type: ignore
        key = os.getenv("WANDB_API_KEY","").strip()
        if not key:
            return None
        run = wandb.init(project=project or "loteca", name=job_name, config=config, reinit=True)
        return run
    except Exception:
        return None

def _wandb_log_safe(run, data: dict):
    try:
        if run:
            run.log(data)
    except Exception:
        pass

def _headers(rapidapi_key: str) -> Dict[str, str]:
    return {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": API_HOST,
        "User-Agent": "loteca-apifoot/1.0"
    }

# =============== API wrappers =============== #
def api_get(path: str, params: Dict[str, Any], headers: Dict[str,str], retry=3, sleep=0.8) -> Dict[str,Any] | None:
    last = None
    url = f"{API_BASE}/{path}"
    for i in range(retry):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = str(e)
            time.sleep(sleep*(i+1))
    print(f"[apifootball] ERRO GET {path} -> {last}", file=sys.stderr)
    return None

def get_fixtures(league_ids: List[int], season: int, start: str, end: str, headers: Dict[str,str]) -> List[dict]:
    all_games: List[dict] = []
    for lid in league_ids:
        js = api_get("fixtures", {"league": lid, "season": season, "from": start, "to": end}, headers)
        count = len(js.get("response", [])) if isinstance(js, dict) else 0
        print(f"[apifootball] liga={lid}: fixtures={count}")
        if count:
            all_games.extend(js["response"])
    return all_games

def get_odds_by_fixture(fixture_id: int, headers: Dict[str,str]) -> List[dict]:
    js = api_get("odds", {"fixture": fixture_id}, headers)
    return js.get("response", []) if isinstance(js, dict) else []

# =============== Matching & Odds =============== #
def best_match(target: str, candidates: List[str]) -> Tuple[str, float]:
    if not candidates:
        return ("", 0.0)
    res = process.extractOne(target, candidates, scorer=fuzz.token_sort_ratio)
    if not res:
        return ("", 0.0)
    return (res[0], float(res[1])/100.0)

def extract_1x2(odds_payload: List[dict]) -> Tuple[float|None, float|None, float|None, str|None]:
    # payload schema:
    # response: [ { "bookmakers": [ { "name": "...", "bets": [ { "name": "Match Winner", "values":[ {"value":"Home","odd":"2.10"}, ... ] } ] } ] } ]
    best_src = None
    best = (None, None, None)
    best_pref = -1
    for r in odds_payload:
        for bm in r.get("bookmakers", []) or []:
            bname = str(bm.get("name","")).strip().lower()
            score_pref = 1 if bname in BOOKMAKER_PREFERRED else 0
            for bet in bm.get("bets", []) or []:
                if str(bet.get("name","")).lower() in {"match winner","1x2","fulltime result","result"}:
                    k1=kx=k2=None
                    for v in bet.get("values", []) or []:
                        val = str(v.get("value","")).strip().lower()
                        try:
                            odd = float(v.get("odd"))
                        except Exception:
                            continue
                        if val in {"home","1"}:
                            k1 = odd
                        elif val in {"draw","x"}:
                            kx = odd
                        elif val in {"away","2"}:
                            k2 = odd
                    if k1 and kx and k2:
                        # prefer preferred bookmakers; otherwise keep first complete
                        if best_pref < score_pref:
                            best = (k1,kx,k2); best_src = bm.get("name","")
                            best_pref = score_pref
    return (*best, best_src)

# =============== Main =============== #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", type=int, default=datetime.now().year)
    ap.add_argument("--window", type=int, default=14, help="dias antes/depois da data do jogo")
    ap.add_argument("--fuzzy", type=float, default=0.90, help="limiar de similaridade 0-1")
    ap.add_argument("--aliases", action="append", default=[], help="caminho de arquivo de aliases (pode repetir)")
    ap.add_argument("--scope", default="br+eu", choices=["br","eu","br+eu"])
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    out_dir = os.path.join("data","out",rodada)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "odds_apifootball.csv")

    # W&B (opcional)
    wandb_run = _wandb_init_safe(
        project=os.getenv("WANDB_PROJECT") or "loteca",
        job_name=f"apifootball_{rodada}",
        config={"season": args.season, "window": args.window, "fuzzy": args.fuzzy, "scope": args.scope}
    )

    # Aliases
    aliases = _load_aliases(args.aliases)

    # RapidAPI key
    rapid_key = os.getenv("RAPIDAPI_KEY","").strip()
    if not rapid_key:
        print("[apifootball] AVISO: RAPIDAPI_KEY ausente — abortando coleta.", file=sys.stderr)
        pd.DataFrame(columns=["match_id","home","away","k1","kx","k2","source"]).to_csv(out_path, index=False)
        return
    hdr = _headers(rapid_key)

    # Matches CSV
    src_path = os.path.join("data","in",rodada,"matches_source.csv")
    if not os.path.isfile(src_path):
        raise FileNotFoundError(f"[apifootball] arquivo não encontrado: {src_path}")
    matches = pd.read_csv(src_path)
    if "match_id" not in matches.columns:
        matches.insert(0, "match_id", range(1, len(matches)+1))

    # janela
    all_dates = []
    if "date" in matches.columns:
        for d in matches["date"]:
            try:
                all_dates.append(pd.to_datetime(str(d)).date())
            except Exception:
                pass
    if all_dates:
        dmin, dmax = min(all_dates), max(all_dates)
    else:
        today = datetime.now().date()
        dmin = today - timedelta(days=args.window//2)
        dmax = today + timedelta(days=args.window//2)
    start = (dmin - timedelta(days=args.window)).isoformat()
    end = (dmax + timedelta(days=args.window)).isoformat()
    print(f"[apifootball] Janela: {start} -> {end}; season={args.season}")

    # ligas
    if args.scope == "br":
        league_ids = [LEAGUES["BRA_SERIE_A"], LEAGUES["BRA_SERIE_B"], LEAGUES["BRA_COPA_DO_BRASIL"], LEAGUES["BRA_CARIOCA"], LEAGUES["BRA_PAULISTA"]]
    elif args.scope == "eu":
        league_ids = [LEAGUES[k] for k in ["ENG_PREMIER","ESP_LALIGA","ITA_SERIE_A","GER_BUNDESLIGA","FRA_LIGUE1","NED_ERE","POR_PRIMEIRA","SCO_PREMIERSHIP","BEL_PRO","TUR_SUPER"]]
    else:
        league_ids = [
            LEAGUES["BRA_SERIE_A"], LEAGUES["BRA_SERIE_B"], LEAGUES["BRA_COPA_DO_BRASIL"], LEAGUES["BRA_CARIOCA"], LEAGUES["BRA_PAULISTA"],
            LEAGUES["ENG_PREMIER"], LEAGUES["ESP_LALIGA"], LEAGUES["ITA_SERIE_A"], LEAGUES["GER_BUNDESLIGA"], LEAGUES["FRA_LIGUE1"],
            LEAGUES["NED_ERE"], LEAGUES["POR_PRIMEIRA"], LEAGUES["SCO_PREMIERSHIP"], LEAGUES["BEL_PRO"], LEAGUES["TUR_SUPER"]
        ]

    # Fixtures
    fixtures = get_fixtures(league_ids, args.season, start, end, hdr)
    print(f"[apifootball] Fixtures coletados: {len(fixtures)}")

    # Índices de busca
    cand_pairs = []
    for fx in fixtures:
        try:
            fid = fx["fixture"]["id"]
            h = fx["teams"]["home"]["name"]
            a = fx["teams"]["away"]["name"]
            cand_pairs.append((fid, _norm(h), _norm(a)))
        except Exception:
            continue

    # Resultado
    rows: List[dict] = []
    miss = 0

    for _, row in matches.iterrows():
        mid = row["match_id"]
        home = str(row["home"]); away = str(row["away"])
        home_n = _canon(home, aliases); away_n = _canon(away, aliases)

        # fuzzy match em duas chaves: "home vs away" e "away vs home"
        best_score = -1.0
        best_fid = None
        tgt1 = f"{home_n}||{away_n}"
        tgt2 = f"{away_n}||{home_n}"
        for fid, h, a in cand_pairs:
            s1 = fuzz.token_sort_ratio(tgt1, f"{h}||{a}")/100.0
            s2 = fuzz.token_sort_ratio(tgt2, f"{h}||{a}")/100.0
            s = max(s1,s2)
            if s > best_score:
                best_score = s
                best_fid = fid

        if best_score < args.fuzzy or best_fid is None:
            print(f"[apifootball] sem match p/ '{home}' vs '{away}' (norm: {home_n} x {away_n})")
            miss += 1
            continue

        # odds por fixture
        payload = get_odds_by_fixture(best_fid, hdr)
        k1,kx,k2,src = extract_1x2(payload)
        if k1 and kx and k2:
            rows.append({"match_id": mid, "home": home, "away": away, "k1": k1, "kx": kx, "k2": k2, "source": f"APIFootball/{src or 'unknown'}"})

    df = pd.DataFrame(rows, columns=["match_id","home","away","k1","kx","k2","source"])
    df.to_csv(out_path, index=False)
    print(f"[apifootball] OK -> {out_path} ({len(df)} linhas)")
    if miss:
        print(f"[apifootball] Aviso: {miss} jogo(s) sem match — ver nomes/ligas/janela/aliases.")

    _wandb_log_safe(wandb_run, {
        "apifootball_rows": len(df),
        "apifootball_misses": miss,
        "apifootball_fixtures": len(fixtures)
    })
    try:
        if wandb_run: wandb_run.finish()
    except Exception:
        pass

if __name__ == "__main__":
    main()
