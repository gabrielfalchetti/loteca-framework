#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_theoddsapi.py — coleta odds da TheOddsAPI
- Descobre esportes disponíveis
- Força chaves BR quando necessário
- Suporte a múltiplos aliases para normalização
- Telemetria opcional em W&B

Saídas:
  data/out/<RODADA>/odds_theoddsapi.csv
  data/out/<RODADA>/theoddsapi_debug.json
"""

from __future__ import annotations
import os, sys, json, math, time, argparse, unicodedata
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Tuple

import requests
import pandas as pd
import numpy as np

BR_TZ = timezone(timedelta(hours=-3))
BASE = "https://api.the-odds-api.com/v4"

FALLBACK_SPORTS_BR = ["soccer_brazil_campeonato", "soccer_brazil_serie_b", "soccer_brazil_cup"]

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip().replace(".", " ")
    return " ".join(s.split())

def _load_aliases(paths: list[str]) -> Dict[str, List[str]]:
    ali: Dict[str, List[str]] = {}
    for p in paths:
        if not p or not os.path.isfile(p): continue
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
            print(f"[theoddsapi] AVISO aliases '{p}': {e}", file=sys.stderr)
    return ali

def _canon(name: str, aliases: Dict[str, List[str]]) -> str:
    n = _norm(name)
    if n in aliases: return n
    for k, vs in aliases.items():
        if n == k: return k
        for v in vs:
            if n == v: return k
    return n

def _wandb_init_safe(project: str | None, job_name: str, config: dict):
    try:
        import wandb  # type: ignore
        key = os.getenv("WANDB_API_KEY","").strip()
        if not key: return None
        return wandb.init(project=project or "loteca", name=job_name, config=config, reinit=True)
    except Exception:
        return None

def _wandb_log_safe(run, data: dict):
    try:
        if run: run.log(data)
    except Exception:
        pass

def _get(path: str, params: dict, retry=3, sleep=0.8) -> Any:
    last = None
    for i in range(retry):
        try:
            r = requests.get(f"{BASE}/{path}", params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = str(e)
            time.sleep(sleep*(i+1))
    print(f"[theoddsapi] ERRO https://api.the-odds-api.com/v4/{path} -> {last}", file=sys.stderr)
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--aliases", action="append", default=[], help="arquivos de aliases (pode repetir)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    out_dir = os.path.join("data","out",rodada)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "odds_theoddsapi.csv")
    dbg_path = os.path.join(out_dir, "theoddsapi_debug.json")

    # W&B
    wandb_run = _wandb_init_safe(os.getenv("WANDB_PROJECT") or "loteca", f"theoddsapi_{rodada}",
                                 {"regions": args.regions})

    key = os.getenv("THEODDSAPI_KEY","").strip()
    if not key:
        print("[theoddsapi] Aviso: THEODDSAPI_KEY ausente — salvando CSV vazio.", file=sys.stderr)
        pd.DataFrame(columns=["match_id","home","away","k1","kx","k2","source"]).to_csv(out_path, index=False)
        return

    aliases = _load_aliases(args.aliases)

    # matches
    in_path = os.path.join("data","in",rodada,"matches_source.csv")
    if not os.path.isfile(in_path):
        raise FileNotFoundError(f"[theoddsapi] arquivo não encontrado: {in_path}")
    matches = pd.read_csv(in_path)
    if "match_id" not in matches.columns:
        matches.insert(0, "match_id", range(1, len(matches)+1))
    matches["home_n"] = matches["home"].apply(lambda x: _canon(x, aliases))
    matches["away_n"] = matches["away"].apply(lambda x: _canon(x, aliases))

    # listar esportes
    sports = _get("sports", {"apiKey": key})
    all_ids = []
    if isinstance(sports, list):
        all_ids = [s.get("key") for s in sports if s.get("active")]
    print(f"[theoddsapi] total sports na conta: {len(all_ids)}")

    # checar BR
    detected = [sid for sid in all_ids if isinstance(sid, str) and sid.startswith("soccer_brazil_")]
    if detected:
        print(f"[theoddsapi] sports detectados (BR): {detected}")
        sports_keys = detected
    else:
        print("[theoddsapi] AVISO: não detectou sports BR ativos — usando fallback.")
        sports_keys = FALLBACK_SPORTS_BR

    diag = []
    rows: List[dict] = []

    for skey in sports_keys:
        # listar eventos por sport
        ev = _get(f"sports/{skey}/events", {"apiKey": key})
        if not isinstance(ev, list):
            print(f"[theoddsapi] Aviso: nenhuma odd coletada para este sport.", file=sys.stderr)
            diag.append({"sport": skey, "events": 0, "odds_events": 0, "skipped_404": True})
            continue
        print(f"[theoddsapi] {skey}: eventos listados={len(ev)}")
        odds_ok = 0

        for e in ev:
            home = _norm((e.get("home_team") or ""))
            away = _norm((e.get("away_team") or ""))
            # odds/mercados
            odds = _get("odds", {"apiKey": key, "regions": args.regions, "markets": "h2h", "eventIds": e.get("id")})
            if not isinstance(odds, list) or not odds:
                continue
            # extrair 1x2 de primeiro bookmaker com h2h
            k1=kx=k2=None
            src=None
            for book in odds[0].get("bookmakers", []) or []:
                for mkt in book.get("markets", []) or []:
                    if (mkt.get("key") or "").lower() == "h2h":
                        for outc in mkt.get("outcomes", []) or []:
                            name = str(outc.get("name","")).strip().lower()
                            try:
                                price = float(outc.get("price"))
                            except Exception:
                                price = None
                            if name == "home": k1 = price
                            elif name == "draw": kx = price
                            elif name == "away": k2 = price
                        src = book.get("title","")
                        break
                if k1 and kx and k2:
                    break
            if not (k1 and kx and k2):
                continue

            # tentar casar com matches
            hit = matches[(matches["home_n"] == home) & (matches["away_n"] == away)]
            if len(hit) == 1:
                mid = int(hit.iloc[0]["match_id"])
                rows.append({"match_id": mid, "home": hit.iloc[0]["home"], "away": hit.iloc[0]["away"],
                             "k1": k1, "kx": kx, "k2": k2, "source": f"TheOddsAPI/{src or 'unknown'}"})
                odds_ok += 1

        diag.append({"sport": skey, "events": len(ev), "odds_events": odds_ok, "skipped_404": False})

    df = pd.DataFrame(rows, columns=["match_id","home","away","k1","kx","k2","source"])
    df.to_csv(out_path, index=False)
    print(f"[theoddsapi] OK -> {out_path} ({len(df)} linhas)")

    dbg = {
        "when": datetime.now(BR_TZ).isoformat(timespec="seconds"),
        "regions": args.regions,
        "sports": sports_keys,
        "diag": diag
    }
    with open(dbg_path, "w", encoding="utf-8") as f:
        json.dump(dbg, f, ensure_ascii=False, indent=2)
    print(f"[theoddsapi] debug salvo em: {dbg_path}")
    if args.debug:
        print("---- theoddsapi_debug.json (head) ----")
        try:
            with open(dbg_path,"r",encoding="utf-8") as f:
                print(f.read()[:4000])
        except Exception:
            pass

    try:
        run = _wandb_init_safe(os.getenv("WANDB_PROJECT") or "loteca", f"theoddsapi_post_{rodada}", {})
        _wandb_log_safe(run, {"theoddsapi_rows": len(df), "sports_consultados": len(sports_keys)})
        if run: run.finish()
    except Exception:
        pass

if __name__ == "__main__":
    main()
