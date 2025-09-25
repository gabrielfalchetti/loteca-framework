#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, argparse, yaml, requests, pandas as pd
from pathlib import Path
from time import sleep

def cfg(): return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def clean_token(name):
    raw = os.getenv(name, ""); tok = (raw or "").strip().replace("\r","").replace("\n","")
    if not tok: print(f"[ERRO] token ausente no env: {name}", file=sys.stderr); sys.exit(1)
    if any(c in tok for c in (" ", "\t")): print("[ERRO] token com espaço/tab.", file=sys.stderr); sys.exit(1)
    return tok

def headers(hcfg, token_env):
    tok = clean_token(token_env)
    return {k: v.replace("${TOKEN}", tok) for k,v in (hcfg or {}).items()}

def GET(url, headers, params, timeout=30, retries=2, backoff=1.5):
    for i in range(retries+1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code >= 400: print(f"[WARN] HTTP {r.status_code} {url} :: {r.text[:200]}", file=sys.stderr)
            r.raise_for_status(); return r
        except requests.RequestException:
            if i==retries: raise
            sleep(backoff**i)

def role_bucket(rolename: str) -> str:
    if not isinstance(rolename, str): return "other"
    s = rolename.lower()
    if "goalkeeper" in s or "gk" in s: return "keeper"
    if "defender" in s or "df" in s:   return "defender"
    if "midfielder" in s or "mf" in s: return "mid"
    if "attacker" in s or "fw" in s or "forward" in s: return "forward"
    return "other"

def main(rodada: str):
    C = cfg(); prov = C.get("provider", {}); avail = C["availability"]
    matches_path = C["paths"]["matches_csv"].replace("${rodada}", rodada)
    out_path = C["paths"]["availability_out"].replace("${rodada}", rodada)
    if not Path(matches_path).exists(): raise SystemExit(f"[ERRO] matches.csv não encontrado: {matches_path}")
    dfm = pd.read_csv(matches_path); 
    if dfm.empty: raise SystemExit("[ERRO] matches.csv vazio.")
    h = headers(avail["api_headers"], avail["api_token_env"])

    rows=[]
    for rec in dfm.to_dict("records"):
        mid = rec.get("match_id")
        if pd.isna(mid): continue
        # lineups
        try:
            url_lineups = avail["endpoints"]["lineups"].replace("${base_url}", prov["base_url"])
            rl = GET(url_lineups, h, {"fixture": mid})
            lineups = pd.json_normalize(rl.json().get("response", []))
        except Exception as e:
            print(f"[WARN] lineups falhou p/ match_id={mid}: {e}", file=sys.stderr); lineups = pd.DataFrame()
        starters_missing = 0; bench_depth = 0
        if not lineups.empty:
            # heurística simples
            start_lists = lineups.get("startXI", [])
            eleven = 0
            for item in start_lists:
                if isinstance(item, list): eleven = max(eleven, len(item))
                elif isinstance(item, dict) and isinstance(item.get("player"), dict): eleven += 1
            if eleven and eleven < 11: starters_missing = 11 - eleven
            subs_lists = lineups.get("substitutes", [])
            subs = 0
            for item in subs_lists:
                if isinstance(item, list): subs = max(subs, len(item))
                elif isinstance(item, dict) and isinstance(item.get("player"), dict): subs += 1
            bench_depth = subs

        # injuries
        try:
            url_inj = avail["endpoints"]["injuries"].replace("${base_url}", prov["base_url"])
            ri = GET(url_inj, h, {"fixture": mid})
            inj = pd.json_normalize(ri.json().get("response", []))
        except Exception as e:
            print(f"[WARN] injuries falhou p/ match_id={mid}: {e}", file=sys.stderr); inj = pd.DataFrame()
        keeper_out=def_out=mid_out=fwd_out=0
        if not inj.empty and "player.type" in inj.columns:
            buckets = inj["player.type"].map(role_bucket).value_counts().to_dict()
            keeper_out = int(buckets.get("keeper",0))
            def_out    = int(buckets.get("defender",0))
            mid_out    = int(buckets.get("mid",0))
            fwd_out    = int(buckets.get("forward",0))

        rows.append({"match_id": mid, "starters_missing": starters_missing, "bench_depth": bench_depth,
                     "keeper_out": keeper_out, "defenders_out": def_out, "mids_out": mid_out, "forwards_out": fwd_out})

    out = pd.DataFrame(rows)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"[OK] availability → {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
