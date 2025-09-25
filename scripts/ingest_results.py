#!/usr/bin/env python3
import os, sys, argparse, yaml, requests, pandas as pd
from pathlib import Path

def cfg():
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def headers(hcfg, token_env):
    token = os.getenv(token_env, "")
    if not token:
        print(f"[ERRO] token ausente no env: {token_env}", file=sys.stderr); sys.exit(1)
    out = {}
    for k,v in (hcfg or {}).items():
        out[k] = v.replace("${TOKEN}", token)
    return out

def fill(template: dict, rodada: str, prov: dict):
    if not template: return {}
    s = yaml.safe_dump(template)
    s = s.replace("${rodada}", rodada)
    for k,v in prov.items():
        s = s.replace("${"+k+"}", str(v))
    return yaml.safe_load(s)

def main(rodada: str):
    C = cfg()
    prov = C.get("provider", {})
    rcfg = C["results"]
    h = headers(rcfg["api_headers"], rcfg["api_token_env"])
    params = fill(rcfg["params"], rodada, prov)
    url = rcfg["api_url"].replace("${base_url}", prov["base_url"])
    r = requests.get(url, headers=h, params=params, timeout=40); r.raise_for_status()
    j = r.json()
    df = pd.json_normalize(j.get("response", j))
    # normalizar um resultado H/D/A simples
    # API-Football tipicamente tem: fixture.status.short, goals.home, goals.away
    if not df.empty:
        def to_res(row):
            gh = row.get("goals.home"); ga = row.get("goals.away")
            if gh is None or ga is None: return None
            if gh > ga: return "H"
            if gh < ga: return "A"
            return "D"
        df["result"] = df.apply(to_res, axis=1)
    out_path = C["paths"]["results_out"].replace("${rodada}", rodada)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[OK] results → {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
