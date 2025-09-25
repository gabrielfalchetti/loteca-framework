#!/usr/bin/env python3
import os, sys, argparse, yaml, requests, pandas as pd
from pathlib import Path

def cfg(): return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

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

def main(rodada):
    C = cfg()
    st = C["standings"]
    prov = C.get("provider", {})
    url = st["api_url"].replace("${base_url}", prov["base_url"])
    h = headers(st["api_headers"], st["api_token_env"])
    params = fill(st.get("api_params", {}), rodada, prov)
    r = requests.get(url, headers=h, params=params, timeout=40); r.raise_for_status()
    j = r.json()
    df = pd.json_normalize(j.get("response", j))
    out_path = C["paths"]["standings_out"].replace("${rodada}", rodada)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[OK] standings → {out_path}")

if __name__=="__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
