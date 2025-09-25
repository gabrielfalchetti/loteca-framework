#!/usr/bin/env python3
import os, sys, argparse, yaml, requests, pandas as pd
from pathlib import Path

def cfg():
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def headers(hcfg, token_env):
    raw = os.getenv(token_env, "")
    tok = (raw or "").strip().replace("\r","").replace("\n","")
    if not tok:
        print(f"[ERRO] token ausente no env: {token_env}", file=sys.stderr); sys.exit(1)
    if any(c in tok for c in (" ", "\t")):
        print("[ERRO] token contém espaço/tab. Edite o Secret p/ 1 linha.", file=sys.stderr); sys.exit(1)
    return {k: v.replace("${TOKEN}", tok) for k,v in (hcfg or {}).items()}

def main(rodada):
    C = cfg()
    st = C["standings"]
    prov = C.get("provider", {})
    url = st["api_url"].replace("${base_url}", prov["base_url"])
    h = headers(st["api_headers"], st["api_token_env"])
    params = {"league": prov.get("league_br"), "season": prov.get("season")}
    r = requests.get(url, headers=h, params=params, timeout=40)
    if r.status_code >= 400:
        print(f"[ERRO] standings HTTP {r.status_code}: {r.text[:500]}", file=sys.stderr)
    r.raise_for_status()
    j = r.json()
    df = pd.json_normalize(j.get("response", j))
    out_path = C["paths"]["standings_out"].replace("${rodada}", rodada)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[OK] standings → {out_path}")

if __name__=="__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
