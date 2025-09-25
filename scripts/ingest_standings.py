#!/usr/bin/env python3
import os, sys, argparse, yaml, requests, pandas as pd
from pathlib import Path

def cfg():
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def headers(hcfg, token_env):
    raw = os.getenv(token_env, "")
    token = (raw or "").strip().replace("\r","").replace("\n","")
    if not token:
        print(f"[ERRO] token ausente no env: {token_env}", file=sys.stderr); sys.exit(1)
    if any(ch in token for ch in (" ", "\t")):
        print("[ERRO] token contém espaço/tab. Edite o Secret para ser uma única linha, sem espaços.", file=sys.stderr); sys.exit(1)
    out = {}
    for k,v in (hcfg or {}).items():
        out[k] = v.replace("${TOKEN}", token)
    return out

def fill(template: dict, mapping: dict):
    if not template: return {}
    s = yaml.safe_dump(template)
    for k,v in mapping.items():
        s = s.replace("${"+k+"}", str(v))
    return yaml.safe_load(s)

def main(rodada):
    C = cfg()
    st = C["standings"]
    prov = C.get("provider", {})
    url = st["api_url"].replace("${base_url}", prov["base_url"])
    h = headers(st["api_headers"], st["api_token_env"])
    params = fill(st.get("api_params", {}), {**prov, "rodada": rodada})
    try:
        r = requests.get(url, headers=h, params=params, timeout=40)
        if r.status_code >= 400:
            print(f"[ERRO] standings HTTP {r.status_code}: {r.text[:500]}", file=sys.stderr)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERRO] Falha ao obter standings: {e}", file=sys.stderr); sys.exit(1)
    j = r.json()
    df = pd.json_normalize(j.get("response", j))
    out_path = C["paths"]["standings_out"].replace("${rodada}", rodada)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[OK] standings → {out_path}")

if __name__=="__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
