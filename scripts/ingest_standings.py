#!/usr/bin/env python3
import os, argparse, pandas as pd, yaml
from pathlib import Path

def load_cfg():
    with open("config/config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def main(rodada: str):
    cfg = load_cfg()
    mode = cfg["standings"]["mode"]
    out_path = cfg["paths"]["standings_out"].replace("${rodada}", rodada)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    if mode == "csv":
        csv_path = cfg["standings"]["csv_path"].replace("${rodada}", rodada)
        df = pd.read_csv(csv_path)
        df.to_csv(out_path, index=False)
        print(f"[OK] standings (CSV) → {out_path}")
    elif mode == "api":
        import requests
        url = cfg["standings"]["api_url"]
        token = os.getenv(cfg["standings"]["api_token_env"], "")
        if not token:
            raise SystemExit("Token da API ausente (configure em Settings > Secrets).")
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"round": rodada}, timeout=30)
        r.raise_for_status()
        data = r.json()
        df = pd.json_normalize(data["standings"])
        df.to_csv(out_path, index=False)
        print(f"[OK] standings (API) → {out_path}")
    else:
        raise SystemExit("Config standings.mode deve ser 'csv' ou 'api'.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
