import os, sys, argparse, pandas as pd, yaml
from pathlib import Path

def load_cfg():
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main(rodada: str):
    cfg = load_cfg()
    csv_path = cfg["standings"]["csv_path"].replace("${rodada}", rodada)
    out_path = cfg["paths"]["standings_out"].replace("${rodada}", rodada)
    df = pd.read_csv(csv_path)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[OK] standings salvos em {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()
    main(args.rodada)
