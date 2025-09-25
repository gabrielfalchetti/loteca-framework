import argparse, pandas as pd
from pathlib import Path
from utils.io import load_schema, ensure_cols
from utils.features import build_features

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT/"data/raw"
PROC = ROOT/"data/processed"

def main(rodada):
    schema = load_schema()
    m = pd.read_csv(RAW/"matches.csv")
    ensure_cols(m, schema["matches"]["required"], "matches.csv")

    o = pd.read_csv(RAW/"odds.csv")
    if "oddway" in o.columns and "odd_away" not in o.columns:
        o = o.rename(columns={"oddway":"odd_away"})
    ensure_cols(o, schema["odds"]["required"], "odds.csv")

    t = _safe_read(RAW/"table.csv",   schema.get("table",{}).get("required",[]))
    w = _safe_read(RAW/"weather.csv", schema.get("weather",{}).get("required",[]))
    n = _safe_read(RAW/"news.csv",    schema.get("news",{}).get("required",[]))

    feats = build_features(m, o, t, w, n)
    PROC.mkdir(parents=True, exist_ok=True)
    feats.to_parquet(PROC/"features.parquet", index=False)

def _safe_read(path, req):
    p = Path(path)
    if not p.exists() or not req:
        return pd.DataFrame(columns=req)
    df = pd.read_csv(p)
    if req: ensure_cols(df, req, p.name)
    return df

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()
    main(args.rodada)
