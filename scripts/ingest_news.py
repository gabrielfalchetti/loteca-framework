import argparse, pandas as pd
from pathlib import Path
from utils.io import load_schema, ensure_cols

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT/"data/raw"

def main(rodada):
    schema = load_schema()
    path = RAW/"news.csv"
    if not path.exists():
        df = pd.DataFrame(columns=schema["news"]["required"])
        df.to_csv(path, index=False)
    df = pd.read_csv(path)
    ensure_cols(df, schema["news"]["required"], "news.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()
    main(args.rodada)
