import argparse, pandas as pd
from pathlib import Path
from utils.io import ensure_cols, load_schema, save_csv

ROOT = Path(__file__).resolve().parents[1]

def main(rodada):
    schema = load_schema()
    path = ROOT/"data/raw/odds.csv"
    df = pd.read_csv(path)

    # Normalizações comuns
    if "oddway" in df.columns and "odd_away" not in df.columns:
        df = df.rename(columns={"oddway":"odd_away"})
    ensure_cols(df, schema["odds"]["required"], "odds.csv")

    # Snapshot para outputs
    out = ROOT/"outputs/odds_snapshot.csv"
    save_csv(df, out)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()
    main(args.rodada)
