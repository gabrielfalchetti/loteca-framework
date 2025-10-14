# -*- coding: utf-8 -*-
import argparse, csv, os, pandas as pd
from _utils_norm import norm_name

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.in_csv)
    assert {"match_id","home","away"}.issubset(df.columns), "CSV precisa match_id,home,away"

    df["home_norm"] = df["home"].astype(str).map(norm_name)
    df["away_norm"] = df["away"].astype(str).map(norm_name)

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    df.to_csv(args.out_csv, index=False, encoding="utf-8")
    print(f"[normalize] OK -> {args.out_csv} (linhas={len(df)})")

if __name__ == "__main__":
    main()