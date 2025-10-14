# -*- coding: utf-8 -*-
import argparse, os, sys, json, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_parquet", required=True)
    ap.add_argument("--out_parquet", required=True)
    args = ap.parse_args()

    try:
        df = pd.read_parquet(args.in_parquet)
    except Exception as e:
        print(f"[enrich] erro lendo {args.in_parquet}: {e}", file=sys.stderr)
        sys.exit(2)

    # Stub de enriquecimento: adiciona colunas reservadas (para evoluirmos)
    for col in ["api_team_id","api_fixture_cnt","h2h_form5","injuries","elo_like"]:
        if col not in df.columns:
            df[col] = None

    os.makedirs(os.path.dirname(args.out_parquet), exist_ok=True)
    df.to_parquet(args.out_parquet, index=False)
    print(f"[enrich] OK -> {args.out_parquet}")

if __name__ == "__main__":
    main()