# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
from unidecode import unidecode

def normalize_team_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = unidecode(name).lower().strip()
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo")
    return name.capitalize()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    args = ap.parse_args()

    if not os.path.isfile(args.in_csv):
        sys.exit(3)

    df = pd.read_csv(args.in_csv)
    if len(df) != 14:
        sys.exit(3)

    home_col = 'team_home' if 'team_home' in df.columns else 'home'
    away_col = 'team_away' if 'team_away' in df.columns else 'away'
    if home_col not in df.columns or away_col not in df.columns:
        sys.exit(3)

    df[home_col] = df[home_col].apply(normalize_team_name)
    df[away_col] = df[away_col].apply(normalize_team_name)

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    df.to_csv(args.out_csv, index=False)

if __name__ == "__main__":
    main()