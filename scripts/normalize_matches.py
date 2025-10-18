# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[normalize] {msg}", flush=True)

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
        _log(f"{args.in_csv} não encontrado")
        sys.exit(3)

    df = pd.read_csv(args.in_csv)
    if len(df) != 14:
        _log(f"Arquivo {args.in_csv} contém {len(df)} jogos, esperado 14")
        sys.exit(3)

    home_col = next((col for col in ['team_home', 'home'] if col in df.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in df.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas")
        sys.exit(3)

    df = df.rename(columns={home_col: 'team_home', away_col: 'team_away'})
    if 'match_id' not in df.columns:
        df['match_id'] = range(1, 15)

    df['team_home'] = df['team_home'].apply(normalize_team_name)
    df['team_away'] = df['team_away'].apply(normalize_team_name)

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    df.to_csv(args.out_csv, index=False)
    _log(f"OK — {args.out_csv} gerado com 14 jogos")

if __name__ == "__main__":
    main()