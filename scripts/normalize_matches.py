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
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "").replace("/pe", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo").replace("inter de milao", "inter").replace("manchester united", "manchester utd").replace("ldu quito", "ldu")
    name = name.replace("sport recife", "sport").replace("atletico mineiro", "atlético").replace("bragantino-sp", "bragantino").replace("vasco da gama", "vasco").replace("fluminense", "fluminense").replace("santos", "santos").replace("vitoria", "vitória").replace("mirassol", "mirassol").replace("gremio", "grêmio").replace("juventude", "juventude").replace("roma", "roma").replace("getafe", "getafe").replace("real madrid", "real madrid").replace("liverpool", "liverpool")
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
    if df.empty:
        _log("Arquivo de entrada vazio")
        sys.exit(3)
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
        df['match_id'] = range(1, len(df) + 1)

    df["team_home"] = df["team_home"].apply(normalize_team_name)
    df["team_away"] = df["team_away"].apply(normalize_team_name)
    
    if df[['team_home', 'team_away']].isnull().any().any():
        _log("Valores nulos em team_home ou team_away após normalização")
        sys.exit(3)

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    df.to_csv(args.out_csv, index=False)
    _log(f"Arquivo {args.out_csv} gerado com {len(df)} jogos")

if __name__ == "__main__":
    main()