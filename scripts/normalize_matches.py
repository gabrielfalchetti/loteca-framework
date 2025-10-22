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
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "").replace("/pe", "").replace("/pr", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo").replace("inter de milao", "inter").replace("manchester united", "manchester utd")
    name = name.replace("sport recife", "sport").replace("atletico mineiro", "atlético mineiro").replace("bragantino-sp", "bragantino").replace("vasco da gama", "vasco")
    name = name.replace("fluminense", "fluminense").replace("santos", "santos").replace("vitoria", "vitória").replace("mirassol", "mirassol").replace("gremio", "grêmio")
    name = name.replace("juventude", "juventude").replace("roma", "roma").replace("getafe", "getafe").replace("real madrid", "real madrid").replace("liverpool", "liverpool")
    name = name.replace("atalanta bergamas", "atalanta").replace("fiorentina", "fiorentina").replace("osasuna", "osasuna").replace("athletico paranaense", "athletico paranaense")
    name = name.replace("operario", "operário").replace("atletico madrid", "atlético madrid")
    return name.capitalize()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    args = ap.parse_args()

    if not os.path.isfile(args.in_csv):
        _log(f"{args.in_csv} não encontrado")
        sys.exit(3)

    try:
        df = pd.read_csv(args.in_csv)
    except Exception as e:
        _log(f"Erro ao ler {args.in_csv}: {e}")
        sys.exit(3)

    if df.empty:
        _log("Arquivo de entrada vazio")
        sys.exit(3)

    home_col = 'team_home' if 'team_home' in df.columns else 'home'
    away_col = 'team_away' if 'team_away' in df.columns else 'away'
    if home_col not in df.columns or away_col not in df.columns:
        _log("Colunas team_home/team_away ou home/away não encontradas")
        sys.exit(3)

    df[home_col] = df[home_col].apply(normalize_team_name)
    df[away_col] = df[away_col].apply(normalize_team_name)
    
    if df[[home_col, away_col]].isnull().any().any():
        _log("Valores nulos em team_home ou team_away após normalização")
        sys.exit(3)

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    df.to_csv(args.out_csv, index=False)
    _log(f"Arquivo {args.out_csv} gerado com {len(df)} jogos")

if __name__ == "__main__":
    main()