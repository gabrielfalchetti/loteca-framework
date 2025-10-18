# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
from unidecode import unidecode

def _log(msg: str) -> None:
    print(f"[normalize] {msg}", flush=True)

def normalize_team_name(name: str) -> str:
    """Normaliza nomes de times para consistência."""
    if not isinstance(name, str):
        return ""
    name = unidecode(name).lower().strip()
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo")
    return name.capitalize()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True, help="CSV de entrada com jogos")
    ap.add_argument("--out_csv", required=True, help="CSV de saída normalizado")
    args = ap.parse_args()

    if not os.path.isfile(args.in_csv):
        _log(f"{args.in_csv} não encontrado")
        sys.exit(3)

    df = pd.read_csv(args.in_csv)
    if df.empty:
        _log("Arquivo de entrada vazio — falhando.")
        sys.exit(3)

    # Verificar colunas de entrada
    home_col = 'team_home' if 'team_home' in df.columns else 'home' if 'home' in df.columns else None
    away_col = 'team_away' if 'team_away' in df.columns else 'away' if 'away' in df.columns else None
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas em in_csv")
        sys.exit(3)

    # Renomear para colunas padrão
    df = df.rename(columns={home_col: 'team_home', away_col: 'team_away'})
    if 'match_id' not in df.columns:
        df['match_id'] = range(1, len(df) + 1)

    # Normalizar nomes
    df["team_home"] = df["team_home"].apply(normalize_team_name)
    df["team_away"] = df["team_away"].apply(normalize_team_name)
    
    # Verificar nulos
    if df[['team_home', 'team_away']].isnull().any().any():
        _log("Valores nulos encontrados em team_home ou team_away após normalização")
        sys.exit(3)

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    df.to_csv(args.out_csv, index=False)
    _log(f"OK — gravado em {args.out_csv} linhas={len(df)}")

if __name__ == "__main__":
    main()