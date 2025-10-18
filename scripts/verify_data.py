# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os

def _log(msg: str) -> None:
    print(f"[verify_data] {msg}", flush=True)

def verify_data(history_file: str) -> None:
    """Verifica dados históricos."""
    if not os.path.isfile(history_file):
        _log(f"Arquivo {history_file} não encontrado")
        sys.exit(1)

    df = pd.read_csv(history_file)
    required_cols = ['match_id', 'team_home', 'team_away', 'score_home', 'score_away']
    optional_cols = ['xG_home', 'xG_away']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        _log(f"Colunas obrigatórias ausentes: {missing}")
        sys.exit(1)

    if df.empty:
        _log("Histórico vazio — falhando.")
        sys.exit(1)

    # Verificar se há valores válidos
    if df[['team_home', 'team_away']].isnull().any().any():
        _log("Valores nulos encontrados em team_home ou team_away")
        sys.exit(1)
    if df[['score_home', 'score_away']].lt(0).any().any():
        _log("Valores negativos encontrados em score_home ou score_away")
        sys.exit(1)

    _log(f"OK — {len(df)} linhas validadas em {history_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="Arquivo CSV de histórico")
    args = ap.parse_args()

    verify_data(args.history)

if __name__ == "__main__":
    main()