# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd

def _log(msg: str) -> None:
    print(f"[verify_data] {msg}", flush=True)

def verify_data(history_file: str) -> None:
    """Valida o arquivo de histórico."""
    if not os.path.isfile(history_file):
        _log(f"Arquivo {history_file} não encontrado")
        sys.exit(1)

    try:
        df = pd.read_csv(history_file)
        required_cols = ["match_id", "home", "away", "home_goals", "away_goals"]
        optional_cols = ["xG_home", "xG_away"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            _log(f"Colunas ausentes: {missing_cols}")
            sys.exit(1)
        
        if df.empty:
            _log("Arquivo de histórico vazio")
            sys.exit(1)
        
        # Verificar se há pelo menos 10 partidas para robustez
        if len(df) < 10:
            _log(f"Arquivo contém apenas {len(df)} partidas, mínimo esperado 10")
            sys.exit(1)
        
        # Validar tipos de dados
        if not all(df["home_goals"].notnull() & df["away_goals"].notnull()):
            _log("Valores nulos encontrados em home_goals ou away_goals")
            sys.exit(1)
        
        _log(f"OK — arquivo {history_file} válido com {len(df)} partidas")
    except Exception as e:
        _log(f"[CRITICAL] Erro: {e}")
        sys.exit(1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="Arquivo CSV de histórico")
    args = ap.parse_args()

    verify_data(args.history)

if __name__ == "__main__":
    main()