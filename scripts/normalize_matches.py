# -*- coding: utf-8 -*-
import pandas as pd
import argparse
import os

def _log(msg: str) -> None:
    print(f"[normalize_matches] {msg}", flush=True)

def normalize_matches(in_csv, out_csv):
    if not os.path.isfile(in_csv):
        _log(f"Arquivo {in_csv} não encontrado")
        return

    try:
        df = pd.read_csv(in_csv)
        _log(f"Conteúdo de {in_csv}:\n{df.to_string()}")
    except Exception as e:
        _log(f"Erro ao ler {in_csv}: {e}")
        return

    # Verificar colunas esperadas
    required_columns = ['home', 'away']
    if not all(col in df.columns for col in required_columns):
        _log(f"Erro: {in_csv} não contém as colunas esperadas: {required_columns}")
        return

    # Filtrar apenas jogos brasileiros (se houver coluna 'league')
    brazilian_leagues = ['Série A', 'Série B', 'Copa do Brasil']
    if 'league' in df.columns:
        df = df[df['league'].isin(brazilian_leagues)]
        _log(f"Filtrado {len(df)} jogos brasileiros")
    else:
        _log("Aviso: coluna 'league' não encontrada, processando todos os jogos")

    # Normalização dos nomes dos times
    df['home'] = df['home'].str.strip().str.lower()
    df['away'] = df['away'].str.strip().str.lower()

    # Salvar o resultado
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False)
    _log(f"Arquivo salvo em {out_csv}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    args = ap.parse_args()
    normalize_matches(args.in_csv, args.out_csv)

if __name__ == "__main__":
    main()