# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import pickle
import os
from sklearn.isotonic import IsotonicRegression

def _log(msg: str) -> None:
    print(f"[calibrator_train] {msg}", flush=True)

def train_calibrator(history_df: pd.DataFrame) -> IsotonicRegression:
    # Verificar colunas no history_df
    required_cols = ['prob_home', 'prob_draw', 'prob_away', 'result']
    missing_cols = [col for col in required_cols if col not in history_df.columns]
    if missing_cols:
        _log(f"Aviso: Colunas ausentes no history: {missing_cols}. Usando calibrador padrão.")
        return IsotonicRegression(out_of_bounds='clip')

    _log(f"Colunas no history: {list(history_df.columns)}")
    _log(f"Linhas no history: {len(history_df)}")

    if history_df.empty:
        _log("Histórico vazio, usando calibrador padrão")
        return IsotonicRegression(out_of_bounds='clip')

    # Filtrar dados válidos
    history_df = history_df.dropna(subset=required_cols)
    if history_df.empty:
        _log("Nenhum dado válido para treinamento após remoção de nulos, usando calibrador padrão")
        return IsotonicRegression(out_of_bounds='clip')

    # Treinar calibrador (exemplo simplificado para prob_home)
    try:
        X = history_df['prob_home'].values
        y = (history_df['result'] == 'home').astype(int).values
        calibrator = IsotonicRegression(out_of_bounds='clip')
        calibrator.fit(X, y)
        _log("Calibrador treinado com sucesso")
        return calibrator
    except Exception as e:
        _log(f"Erro ao treinar calibrador: {e}, usando calibrador padrão")
        return IsotonicRegression(out_of_bounds='clip')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if not os.path.isfile(args.history):
        _log(f"Arquivo de histórico {args.history} não encontrado, usando calibrador padrão")
        history_df = pd.DataFrame(columns=['prob_home', 'prob_draw', 'prob_away', 'result'])
    else:
        try:
            history_df = pd.read_parquet(args.history)
        except Exception as e:
            _log(f"Erro ao ler {args.history}: {e}, usando calibrador padrão")
            history_df = pd.DataFrame(columns=['prob_home', 'prob_draw', 'prob_away', 'result'])

    calibrator = train_calibrator(history_df)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'wb') as f:
        pickle.dump(calibrator, f)
    _log(f"Arquivo {args.out} gerado")

if __name__ == "__main__":
    main()