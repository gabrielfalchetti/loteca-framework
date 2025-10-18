# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import pickle
import os
from sklearn.isotonic import IsotonicRegression

def _log(msg: str) -> None:
    print(f"[calibrator_train] {msg}", flush=True)

def train_calibrator(history_file: str) -> dict:
    """Treina calibradores isotônicos para cada resultado."""
    df = pd.read_parquet(history_file)
    required_cols = ["score_home", "score_away", "p_home", "p_draw", "p_away"]
    if not all(col in df.columns for col in required_cols):
        _log("Histórico sem colunas esperadas para calibração")
        sys.exit(9)

    calibrators = {
        "home": IsotonicRegression(out_of_bounds="clip"),
        "draw": IsotonicRegression(out_of_bounds="clip"),
        "away": IsotonicRegression(out_of_bounds="clip")
    }
    # Placeholder para treinamento (substitua com lógica real)
    return calibrators

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="Arquivo Parquet de histórico")
    ap.add_argument("--out", required=True, help="Arquivo PKL de saída")
    args = ap.parse_args()

    if not os.path.isfile(args.history):
        _log(f"{args.history} não encontrado")
        sys.exit(9)

    calibrators = train_calibrator(args.history)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "wb") as f:
        pickle.dump(calibrators, f)
    _log(f"OK — calibrador salvo em {args.out}")

if __name__ == "__main__":
    main()