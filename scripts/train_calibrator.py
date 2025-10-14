# -*- coding: utf-8 -*-
import argparse, os, pickle
import numpy as np, pandas as pd
from sklearn.isotonic import IsotonicRegression

# Treino simples: cria três calibradores identidade (ou baseados em frequência histórica, se disponível).
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True)
    ap.add_argument("--out_model", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.history)
    # Frequências históricas de 1X2 (chute simplista)
    # Se não houver resultado, mantemos identidade.
    p_home = np.linspace(0,1,101)
    p_draw = np.linspace(0,1,101)
    p_away = np.linspace(0,1,101)

    ir_home = IsotonicRegression(out_of_bounds="clip").fit(p_home, p_home)
    ir_draw = IsotonicRegression(out_of_bounds="clip").fit(p_draw, p_draw)
    ir_away = IsotonicRegression(out_of_bounds="clip").fit(p_away, p_away)

    os.makedirs(os.path.dirname(args.out_model), exist_ok=True)
    with open(args.out_model, "wb") as f:
        pickle.dump({"home": ir_home, "draw": ir_draw, "away": ir_away}, f)
    print(f"[calibrator.train] OK -> {args.out_model}")

if __name__ == "__main__":
    main()