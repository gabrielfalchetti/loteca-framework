#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Treina modelos de calibração isotônica para probabilidades de futebol (home, draw, away)
usando dados históricos de previsões e resultados reais.

Saída: Pickle com dicionário de IsotonicRegression por classe.

Uso:
  python -m scripts.calibrator_train --history data/history/features.parquet --out data/out/calibrator.pkl
"""

from __future__ import annotations

import argparse
import os
import pickle
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from typing import Dict

def _log(msg: str) -> None:
    print(f"[calibrator_train] {msg}", flush=True)

def train_calibrator(history: str, out: str) -> None:
    """Treina calibradores isotônicos para cada classe (home, draw, away)."""
    if not os.path.isfile(history):
        _log(f"{history} não encontrado")
        return

    try:
        # Placeholder: Substitua por CSV com previsões e resultados reais
        df = pd.read_parquet(history)
        if not all(col in df.columns for col in ["p_home", "p_draw", "p_away", "true_home", "true_draw", "true_away"]):
            raise ValueError("Histórico sem colunas esperadas para calibração")

        calibrators: Dict[str, IsotonicRegression] = {}
        for cls in ["home", "draw", "away"]:
            X = df[f"p_{cls}"].values
            y = df[f"true_{cls}"].values  # 1 se verdadeiro, 0 se falso (exemplo)
            if len(X) == 0 or len(y) == 0:
                _log(f"Sem dados para {cls}, usando modelo vazio")
                calibrators[cls] = IsotonicRegression()
            else:
                calibrators[cls] = IsotonicRegression().fit(X, y)

        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "wb") as f:
            pickle.dump(calibrators, f)
        _log(f"OK — calibradores salvos em {out}")

    except Exception as e:
        _log(f"[CRITICAL] Erro: {e}")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "wb") as f:
            pickle.dump({"home": IsotonicRegression(), "draw": IsotonicRegression(), "away": IsotonicRegression()}, f)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=str, required=True, help="Caminho do histórico de features")
    parser.add_argument("--out", type=str, required=True, help="Caminho do arquivo pickle de saída")
    args = parser.parse_args()
    train_calibrator(args.history, args.out)

if __name__ == "__main__":
    main()