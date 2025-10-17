# -*- coding: utf-8 -*-
import os
import sys

# Função _log definida antes de qualquer uso
def _log(msg: str) -> None:
    print(f"[calibrate] {msg}", flush=True)

# Verificação e importação de pandas
try:
    import pandas as pd
    _log(f"Versão do pandas: {pd.__version__}")
except ImportError as e:
    _log(f"Erro crítico: falha na importação de pandas: {e}")
    sys.exit(9)

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.calibration import CalibratedClassifierCV
import csv
from typing import Dict, List

"""
Calibra probabilidades de previsão de resultados de futebol usando Regressão Isotônica ou Dirichlet.
Aplica modelo pré-treinado salvo em pickle, ajustando probs brutas para valores calibrados.

Saída: CSV com cabeçalho: match_id,team_home,team_away,p_home_cal,p_draw_cal,p_away_cal

Uso:
  python -m scripts.calibrate_probs --in predictions.csv --cal calibrator.pkl --out predictions_calibrated.csv
"""

def _calculate_brier_score(true_probs: np.ndarray, pred_probs: np.ndarray) -> float:
    """Calcula Brier Score para avaliar calibração."""
    return np.mean(np.sum((pred_probs - true_probs) ** 2, axis=1))

def _apply_calibration(probs: np.ndarray, calibrator: IsotonicRegression, method: str = "isotonic") -> np.ndarray:
    """Aplica calibração isotônica ou Dirichlet."""
    if calibrator is None or method == "none":
        return probs
    try:
        if method == "isotonic":
            return calibrator.predict(probs)
        elif method == "dirichlet":
            # Placeholder: Dirichlet requer mais dados (ex.: matriz de confusão)
            return probs  # Implementar futuramente com CalibratedClassifierCV
    except Exception:
        _log("Falha na calibração, retornando probs originais.")
        return probs

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="CSV de entrada com probs brutas")
    ap.add_argument("--cal", required=True, help="Arquivo pickle com calibrador")
    ap.add_argument("--out", required=True, help="CSV de saída com probs calibradas")
    ap.add_argument("--method", type=str, default="isotonic", choices=["isotonic", "dirichlet", "none"], help="Método de calibração")
    args = ap.parse_args()

    if not os.path.isfile(args.inp):
        _log(f"{args.inp} não encontrado")
        sys.exit(9)
    if not os.path.isfile(args.cal):
        _log(f"{args.cal} não encontrado")
        sys.exit(9)

    try:
        df = pd.read_csv(args.inp)
        # Validação de entrada
        if not all(col in df.columns for col in ["match_id", "team_home", "team_away", "p_home", "p_draw", "p_away"]):
            raise ValueError("CSV de entrada sem colunas esperadas")
        probs = df[["p_home", "p_draw", "p_away"]].values
        if not np.all((probs >= 0) & (probs <= 1)):
            raise ValueError("Probs inválidas (fora de [0,1])")
        if not np.allclose(probs.sum(axis=1), 1, atol=0.01):
            _log("Soma de probs != 1, normalizando...")
            probs = probs / probs.sum(axis=1, keepdims=True)

        # Carregar calibrador
        calibrators = None
        try:
            with open(args.cal, "rb") as f:
                calibrators = pickle.load(f)
        except Exception as e:
            _log(f"Erro ao carregar calibrador: {e}. Usando probs originais.")
            calibrators = {"home": None, "draw": None, "away": None}
        if not isinstance(calibrators, dict) or not all(k in calibrators for k in ["home", "draw", "away"]):
            _log("Calibrador inválido, usando probs originais.")
            calibrators = {"home": None, "draw": None, "away": None}

        # Aplicar calibração
        cal_probs = np.zeros_like(probs)
        for i, (ph, pd, pa) in enumerate(probs):
            cal_probs[i, 0] = _apply_calibration(np.array([ph]), calibrators["home"], args.method)
            cal_probs[i, 1] = _apply_calibration(np.array([pd]), calibrators["draw"], args.method)
            cal_probs[i, 2] = _apply_calibration(np.array([pa]), calibrators["away"], args.method)
        s = cal_probs.sum(axis=1, keepdims=True)
        cal_probs = cal_probs / s if s.any() > 0 else probs  # Normaliza se soma > 0

        # Calcular Brier Score (placeholder, requer verdadeiros)
        # brier = _calculate_brier_score(np.ones_like(cal_probs) * 0.33, cal_probs)  # Exemplo fictício
        # _log(f"Brier Score: {brier:.4f}")

        # Salvar resultados
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        out_rows = [[r["match_id"], r["team_home"], r["team_away"], cal_p[0], cal_p[1], cal_p[2]] 
                    for r, cal_p in zip(df.to_dict("records"), cal_probs)]
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["match_id", "team_home", "team_away", "p_home_cal", "p_draw_cal", "p_away_cal"])
            w.writerows(out_rows)
        _log(f"OK -> {args.out} (linhas={len(out_rows)})")
    except Exception as e:
        _log(f"[CRITICAL] Erro: {e}")
        sys.exit(9)

if __name__ == "__main__":
    main()