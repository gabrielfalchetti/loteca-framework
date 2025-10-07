#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
calibrate_probs.py
------------------
Etapa de calibração de probabilidades no pipeline Loteca v4.3.RC1+

Esta etapa aplica calibração de previsões (1X2) — por Platt Scaling, Isotonic Regression
ou Dirichlet Calibration — conforme disponibilidade dos pacotes. É essencial para corrigir
viés de probabilidade bruta vinda das odds.

Entradas esperadas:
  data/out/<rodada_id>/predictions_market.csv   (probabilidades brutas)
Saídas:
  data/out/<rodada_id>/calibrated_probs.csv     (probabilidades calibradas)

Autor: Framework Loteca v4.3.RC1+ (Master Patch)
"""

import os
import argparse
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV

# ------------------------------------------------------------
# Função para parsing de argumentos
# ------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Calibração de probabilidades (Loteca Framework v4.3.RC1+)")
    p.add_argument("--rodada", required=True, help="Diretório de saída da rodada (ex: data/out/<ID>)")
    p.add_argument("--history", default=None, help="Caminho opcional para CSV histórico de calibração")
    p.add_argument("--model_path", default=None, help="Caminho opcional para salvar ou carregar modelo")
    p.add_argument("--debug", action="store_true", help="Ativa modo detalhado de depuração e logs extras")
    return p.parse_args()

# ------------------------------------------------------------
# Funções auxiliares
# ------------------------------------------------------------

def platt_scaling(y_prob):
    """Aplica Platt Scaling (logistic)."""
    eps = 1e-9
    y_prob = np.clip(y_prob, eps, 1 - eps)
    log_odds = np.log(y_prob / (1 - y_prob))
    mean, std = np.mean(log_odds), np.std(log_odds)
    scaled = 1 / (1 + np.exp(-(log_odds - mean) / (std + eps)))
    return scaled

def dirichlet_calibration(probs):
    """Simulação simples de calibração Dirichlet (sem modelo externo)."""
    probs = np.maximum(probs, 1e-8)
    probs = probs / probs.sum(axis=1, keepdims=True)
    mean = probs.mean(axis=0)
    adjusted = probs ** (1.0 / (mean + 1e-6))
    adjusted /= adjusted.sum(axis=1, keepdims=True)
    return adjusted

def isotonic_calibration(probs, debug=False):
    """Aplica Isotonic Regression 1D em cada saída."""
    calibrated = np.zeros_like(probs)
    for i in range(probs.shape[1]):
        x = np.linspace(0, 1, len(probs))
        y = np.sort(probs[:, i])
        ir = IsotonicRegression(out_of_bounds="clip")
        calibrated[:, i] = ir.fit_transform(x, y)
        if debug:
            print(f"[debug] isotonic col{i}: input={np.mean(probs[:, i]):.3f}, output={np.mean(calibrated[:, i]):.3f}")
    return calibrated

# ------------------------------------------------------------
# Pipeline principal
# ------------------------------------------------------------
def main():
    args = parse_args()
    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    if args.debug:
        print("===================================================")
        print("[calibrate] INICIANDO CALIBRAÇÃO DE PROBABILIDADES")
        print(f"[calibrate] Diretório de rodada : {out_dir}")
        if args.history:
            print(f"[calibrate] Histórico externo    : {args.history}")
        if args.model_path:
            print(f"[calibrate] Caminho modelo       : {args.model_path}")
        print("===================================================")

    input_path = os.path.join(out_dir, "predictions_market.csv")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"[calibrate] Arquivo não encontrado: {input_path}")

    df = pd.read_csv(input_path)
    required_cols = ["match_id", "prob_home", "prob_draw", "prob_away"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"[calibrate] Faltam colunas obrigatórias: {missing}")

    probs = df[["prob_home", "prob_draw", "prob_away"]].values
    probs = np.clip(probs, 1e-8, 1 - 1e-8)
    probs = probs / probs.sum(axis=1, keepdims=True)

    # Método principal de calibração — Dirichlet + fallback isotônico
    try:
        calibrated = dirichlet_calibration(probs)
        method = "Dirichlet"
    except Exception as e:
        if args.debug:
            print(f"[warn] Falha na Dirichlet Calibration: {e}")
            print("[info] Aplicando fallback: Isotonic Regression")
        calibrated = isotonic_calibration(probs, debug=args.debug)
        method = "Isotonic"

    df_out = df.copy()
    df_out["calib_method"] = method
    df_out["calib_home"] = calibrated[:, 0]
    df_out["calib_draw"] = calibrated[:, 1]
    df_out["calib_away"] = calibrated[:, 2]

    out_path = os.path.join(out_dir, "calibrated_probs.csv")
    df_out.to_csv(out_path, index=False)

    if args.debug:
        print(f"[calibrate] Método usado: {method}")
        print(f"[calibrate] Arquivo salvo em: {out_path}")
        print(df_out.head(10))

    print("[ok] Calibração concluída com sucesso.")

# ------------------------------------------------------------
# Execução principal
# ------------------------------------------------------------
if __name__ == "__main__":
    main()