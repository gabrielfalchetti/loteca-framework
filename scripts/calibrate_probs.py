#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Calibra probabilidades (3 classes: 1, X, 2) via Isotonic Regression one-vs-rest.

Fluxo:
- Treina calibradores se houver histórico (data/history/calibration.csv).
  Formatos aceitos (obrigatório ter rótulo):
    colunas: p1, px, p2, y    (y ∈ {'1','X','2'} ou {1,'X',2} ou {0,1,2})
- Se não houver histórico, tenta carregar calibradores salvos em data/model/calibration_isotonic.pkl
- Se nada existir, faz NO-OP (copia de entrada para saída).

Entrada (em data/out/<rodada>/):
- probabilities_blended.csv OU probabilities.csv  (usa o primeiro que existir)

Saídas (em data/out/<rodada>/):
- probabilities_calibrated.csv
- calibration_report.json  (métricas no histórico, se houver)

Exemplo:
  python scripts/calibrate_probs.py --rodada 2025-09-27_1213
"""

from __future__ import annotations
import argparse
import os
import sys
import json
import pickle
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss

MODEL_DIR = os.path.join("data","model")
HIST_PATH_DEFAULT = os.path.join("data","history","calibration.csv")
MODEL_PATH_DEFAULT = os.path.join(MODEL_DIR, "calibration_isotonic.pkl")

CLASSES = ["1","X","2"]

def _coerce_label(y):
    if y in [1,"1","H","home","Home"]:
        return "1"
    if y in ["X","x",0,"D","draw","empate","Empate"]:
        return "X"
    if y in [2,"2","A","away","Away"]:
        return "2"
    # tentar inteiros 0,1,2 com mapeamento 0->1,1->X,2->2? (não comum)
    try:
        yi = int(y)
        if yi == 0: return "1"
        if yi == 1: return "X"
        if yi == 2: return "2"
    except Exception:
        pass
    return None

def _load_input_probs(out_dir: str) -> tuple[pd.DataFrame, str]:
    p_blend = os.path.join(out_dir, "probabilities_blended.csv")
    p_base  = os.path.join(out_dir, "probabilities.csv")
    path = p_blend if os.path.exists(p_blend) else p_base
    if not os.path.exists(path):
        raise FileNotFoundError("Não encontrei probabilities_blended.csv nem probabilities.csv")
    df = pd.read_csv(path)
    lower = {c: c.lower() for c in df.columns}
    df.rename(columns=lower, inplace=True)
    need = {"match_id","p1","px","p2"}
    if not need.issubset(df.columns):
        raise ValueError(f"Arquivo {path} sem colunas necessárias: {need - set(df.columns)}")
    return df, path

def _fit_isotonic(history: pd.DataFrame):
    # y binário por classe (one-vs-rest)
    calibrators = {}
    metrics = {}
    P = history[["p1","px","p2"]].to_numpy().astype(float)
    y_raw = history["y"].apply(_coerce_label)
    if y_raw.isna().any():
        raise ValueError("Histórico contém rótulos inválidos em 'y' (esperado 1/X/2).")

    y_raw = y_raw.to_list()

    # brier e logloss pré
    y_onehot = np.zeros((len(history),3))
    for i,lab in enumerate(y_raw):
        y_onehot[i, CLASSES.index(lab)] = 1.0
    brier_pre = np.mean(np.sum((P - y_onehot)**2, axis=1))
    logloss_pre = log_loss(y_raw, P, labels=CLASSES)

    P_cal = np.zeros_like(P)
    for ci, cname in enumerate(CLASSES):
        y_bin = np.array([1.0 if lab == cname else 0.0 for lab in y_raw])
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(P[:,ci], y_bin)
        P_cal[:,ci] = iso.transform(P[:,ci])
        calibrators[cname] = iso

    # renormaliza
    s = P_cal.sum(axis=1, keepdims=True)
    s[s <= 0] = 1.0
    P_cal = P_cal / s

    brier_pos = np.mean(np.sum((P_cal - y_onehot)**2, axis=1))
    logloss_pos = log_loss(y_raw, P_cal, labels=CLASSES)

    metrics = {
        "samples": int(len(history)),
        "brier_pre": float(brier_pre),
        "brier_pos": float(brier_pos),
        "logloss_pre": float(logloss_pre),
        "logloss_pos": float(logloss_pos),
        "improvement_brier": float(brier_pre - brier_pos),
        "improvement_logloss": float(logloss_pre - logloss_pos),
    }
    return calibrators, metrics

def _apply_isotonic(df: pd.DataFrame, calibrators: dict) -> pd.DataFrame:
    P = df[["p1","px","p2"]].to_numpy().astype(float)
    P_cal = np.zeros_like(P)
    for ci, cname in enumerate(CLASSES):
        iso: IsotonicRegression = calibrators[cname]
        P_cal[:,ci] = iso.transform(P[:,ci])
    s = P_cal.sum(axis=1, keepdims=True)
    s[s <= 0] = 1.0
    P_cal = P_cal / s
    out = df.copy()
    out["p1"] = P_cal[:,0]
    out["px"] = P_cal[:,1]
    out["p2"] = P_cal[:,2]
    return out

def main():
    ap = argparse.ArgumentParser(description="Calibração isotônica 1/X/2 com histórico opcional.")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--history", default=HIST_PATH_DEFAULT, help="CSV histórico para treinar calibradores")
    ap.add_argument("--model_path", default=MODEL_PATH_DEFAULT, help="onde salvar/carregar calibradores")
    args = ap.parse_args()

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    # entrada
    df_in, in_path = _load_input_probs(out_dir)

    # 1) tentar treinar com histórico
    trained = False
    report = {"used_history": False, "history_path": args.history, "model_path": args.model_path}
    calibrators = None
    metrics = None

    if os.path.exists(args.history):
        try:
            hist = pd.read_csv(args.history)
            lower = {c: c.lower() for c in hist.columns}
            hist.rename(columns=lower, inplace=True)
            # mapear nomes alternativos
            alt = {"prob_1":"p1", "prob_x":"px", "prob_2":"p2", "label":"y", "resultado":"y"}
            for a,b in alt.items():
                if a in hist.columns and b not in hist.columns:
                    hist[b] = hist[a]
            need = {"p1","px","p2","y"}
            if need.issubset(hist.columns):
                calibrators, metrics = _fit_isotonic(hist)
                os.makedirs(MODEL_DIR, exist_ok=True)
                with open(args.model_path, "wb") as f:
                    pickle.dump(calibrators, f)
                trained = True
                report["used_history"] = True
                report["metrics_history"] = metrics
            else:
                print("[calib] AVISO: histórico encontrado mas sem p1/px/p2/y — ignorando.")
        except Exception as e:
            print(f"[calib] AVISO: falha ao treinar com histórico: {e}")

    # 2) se não treinou, tenta carregar calibradores
    if not trained and os.path.exists(args.model_path):
        try:
            with open(args.model_path, "rb") as f:
                calibrators = pickle.load(f)
            trained = True
            report["loaded_model"] = True
        except Exception as e:
            print(f"[calib] AVISO: falha ao carregar calibradores: {e}")

    out_path = os.path.join(out_dir, "probabilities_calibrated.csv")
    if trained and calibrators:
        df_out = _apply_isotonic(df_in, calibrators)
        df_out.to_csv(out_path, index=False, encoding="utf-8")
        print(f"[calib] OK -> {out_path} ({len(df_out)} linhas)")
    else:
        # NO-OP (copia)
        df_in.to_csv(out_path, index=False, encoding="utf-8")
        report["noop"] = True
        print(f"[calib] NO-OP (sem histórico/modelo). Copiado -> {out_path}")

    # report JSON
    with open(os.path.join(out_dir, "calibration_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[calib] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
