#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
calibrate_probs.py
------------------
Aplica calibração estatística (ex.: Regressão Isotônica) nas probabilidades 1X2
produzidas por <OUT_DIR>/xg_bivariate.csv e grava <OUT_DIR>/probs_calibrated.csv.

Uso (no workflow):
  python -m scripts.calibrate_probs --rodada <OUT_DIR> [--calibration_dir data/history/calibration]

Entradas esperadas:
  - <OUT_DIR>/xg_bivariate.csv   (colunas: match_id, team_home, team_away, p_home, p_draw, p_away)
  - (opcional) calibradores em <calibration_dir>/calibrator_home.pkl, calibrator_draw.pkl, calibrator_away.pkl

Saída:
  - <OUT_DIR>/probs_calibrated.csv  com colunas:
      match_id, team_home, team_away,
      p_home_raw, p_draw_raw, p_away_raw,
      p_home,     p_draw,     p_away,
      cal_home, cal_draw, cal_away  (flags se foi aplicado calibrador)
Regras:
  - Se um ou mais calibradores não existirem, aplica identidade para o respectivo alvo.
  - As três probabilidades calibradas são renormalizadas para somarem 1.
  - Valores são "clampados" para [1e-9, 1-1e-9] antes da renormalização.
Saída de erro (exit 9) se:
  - xg_bivariate.csv não existir ou não tiver colunas obrigatórias
  - falha ao salvar o arquivo final
"""

from __future__ import annotations

import argparse
import os
import sys
import json
from typing import Optional, Tuple

import numpy as np
import pandas as pd

# joblib é dependência do scikit-learn (instalado no workflow)
try:
    import joblib
except Exception:  # fallback em raros ambientes
    joblib = None


# ---------------------------- Utilidades ----------------------------- #

def _clamp01(x: np.ndarray, lo: float = 1e-9, hi: float = 1.0 - 1e-9) -> np.ndarray:
    return np.clip(x, lo, hi)


class _IdentityCalibrator:
    """Aplica identidade (sem calibração)."""
    def predict(self, x):
        x = np.asarray(x, dtype=float)
        return x


def _load_calibrator(path: str) -> Optional[object]:
    if joblib is None:
        return None
    if not os.path.exists(path):
        return None
    try:
        return joblib.load(path)
    except Exception as e:
        print(f"[calibrate][WARN] Falha carregando calibrador {path}: {e}")
        return None


def _require_columns(df: pd.DataFrame, cols: Tuple[str, ...], tag: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{tag} sem colunas obrigatórias: {missing}")


def _renorm_triple(p1: np.ndarray, pX: np.ndarray, p2: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Renormaliza três vetores de probabilidades para somarem 1 (com clamp prévio)."""
    p1 = _clamp01(p1)
    pX = _clamp01(pX)
    p2 = _clamp01(p2)
    s = p1 + pX + p2
    # Evita divisão por zero
    s = np.where(s <= 0, 1.0, s)
    return p1 / s, pX / s, p2 / s


# ------------------------------- MAIN -------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (OUT_DIR)")
    ap.add_argument("--calibration_dir", default="data/history/calibration",
                    help="Diretório onde estão os calibradores *.pkl")
    args = ap.parse_args()

    out_dir = args.rodada
    calib_dir = args.calibration_dir

    print("===================================================")
    print("[calibrate] INICIANDO CALIBRAÇÃO DE PROBABILIDADES")
    print(f"[calibrate] Diretório de rodada : {out_dir}")
    print(f"[calibrate] Diretório calibrador: {calib_dir}")
    print("===================================================")

    # 1) Carrega previsões do núcleo (xg_bivariate.csv)
    xg_path = os.path.join(out_dir, "xg_bivariate.csv")
    if not os.path.exists(xg_path):
        print(f"[calibrate][CRITICAL] Arquivo {xg_path} não encontrado.")
        return 9

    try:
        df = pd.read_csv(xg_path)
    except Exception as e:
        print(f"[calibrate][CRITICAL] Falha lendo {xg_path}: {e}")
        return 9

    try:
        _require_columns(
            df,
            ("match_id", "team_home", "team_away", "p_home", "p_draw", "p_away"),
            "xg_bivariate.csv",
        )
    except Exception as e:
        print(f"[calibrate][CRITICAL] {e}")
        return 9

    n = len(df)
    if n == 0:
        print("[calibrate][CRITICAL] xg_bivariate.csv está vazio.")
        return 9

    # 2) Carrega calibradores (se existirem); do contrário, identidade
    cal_home_path = os.path.join(calib_dir, "calibrator_home.pkl")
    cal_draw_path = os.path.join(calib_dir, "calibrator_draw.pkl")
    cal_away_path = os.path.join(calib_dir, "calibrator_away.pkl")

    ch = _load_calibrator(cal_home_path)
    cx = _load_calibrator(cal_draw_path)
    ca = _load_calibrator(cal_away_path)

    ch = ch if ch is not None else _IdentityCalibrator()
    cx = cx if cx is not None else _IdentityCalibrator()
    ca = ca if ca is not None else _IdentityCalibrator()

    flag_h = 0 if isinstance(ch, _IdentityCalibrator) else 1
    flag_x = 0 if isinstance(cx, _IdentityCalibrator) else 1
    flag_a = 0 if isinstance(ca, _IdentityCalibrator) else 1

    if flag_h + flag_x + flag_a == 0:
        print("[calibrate][NOTICE] Calibradores não encontrados — aplicando identidade (pass-through).")

    # 3) Aplica calibração
    p_home_raw = df["p_home"].to_numpy(dtype=float)
    p_draw_raw = df["p_draw"].to_numpy(dtype=float)
    p_away_raw = df["p_away"].to_numpy(dtype=float)

    # Garantia de faixa antes de alimentar o calibrador
    p_home_in = _clamp01(p_home_raw)
    p_draw_in = _clamp01(p_draw_raw)
    p_away_in = _clamp01(p_away_raw)

    try:
        p_home_cal = np.asarray(ch.predict(p_home_in), dtype=float).reshape(-1)
    except Exception as e:
        print(f"[calibrate][WARN] Falha no calibrador HOME, usando identidade: {e}")
        p_home_cal = p_home_in.copy()
        flag_h = 0

    try:
        p_draw_cal = np.asarray(cx.predict(p_draw_in), dtype=float).reshape(-1)
    except Exception as e:
        print(f"[calibrate][WARN] Falha no calibrador DRAW, usando identidade: {e}")
        p_draw_cal = p_draw_in.copy()
        flag_x = 0

    try:
        p_away_cal = np.asarray(ca.predict(p_away_in), dtype=float).reshape(-1)
    except Exception as e:
        print(f"[calibrate][WARN] Falha no calibrador AWAY, usando identidade: {e}")
        p_away_cal = p_away_in.copy()
        flag_a = 0

    # 4) Renormaliza para somar 1
    p_home, p_draw, p_away = _renorm_triple(p_home_cal, p_draw_cal, p_away_cal)

    # 5) Monta saída
    out = df.copy()
    out.insert(out.columns.get_loc("p_home"), "p_home_raw", p_home_raw)
    out.insert(out.columns.get_loc("p_draw"), "p_draw_raw", p_draw_raw)
    out.insert(out.columns.get_loc("p_away"), "p_away_raw", p_away_raw)

    out["p_home"] = np.round(p_home, 6)
    out["p_draw"] = np.round(p_draw, 6)
    out["p_away"] = np.round(p_away, 6)

    out["cal_home"] = int(flag_h)
    out["cal_draw"] = int(flag_x)
    out["cal_away"] = int(flag_a)

    # 6) Salva
    out_path = os.path.join(out_dir, "probs_calibrated.csv")
    try:
        out.to_csv(out_path, index=False)
    except Exception as e:
        print(f"[calibrate][CRITICAL] Falha salvando {out_path}: {e}")
        return 9

    # 7) (Opcional) Log de sanidade
    s_ok = np.allclose(out["p_home"] + out["p_draw"] + out["p_away"], 1.0, atol=1e-6)
    print(f"[ok] Calibração concluída. Registros={len(out)}  Renormalizado={bool(s_ok)}")
    print(f"[ok] Arquivo gerado: {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())