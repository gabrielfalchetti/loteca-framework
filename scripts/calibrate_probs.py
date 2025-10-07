#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
calibrate_probs.py
------------------
Etapa de calibração de probabilidades no pipeline Loteca v4.3.RC1+

Fluxo:
1) Tenta ler data/out/<RODADA_ID>/predictions_market.csv com colunas:
   [match_id, prob_home, prob_draw, prob_away]
2) Se estiver ausente ou sem essas colunas, cai em fallback:
   - Lê data/out/<RODADA_ID>/odds_consensus.csv
   - Requer colunas: [team_home, team_away, odds_home, odds_draw, odds_away]
   - Converte odds -> probabilidades implícitas (normalizadas)
   - Gera match_id = "<home>__<away>" e o CSV intermediário em memória
3) Aplica calibração (Dirichlet; fallback para Isotonic)
4) Salva data/out/<RODADA_ID>/calibrated_probs.csv

Uso:
  python scripts/calibrate_probs.py --rodada data/out/<ID> [--history ...] [--model_path ...] [--debug]

Saída:
  data/out/<ID>/calibrated_probs.csv
"""

import os
import argparse
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

# -------------------- CLI --------------------
def parse_args():
    p = argparse.ArgumentParser(description="Calibração de probabilidades (Loteca Framework v4.3.RC1+)")
    p.add_argument("--rodada", required=True, help="Diretório de saída da rodada (ex: data/out/<ID>)")
    p.add_argument("--history", default=None, help="(Opcional) CSV com histórico para calibração")
    p.add_argument("--model_path", default=None, help="(Opcional) caminho p/ salvar/carregar modelo")
    p.add_argument("--debug", action="store_true", help="Ativa logs detalhados")
    return p.parse_args()

# -------------------- Helpers --------------------
def _log(debug, msg):
    if debug:
        print(msg)

def _require_cols(df, cols):
    return [c for c in cols if c not in df.columns]

def _from_odds_to_probs(df, debug=False):
    """Converte odds para probabilidades implícitas e normaliza por linha."""
    for c in ["odds_home", "odds_draw", "odds_away"]:
        if c not in df.columns:
            raise ValueError(f"[calibrate] Faltou coluna em odds_consensus.csv: {c}")

    odds = df[["odds_home", "odds_draw", "odds_away"]].astype(float).values
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0 / np.clip(odds, 1e-9, None)
    inv[np.isnan(inv)] = 0.0
    row_sum = inv.sum(axis=1, keepdims=True)
    row_sum = np.clip(row_sum, 1e-9, None)
    probs = inv / row_sum

    out = pd.DataFrame({
        "match_id": (df["team_home"].astype(str).str.strip() + "__" +
                     df["team_away"].astype(str).str.strip()),
        "prob_home": probs[:, 0],
        "prob_draw": probs[:, 1],
        "prob_away": probs[:, 2],
    })
    _log(debug, f"[calibrate] Fallback probs (odds→probs) gerado para {len(out)} jogos.")
    return out

def _dirichlet_calibration(probs):
    """Heurística simples tipo Dirichlet (sem modelo externo)."""
    probs = np.maximum(probs, 1e-8)
    probs = probs / probs.sum(axis=1, keepdims=True)
    mean = probs.mean(axis=0)
    adjusted = probs ** (1.0 / (mean + 1e-6))
    adjusted = adjusted / adjusted.sum(axis=1, keepdims=True)
    return adjusted

def _isotonic_calibration(probs, debug=False):
    """Isotonic 1D independente por coluna (fallback)."""
    calibrated = np.zeros_like(probs)
    n = probs.shape[0]
    x = np.linspace(0, 1, n)
    for i in range(probs.shape[1]):
        y = np.sort(probs[:, i])
        ir = IsotonicRegression(out_of_bounds="clip")
        calibrated[:, i] = ir.fit_transform(x, y)
        _log(debug, f"[debug] isotonic col{i}: in_mean={np.mean(probs[:, i]):.3f} -> out_mean={np.mean(calibrated[:, i]):.3f}")
    # renormaliza por segurança
    s = calibrated.sum(axis=1, keepdims=True)
    s = np.clip(s, 1e-9, None)
    calibrated = calibrated / s
    return calibrated

# -------------------- Main --------------------
def main():
    args = parse_args()
    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    print("===================================================")
    print("[calibrate] INICIANDO CALIBRAÇÃO DE PROBABILIDADES")
    print(f"[calibrate] Diretório de rodada : {out_dir}")
    if args.history:   print(f"[calibrate] Histórico extern.: {args.history}")
    if args.model_path:print(f"[calibrate] Modelo (path)    : {args.model_path}")
    print("===================================================")

    # 1) Tenta ler predictions_market.csv
    pred_path = os.path.join(out_dir, "predictions_market.csv")
    df_pred = None
    if os.path.exists(pred_path):
        try:
            df_pred = pd.read_csv(pred_path)
            miss = _require_cols(df_pred, ["match_id", "prob_home", "prob_draw", "prob_away"])
            if miss:
                _log(args.debug, f"[calibrate] predictions_market.csv sem colunas {miss}. Usando fallback por odds.")
                df_pred = None
        except Exception as e:
            _log(args.debug, f"[calibrate] falha lendo predictions_market.csv: {e}. Usando fallback por odds.")
            df_pred = None
    else:
        _log(args.debug, "[calibrate] predictions_market.csv não existe. Usando fallback por odds.")

    # 2) Fallback: odds_consensus.csv
    if df_pred is None:
        odds_path = os.path.join(out_dir, "odds_consensus.csv")
        if not os.path.exists(odds_path):
            raise FileNotFoundError(
                "[calibrate] Nenhuma fonte válida: "
                "predictions_market.csv ausente/sem colunas E odds_consensus.csv não existe."
            )
        df_odds = pd.read_csv(odds_path)
        miss_odds = _require_cols(df_odds, ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"])
        if miss_odds:
            raise ValueError(f"[calibrate] odds_consensus.csv está sem colunas obrigatórias: {miss_odds}")
        df_pred = _from_odds_to_probs(df_odds, debug=args.debug)

    # 3) Prepara matriz de probabilidades
    probs = df_pred[["prob_home", "prob_draw", "prob_away"]].astype(float).values
    probs = np.clip(probs, 1e-8, 1 - 1e-8)
    probs = probs / probs.sum(axis=1, keepdims=True)

    # 4) Calibração: Dirichlet com fallback Isotonic
    try:
        calibrated = _dirichlet_calibration(probs)
        method = "Dirichlet"
    except Exception as e:
        _log(args.debug, f"[warn] Dirichlet falhou: {e}. Aplicando Isotonic Regression.")
        calibrated = _isotonic_calibration(probs, debug=args.debug)
        method = "Isotonic"

    # 5) Monta saída
    df_out = pd.DataFrame({
        "match_id": df_pred["match_id"],
        "calib_method": method,
        "calib_home": calibrated[:, 0],
        "calib_draw": calibrated[:, 1],
        "calib_away": calibrated[:, 2],
    })

    out_path = os.path.join(out_dir, "calibrated_probs.csv")
    df_out.to_csv(out_path, index=False)

    if args.debug:
        print(f"[calibrate] Método usado: {method}")
        print(f"[calibrate] Salvo em: {out_path}")
        print(df_out.head(10))

    print("[ok] Calibração concluída com sucesso.")

if __name__ == "__main__":
    main()