#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
train_calibrator.py
-------------------
Treina calibradores estatísticos (Isotonic Regression) para converter probabilidades
brutas (p_home, p_draw, p_away) em probabilidades calibradas.

Uso (no workflow):
  python -m scripts.train_calibrator \
      --history data/history/results.csv \
      --pred_store data/history/predictions.csv \
      --out_dir data/history/calibration

Entradas:
  - --history: CSV com resultados reais. Deve conter pelo menos:
      * match_id  (opcional mas preferido)
      * team_home, team_away
      * E UMA das opções:
          a) colunas de gols (ex.: home_goals/away_goals, ou home_score/away_score)
          b) coluna 'result' com rótulos {H, D, A} (Home/Draw/Away)
  - --pred_store: CSV com previsões históricas. Deve conter:
      * match_id (opcional, mas facilita o join)
      * team_home, team_away
      * p_home, p_draw, p_away  (probabilidades previstas na época)

Saídas (em --out_dir):
  - calibrator_home.pkl
  - calibrator_draw.pkl
  - calibrator_away.pkl
  * Sempre gerados. Se não houver dados suficientes, grava calibradores-identidade.
  - calibration_summary.json  (resumo)
  - diag_reliability_{home,draw,away}.csv (curvas de confiabilidade binned)

Regras:
  - Faz o merge por match_id quando possível; senão, tenta por nomes de times normalizados.
  - Aplica filtros básicos e validações. Se não houver amostras suficientes, cai em identidade.
  - Isotonic Regression com out_of_bounds='clip' e clamp em [1e-6, 1-1e-6].

Exit codes:
  0  = sucesso
  12 = erro crítico (ex.: arquivos ausentes ou falha de IO)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd

# Dependências do workflow:
from sklearn.isotonic import IsotonicRegression
try:
    import joblib
except Exception:
    joblib = None

try:
    # opcional: ajuda a normalizar nomes de times
    from unidecode import unidecode
except Exception:
    def unidecode(x):  # fallback simples
        return x


# ---------------------------- Utils ---------------------------- #

@dataclass
class IdentityCalibrator:
    """Calibrador identidade (para salvar quando não há dados suficientes)."""
    def predict(self, x):
        x = np.asarray(x, dtype=float)
        return x


def _clamp01(a: np.ndarray, lo: float = 1e-6, hi: float = 1.0 - 1e-6) -> np.ndarray:
    return np.clip(a, lo, hi)


def _norm_team(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unidecode(s)
    s = " ".join(s.split())
    return s


def _infer_result_from_scores(row: pd.Series) -> Optional[str]:
    """Retorna 'H', 'D' ou 'A' a partir de colunas de gols/placar."""
    score_candidates = [
        ("home_goals", "away_goals"),
        ("home_score", "away_score"),
        ("goals_home", "goals_away"),
        ("ft_home_goals", "ft_away_goals"),
    ]
    for hcol, acol in score_candidates:
        if hcol in row and acol in row and pd.notna(row[hcol]) and pd.notna(row[acol]):
            try:
                h = int(row[hcol])
                a = int(row[acol])
            except Exception:
                continue
            if h > a:
                return "H"
            elif h < a:
                return "A"
            else:
                return "D"
    return None


def _require_file(path: str, tag: str):
    if not os.path.exists(path):
        print(f"[calib][CRITICAL] {tag} não encontrado: {path}")
        sys.exit(12)


def _save_joblib(obj, path: str):
    try:
        joblib.dump(obj, path)
    except Exception as e:
        print(f"[calib][CRITICAL] Falha salvando {path}: {e}")
        sys.exit(12)


def _reliability_curve(y_true: np.ndarray, p_pred: np.ndarray, bins: int = 12) -> pd.DataFrame:
    """Curva de confiabilidade (binning simples)."""
    p_pred = _clamp01(np.asarray(p_pred, dtype=float))
    y_true = np.asarray(y_true, dtype=float)

    edges = np.linspace(0.0, 1.0, bins + 1)
    idx = np.digitize(p_pred, edges, right=True)
    idx[idx == 0] = 1
    idx[idx > bins] = bins

    rows = []
    for b in range(1, bins + 1):
        sel = idx == b
        n = int(sel.sum())
        if n == 0:
            rows.append({
                "bin": b,
                "p_low": float(edges[b - 1]),
                "p_high": float(edges[b]),
                "n": 0,
                "p_mean": np.nan,
                "emp_rate": np.nan
            })
        else:
            rows.append({
                "bin": b,
                "p_low": float(edges[b - 1]),
                "p_high": float(edges[b]),
                "n": n,
                "p_mean": float(p_pred[sel].mean()),
                "emp_rate": float(y_true[sel].mean())
            })
    return pd.DataFrame(rows)


# ---------------------------- Core ---------------------------- #

def load_history(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Normaliza nomes de times se existirem:
    for c in ("team_home", "team_away"):
        if c in df.columns:
            df[c + "_norm"] = df[c].map(_norm_team)
    # Infere result se preciso:
    if "result" not in df.columns or df["result"].isna().all():
        df["result"] = df.apply(_infer_result_from_scores, axis=1)
    # Normaliza rótulos
    if "result" in df.columns:
        df["result"] = df["result"].map(lambda r: str(r).strip().upper() if pd.notna(r) else r)
        df.loc[~df["result"].isin(["H", "D", "A"]), "result"] = pd.NA

    return df


def load_predictions(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Normaliza nomes de times (se existirem)
    for c in ("team_home", "team_away"):
        if c in df.columns:
            df[c + "_norm"] = df[c].map(_norm_team)
    # Clamp e renormaliza probabilidades se necessário
    needed = ["p_home", "p_draw", "p_away"]
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"pred_store sem coluna obrigatória: {c}")
    for c in needed:
        df[c] = _clamp01(df[c].astype(float))
    s = df["p_home"] + df["p_draw"] + df["p_away"]
    s = s.replace(0, np.nan)
    df["p_home"] = df["p_home"] / s
    df["p_draw"] = df["p_draw"] / s
    df["p_away"] = df["p_away"] / s
    return df


def merge_history_preds(his: pd.DataFrame, pred: pd.DataFrame) -> pd.DataFrame:
    # Preferência: match_id
    if "match_id" in his.columns and "match_id" in pred.columns:
        df = pd.merge(pred, his, on="match_id", how="inner", suffixes=("_pred", "_his"))
    else:
        # fallback por nomes normalizados
        for c in ("team_home_norm", "team_away_norm"):
            if c not in his.columns or c not in pred.columns:
                raise ValueError("Não foi possível cruzar history e pred_store (faltam match_id e/ou team_*).")
        df = pd.merge(
            pred, his,
            on=["team_home_norm", "team_away_norm"],
            how="inner",
            suffixes=("_pred", "_his")
        )
    return df


def fit_isotonic_or_identity(p_hat: np.ndarray, y: np.ndarray,
                             min_samples: int = 300, min_unique: int = 25) -> Tuple[object, int]:
    """
    Tenta ajustar IsotonicRegression. Se amostra for insuficiente ou valores pouco variados,
    retorna calibrador identidade.
    Retorna (modelo, flag_foi_isotonic).
    """
    p_hat = _clamp01(np.asarray(p_hat, dtype=float))
    y = np.asarray(y, dtype=float)

    ok = (len(p_hat) >= min_samples) and (np.unique(p_hat).size >= min_unique)
    if not ok:
        return IdentityCalibrator(), 0

    try:
        iso = IsotonicRegression(out_of_bounds="clip", y_min=1e-6, y_max=1-1e-6)
        iso.fit(p_hat, y)
        return iso, 1
    except Exception as e:
        print(f"[calib][WARN] IsotonicRegression falhou, usando identidade. Erro: {e}")
        return IdentityCalibrator(), 0


def train_all_calibrators(df: pd.DataFrame, out_dir: str,
                          min_samples: int = 300, min_unique: int = 25) -> Dict[str, dict]:
    """
    A partir do dataframe mesclado (previsões + resultado), treina calibradores
    p/ home, draw e away. Salva os .pkl e retorna um resumo.
    """
    os.makedirs(out_dir, exist_ok=True)

    # Label binário para cada alvo:
    res = df["result"].astype(str).str.upper()
    yH = (res == "H").astype(int).to_numpy()
    yD = (res == "D").astype(int).to_numpy()
    yA = (res == "A").astype(int).to_numpy()

    pH = df["p_home"].to_numpy(dtype=float)
    pD = df["p_draw"].to_numpy(dtype=float)
    pA = df["p_away"].to_numpy(dtype=float)

    # Treina/fallback identidade:
    modelH, flagH = fit_isotonic_or_identity(pH, yH, min_samples, min_unique)
    modelD, flagD = fit_isotonic_or_identity(pD, yD, min_samples, min_unique)
    modelA, flagA = fit_isotonic_or_identity(pA, yA, min_samples, min_unique)

    # Salva modelos (sempre):
    if joblib is None:
        print("[calib][CRITICAL] joblib indisponível — não é possível salvar modelos.")
        sys.exit(12)

    _save_joblib(modelH, os.path.join(out_dir, "calibrator_home.pkl"))
    _save_joblib(modelD, os.path.join(out_dir, "calibrator_draw.pkl"))
    _save_joblib(modelA, os.path.join(out_dir, "calibrator_away.pkl"))

    # Curvas de confiabilidade (útil para acompanhamento):
    relH = _reliability_curve(yH, pH, bins=12)
    relD = _reliability_curve(yD, pD, bins=12)
    relA = _reliability_curve(yA, pA, bins=12)

    relH.to_csv(os.path.join(out_dir, "diag_reliability_home.csv"), index=False)
    relD.to_csv(os.path.join(out_dir, "diag_reliability_draw.csv"), index=False)
    relA.to_csv(os.path.join(out_dir, "diag_reliability_away.csv"), index=False)

    summary = {
        "n_samples": int(len(df)),
        "min_samples": int(min_samples),
        "min_unique": int(min_unique),
        "fitted": {
            "home_isotonic": bool(flagH),
            "draw_isotonic": bool(flagD),
            "away_isotonic": bool(flagA),
        },
        "rates": {
            "emp_home": float(yH.mean()) if len(yH) else None,
            "emp_draw": float(yD.mean()) if len(yD) else None,
            "emp_away": float(yA.mean()) if len(yA) else None,
        }
    }
    with open(os.path.join(out_dir, "calibration_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    return summary


# ---------------------------- Main ---------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="CSV de resultados históricos")
    ap.add_argument("--pred_store", required=True, help="CSV com previsões históricas")
    ap.add_argument("--out_dir", required=True, help="Diretório de saída dos calibradores")
    ap.add_argument("--min_samples", type=int, default=300, help="Amostras mínimas para treinar isotônico")
    ap.add_argument("--min_unique", type=int, default=25, help="Qtde mínima de valores distintos em p para treinar")
    args = ap.parse_args()

    print("============================================")
    print("[calib] TREINANDO CALIBRADORES ISOTÔNICOS")
    print(f"[calib] history     : {args.history}")
    print(f"[calib] pred_store  : {args.pred_store}")
    print(f"[calib] out_dir     : {args.out_dir}")
    print(f"[calib] min_samples : {args.min_samples}")
    print(f"[calib] min_unique  : {args.min_unique}")
    print("============================================")

    # Verificações iniciais
    _require_file(args.history, "history CSV")
    _require_file(args.pred_store, "pred_store CSV")
    os.makedirs(args.out_dir, exist_ok=True)

    # Carrega dados
    try:
        his = load_history(args.history)
    except Exception as e:
        print(f"[calib][CRITICAL] Falha lendo history: {e}")
        return 12

    try:
        pred = load_predictions(args.pred_store)
    except Exception as e:
        print(f"[calib][CRITICAL] Falha lendo pred_store: {e}")
        return 12

    # Merge
    try:
        df = merge_history_preds(his, pred)
    except Exception as e:
        print(f"[calib][CRITICAL] Falha no merge history x pred_store: {e}")
        return 12

    if len(df) == 0:
        print("[calib][CRITICAL] Sem interseção entre history e pred_store.")
        # Ainda assim, grava calibradores-identidade para não quebrar o workflow
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_home.pkl"))
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_draw.pkl"))
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_away.pkl"))
        # E retorna 0 (sucesso “degradado”) para manter a execução,
        # já que calibrate_probs.py lida com identidade.
        print("[calib][NOTICE] Gravados calibradores-identidade por falta de dados.")
        return 0

    # Necessário ter 'result'
    if "result" not in df.columns or df["result"].isna().all():
        print("[calib][CRITICAL] 'result' ausente/indisponível após merge.")
        # gera identidades mesmo assim
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_home.pkl"))
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_draw.pkl"))
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_away.pkl"))
        print("[calib][NOTICE] Gravados calibradores-identidade por falta de labels.")
        return 0

    # Filtra linhas válidas
    need_cols = ["p_home", "p_draw", "p_away", "result"]
    miss = [c for c in need_cols if c not in df.columns]
    if miss:
        print(f"[calib][CRITICAL] Colunas ausentes para treino: {miss}")
        return 12

    df = df.dropna(subset=need_cols)
    if len(df) == 0:
        print("[calib][CRITICAL] Sem linhas válidas após limpeza.")
        # grava identidades
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_home.pkl"))
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_draw.pkl"))
        _save_joblib(IdentityCalibrator(), os.path.join(args.out_dir, "calibrator_away.pkl"))
        print("[calib][NOTICE] Gravados calibradores-identidade (sem dados).")
        return 0

    # Treina e salva
    summary = train_all_calibrators(
        df, args.out_dir,
        min_samples=args.min_samples,
        min_unique=args.min_unique
    )

    print("[ok] Calibradores gerados em:", args.out_dir)
    print("[ok] Resumo:", json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())