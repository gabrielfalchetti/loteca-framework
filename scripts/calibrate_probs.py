# scripts/calibrate_probs.py
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

def _softmax(z):
    z = z - np.max(z, axis=1, keepdims=True)
    e = np.exp(z)
    return e / e.sum(axis=1, keepdims=True)

def _logit(p):
    p = np.clip(p, 1e-9, 1-1e-9)
    return np.log(p)

def _nll(probs, y_idx):
    p = np.clip(probs[np.arange(len(y_idx)), y_idx], 1e-12, 1.0)
    return float(-np.mean(np.log(p)))

def _apply_temperature(p, T):
    logits = _logit(p)
    logits_T = logits / T
    return _softmax(logits_T)

def fit_temperature(p_hist: np.ndarray, y_hist: np.ndarray, grid=None):
    if grid is None:
        grid = np.linspace(0.5, 2.0, 31)
    y_map = {"1":0,"X":1,"2":2}
    y_idx = np.array([y_map[str(v).upper()] for v in y_hist], dtype=int)
    bestT = 1.0; bestNLL = 1e9
    for T in grid:
        pT = _apply_temperature(p_hist, T)
        nll = _nll(pT, y_idx)
        if nll < bestNLL:
            bestNLL, bestT = nll, float(T)
    return bestT, bestNLL

def main():
    ap = argparse.ArgumentParser(description="Calibração de probabilidades por temperature scaling")
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    src = None
    for name in ["joined_referee.csv","joined_weather.csv","joined_enriched.csv","joined.csv"]:
        p = base/name
        if p.exists() and p.stat().st_size>0:
            src = p; break
    if src is None:
        raise RuntimeError("[calib] nenhum joined* encontrado.")

    df = pd.read_csv(src)

    # p_* presentes? se não, derive de odds
    if set(["p_home","p_draw","p_away"]).issubset(df.columns):
        P_now = df[["p_home","p_draw","p_away"]].values.astype(float)
    else:
        need = ["odd_home","odd_draw","odd_away"]
        if not set(need).issubset(df.columns):
            raise RuntimeError("[calib] joined sem p_* e sem odds_*.")
        arr = df[need].values.astype(float)
        with np.errstate(divide="ignore", invalid="ignore"):
            inv = 1.0/arr
        inv[~np.isfinite(inv)] = 0.0
        s = inv.sum(axis=1, keepdims=True)
        P_now = np.divide(inv, np.where(s>0, s, 1.0))

    hist = Path("data/history/calibration.csv")
    if not hist.exists() or hist.stat().st_size==0:
        # sem histórico: só copia
        out = base/"joined_calibrated.csv"
        df.to_csv(out, index=False)
        print("[calib] histórico ausente; cópia salva sem calibração.")
        return

    H = pd.read_csv(hist)
    need = {"p_home","p_draw","p_away","resultado"}
    if not need.issubset(H.columns):
        out = base/"joined_calibrated.csv"
        df.to_csv(out, index=False)
        print("[calib] histórico inválido; cópia salva sem calibração.")
        return

    P_hist = H[["p_home","p_draw","p_away"]].values.astype(float)
    y_hist = H["resultado"].values
    T, nll_star = fit_temperature(P_hist, y_hist)

    P_cal = _apply_temperature(P_now, T)
    df["p_home"] = P_cal[:,0]
    df["p_draw"] = P_cal[:,1]
    df["p_away"] = P_cal[:,2]
    # odds coerentes
    df["odd_home"] = 1.0/np.clip(df["p_home"], 1e-9, 1.0)
    df["odd_draw"] = 1.0/np.clip(df["p_draw"], 1e-9, 1.0)
    df["odd_away"] = 1.0/np.clip(df["p_away"], 1e-9, 1.0)

    out = base/"joined_calibrated.csv"
    df.to_csv(out, index=False)
    print(f"[calib] T*={T:.3f} aplicado. Saída: {out}")

if __name__ == "__main__":
    main()
