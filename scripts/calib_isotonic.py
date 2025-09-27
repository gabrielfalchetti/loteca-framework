# scripts/calib_isotonic.py
# Treina calibração isotônica por classe (1, X, 2) usando o histórico.
from __future__ import annotations
import argparse, joblib
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

CLASSES = ["1","X","2"]

def _prepare_xy(hist: pd.DataFrame):
    """Espera colunas: p_home, p_draw, p_away, resultado ('1'/'X'/'2')."""
    hist = hist.dropna(subset=["p_home","p_draw","p_away","resultado"]).copy()
    hist["resultado"] = hist["resultado"].astype(str).str.upper().str.strip()
    X = {
        "1": hist["p_home"].to_numpy(float),
        "X": hist["p_draw"].to_numpy(float),
        "2": hist["p_away"].to_numpy(float),
    }
    Y = {
        "1": (hist["resultado"]=="1").astype(int).to_numpy(),
        "X": (hist["resultado"]=="X").astype(int).to_numpy(),
        "2": (hist["resultado"]=="2").astype(int).to_numpy(),
    }
    return X, Y

def main():
    ap = argparse.ArgumentParser(description="Calibração isotônica 1/X/2")
    ap.add_argument("--history-path", default="data/history/calibration.csv")
    ap.add_argument("--out-path", default="models/calib_isotonic.pkl")
    args = ap.parse_args()

    hp = Path(args.history_path)
    if not hp.exists() or hp.stat().st_size==0:
        raise RuntimeError(f"[calib] histórico ausente/vazio: {hp}")

    df = pd.read_csv(hp).rename(columns=str.lower)
    need = {"p_home","p_draw","p_away","resultado"}
    if not need.issubset(df.columns):
        raise RuntimeError(f"[calib] histórico sem colunas necessárias: {sorted(need)}")

    X, Y = _prepare_xy(df)
    models={}
    for c in CLASSES:
        x = X[c]; y = Y[c]
        if len(x)==0 or y.sum()==0 or y.sum()==len(y):
            # dados insuficientes -> identidade
            models[c] = ("identity", None)
            continue
        ir = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
        ir.fit(x, y)
        models[c] = ("isotonic", ir)

    Path(args.out_path).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(models, args.out_path)
    print(f"[calib] OK -> {args.out_path}")

if __name__ == "__main__":
    main()
