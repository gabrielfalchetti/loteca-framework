# scripts/calibrate_probs_isotonic.py
# Calibração multiclasse via Isotonic Regression (one-vs-rest) sobre as probabilidades 1/X/2.
# - Usa data/history/calibration.csv para aprender 3 funções isotônicas (1, X, 2)
# - Aplica no arquivo joined* da rodada e gera joined_calibrated_iso.csv
# Observações:
#   • Para cada classe k, ajusta P(y=k | p_k) ≈ f_k(p_k), depois renormaliza por linha.
#   • Funciona mesmo que a distribuição esteja mal calibrada (não assume forma paramétrica).
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

def _pick_joined(base: Path) -> Path:
    for name in ["joined_referee.csv","joined_weather.csv","joined_enriched.csv","joined.csv"]:
        p = base / name
        if p.exists() and p.stat().st_size > 0:
            return p
    raise RuntimeError("[iso] nenhum joined* encontrado.")

def _ensure_probs(df: pd.DataFrame) -> np.ndarray:
    if {"p_home","p_draw","p_away"}.issubset(df.columns):
        P = df[["p_home","p_draw","p_away"]].values.astype(float)
    else:
        need = ["odd_home","odd_draw","odd_away"]
        if not set(need).issubset(df.columns):
            raise RuntimeError("[iso] joined sem p_* e sem odds_*.")
        arr = df[need].values.astype(float)
        with np.errstate(divide="ignore", invalid="ignore"):
            inv = 1.0/arr
        inv[~np.isfinite(inv)] = 0.0
        s = inv.sum(axis=1, keepdims=True)
        P = np.divide(inv, np.where(s>0, s, 1.0))
    # sanidade
    P = np.clip(P, 1e-9, 1.0)
    P = P / P.sum(axis=1, keepdims=True)
    return P

def main():
    ap = argparse.ArgumentParser(description="Calibração por Isotonic Regression (multiclasse 1/X/2)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--history-path", default="data/history/calibration.csv")
    ap.add_argument("--min-samples", type=int, default=200, help="mínimo de amostras no histórico para calibrar")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    src = _pick_joined(base)
    df = pd.read_csv(src)

    hist = Path(args.history_path)
    if not hist.exists() or hist.stat().st_size == 0:
        # sem histórico: só copia
        out = base/"joined_calibrated_iso.csv"
        df.to_csv(out, index=False)
        print("[iso] histórico ausente; cópia salva sem calibração.")
        return

    H = pd.read_csv(hist)
    need = {"p_home","p_draw","p_away","resultado"}
    if not need.issubset(H.columns) or len(H) < args.min_samples:
        out = base/"joined_calibrated_iso.csv"
        df.to_csv(out, index=False)
        print(f"[iso] histórico insuficiente/indisponível (n={len(H)}); cópia salva sem calibração.")
        return

    # prepara histórico
    P_hist = H[["p_home","p_draw","p_away"]].values.astype(float)
    y_map = {"1":0,"X":1,"2":2}
    y_idx = np.array([y_map[str(v).upper()] for v in H["resultado"].values], dtype=int)
    # alvos one-vs-rest
    Y1 = (y_idx == 0).astype(float)
    YX = (y_idx == 1).astype(float)
    Y2 = (y_idx == 2).astype(float)

    # fit isotônicas
    try:
        from sklearn.isotonic import IsotonicRegression
    except Exception:
        raise RuntimeError("[iso] scikit-learn não instalado. Adicione 'scikit-learn' nas dependências.")

    iso1 = IsotonicRegression(out_of_bounds="clip"); iso1.fit(P_hist[:,0], Y1)
    isox = IsotonicRegression(out_of_bounds="clip"); isox.fit(P_hist[:,1], YX)
    iso2 = IsotonicRegression(out_of_bounds="clip"); iso2.fit(P_hist[:,2], Y2)

    # aplica no atual
    P_now = _ensure_probs(df)
    q1 = np.clip(iso1.predict(P_now[:,0]), 1e-9, 1.0)
    qx = np.clip(isox.predict(P_now[:,1]), 1e-9, 1.0)
    q2 = np.clip(iso2.predict(P_now[:,2]), 1e-9, 1.0)
    Q = np.vstack([q1,qx,q2]).T
    Q = np.clip(Q, 1e-9, 1.0)
    Q = Q / Q.sum(axis=1, keepdims=True)

    # escreve e gera odds coerentes
    df["p_home"], df["p_draw"], df["p_away"] = Q[:,0], Q[:,1], Q[:,2]
    df["odd_home"] = 1.0/np.clip(df["p_home"], 1e-9, 1.0)
    df["odd_draw"] = 1.0/np.clip(df["p_draw"], 1e-9, 1.0)
    df["odd_away"] = 1.0/np.clip(df["p_away"], 1e-9, 1.0)

    out = base/"joined_calibrated_iso.csv"
    df.to_csv(out, index=False)
    print(f"[iso] calibração isotônica aplicada. Saída: {out}")

if __name__ == "__main__":
    main()
