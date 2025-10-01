# scripts/calibracao_isotonica.py
from __future__ import annotations
import argparse, os, pickle
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import brier_score_loss

def main() -> None:
    ap = argparse.ArgumentParser(description="Calibração isotônica em probabilidades preditas.")
    ap.add_argument("--train_probs_csv", required=True, help="CSV com colunas: y_true, p_pred")
    ap.add_argument("--outdir", default="data/models/ml_new/calibracao")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_csv(args.train_probs_csv).dropna(subset=["y_true","p_pred"])
    y = df["y_true"].astype(int).values
    p = df["p_pred"].astype(float).values

    # Ajusta a isotônica mapeando p_pred -> p_cal
    iso = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
    p_cal = iso.fit_transform(p, y)
    print("[iso] brier antes:", brier_score_loss(y, p))
    print("[iso] brier depois:", brier_score_loss(y, p_cal))

    with open(os.path.join(args.outdir, "isotonic.pkl"), "wb") as f:
        pickle.dump(iso, f)
    print(f"[iso] salvo -> {os.path.join(args.outdir,'isotonic.pkl')}")

if __name__ == "__main__":
    main()
