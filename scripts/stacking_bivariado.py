# scripts/stacking_bivariado.py
from __future__ import annotations
import argparse, os, pickle
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.model_selection import train_test_split

def main() -> None:
    ap = argparse.ArgumentParser(description="Stacking bivariado em duas colunas de probabilidade.")
    ap.add_argument("--train_probs_csv", required=True, help="CSV com colunas: y_true, p1, p2")
    ap.add_argument("--outdir", default="data/models/ml_new/stacking")
    ap.add_argument("--test_size", type=float, default=0.2)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_csv(args.train_probs_csv).dropna(subset=["y_true","p1","p2"])
    X = df[["p1","p2"]].astype(float)
    y = df["y_true"].astype(int)

    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=args.test_size, random_state=42, stratify=y)
    meta = LogisticRegression(max_iter=1000)
    meta.fit(Xtr, ytr)
    proba = meta.predict_proba(Xte)[:, 1]

    try:
        print("[stack] logloss:", log_loss(yte, proba))
    except Exception:
        pass
    print("[stack] brier:", brier_score_loss(yte, proba))

    with open(os.path.join(args.outdir, "stack.pkl"), "wb") as f:
        pickle.dump(meta, f)
    print(f"[stack] salvo -> {os.path.join(args.outdir,'stack.pkl')}")

if __name__ == "__main__":
    main()
