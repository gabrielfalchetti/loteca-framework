# scripts/xg_univariado.py
from __future__ import annotations
import argparse, os, pickle
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.model_selection import train_test_split

def main() -> None:
    ap = argparse.ArgumentParser(description="Treino univariado (usa apenas uma feature transformada das odds).")
    ap.add_argument("--train", default="data/training/historico.csv")
    ap.add_argument("--outdir", default="data/models/ml_new/univariado")
    ap.add_argument("--target", default="target_home_win")
    ap.add_argument("--test_size", type=float, default=0.2)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_csv(args.train)

    # Feature univariada simples: 'price' do mandante => 1/home_odds (proxy de prob)
    df = df.dropna(subset=["home_odds"])
    X = pd.DataFrame({"inv_home_price": 1.0 / df["home_odds"].astype(float)})
    y = df[args.target].astype(int)

    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=args.test_size, random_state=42, stratify=y)
    clf = HistGradientBoostingClassifier(random_state=42)
    clf.fit(Xtr, ytr)
    proba = clf.predict_proba(Xte)[:, 1]

    # métricas rápidas
    try:
        print("[xg_uni] logloss:", log_loss(yte, proba))
    except Exception:
        pass
    print("[xg_uni] brier:", brier_score_loss(yte, proba))

    with open(os.path.join(args.outdir, "model.pkl"), "wb") as f:
        pickle.dump(clf, f)
    print(f"[xg_uni] salvo -> {os.path.join(args.outdir,'model.pkl')}")

if __name__ == "__main__":
    main()
