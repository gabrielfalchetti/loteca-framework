import argparse, json
from pathlib import Path
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

ROOT = Path(__file__).resolve().parents[1]
PROC = ROOT/"data/processed"
MODELS = ROOT/"models"

def main(rodada):
    X = pd.read_parquet(PROC/"features.parquet")
    y = X["target"] if "target" in X else None
    Xnum = X[[c for c in X.columns if c.startswith("X_")]]

    pipe = Pipeline([
        ("scaler", StandardScaler(with_mean=False)),
        ("clf", LogisticRegression(max_iter=200))
    ])
    if y is not None and y.dropna().size > 0:
        pipe.fit(Xnum.fillna(0), y.fillna(0))
    MODELS.mkdir(parents=True, exist_ok=True)
    (MODELS/"registry.json").write_text(
        json.dumps({"current":"v1","rodada":rodada}, indent=2),
        encoding="utf-8"
    )

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()
    main(args.rodada)
