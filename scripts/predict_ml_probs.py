# scripts/predict_ml_probs.py
import argparse, joblib
import pandas as pd
from pathlib import Path
import numpy as np

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--model-path", default="models/ml_model.pkl")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    stats_path = base / "matchstats.csv"
    if not stats_path.exists():
        raise RuntimeError(f"[ml_predict] matchstats.csv ausente: {stats_path}")

    df = pd.read_csv(stats_path)
    model = joblib.load(args.model_path)

    X = df.drop(columns=["match_id","home","away"], errors="ignore")
    probs = model.predict_proba(X)

    out = df[["match_id","home","away"]].copy()
    out["p_home_ml"], out["p_draw_ml"], out["p_away_ml"] = probs[:,0], probs[:,1], probs[:,2]
    out.to_csv(base/"ml_probs.csv", index=False)
    print(f"[ml_predict] OK -> {base/'ml_probs.csv'}")

if __name__ == "__main__":
    main()
