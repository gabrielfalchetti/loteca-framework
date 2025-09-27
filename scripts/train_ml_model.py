# scripts/train_ml_model.py
import argparse, joblib
import pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history-dir", default="data/history/stats")
    ap.add_argument("--out-model", default="models/ml_model.pkl")
    args = ap.parse_args()

    base = Path(args.history_dir)
    dfs=[]
    for f in base.glob("*/matchstats.csv"):
        df = pd.read_csv(f)
        if "result" not in df.columns:  # precisa col resultado (0,1,2)
            continue
        dfs.append(df)
    if not dfs:
        raise RuntimeError("[train_ml] Nenhum histórico com matchstats.csv + resultado.")

    df = pd.concat(dfs, ignore_index=True)
    y = df["result"].astype(int)
    X = df.drop(columns=["match_id","home","away","result"])

    X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=0.2,random_state=42)

    model = GradientBoostingClassifier(n_estimators=200, max_depth=3)
    model = CalibratedClassifierCV(model, method="isotonic", cv=3)
    model.fit(X_train,y_train)

    Path("models").mkdir(exist_ok=True)
    joblib.dump(model, args.out_model)
    print(f"[train_ml] Modelo salvo -> {args.out_model}")

if __name__ == "__main__":
    main()
