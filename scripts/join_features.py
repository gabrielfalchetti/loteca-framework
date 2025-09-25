#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, warnings
from pathlib import Path
import numpy as np, pandas as pd, yaml
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss

# W&B opcional
USE_WANDB = True
try:
    import wandb
except Exception:
    USE_WANDB = False

def load_cfg():
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

FEATURES_MIN = [
    "home_prob_market","draw_prob_market","away_prob_market",
    "starters_missing","bench_depth","keeper_out","defenders_out","mids_out","forwards_out",
    "temperature_2m","precipitation_probability","precipitation","wind_speed_10m",
]

def ensure_cols(df: pd.DataFrame, cols, fill=0.0):
    for c in cols:
        if c not in df.columns: df[c] = fill
    return df

def prepare_X(df: pd.DataFrame, feat_list=FEATURES_MIN) -> np.ndarray:
    ensure_cols(df, feat_list, fill=0.0)
    return df[feat_list].astype(float).fillna(0.0).to_numpy()

def main(rodada: str):
    cfg = load_cfg()

    # W&B
    if USE_WANDB and os.getenv("WANDB_API_KEY"):
        wandb.login(key=os.getenv("WANDB_API_KEY"))
        run = wandb.init(project="loteca-framework", config={"rodada": rodada})
        wandb.log({"wandb_ping": 1})
    else:
        run = None

    # carregar datasets
    paths = cfg["paths"]
    matches = pd.read_csv(paths["matches_csv"].replace("${rodada}", rodada))
    odds    = pd.read_csv(paths["odds_out"].replace("${rodada}", rodada)) if Path(paths["odds_out"].replace("${rodada}", rodada)).exists() else pd.DataFrame()
    weather = pd.read_csv(paths["weather_out"].replace("${rodada}", rodada)) if Path(paths["weather_out"].replace("${rodada}", rodada)).exists() else pd.DataFrame()
    avail   = pd.read_csv(paths["availability_out"].replace("${rodada}", rodada)) if Path(paths["availability_out"].replace("${rodada}", rodada)).exists() else pd.DataFrame()

    # merge por match_id
    df = matches.copy()
    if not odds.empty:    df = df.merge(odds.rename(columns={"p_home":"home_prob_market","p_draw":"draw_prob_market","p_away":"away_prob_market"}), on="match_id", how="left")
    if not weather.empty: df = df.merge(weather, on="match_id", how="left")
    if not avail.empty:   df = df.merge(avail, on="match_id", how="left")

    joined_out = paths["joined_out"].replace("${rodada}", rodada)
    Path(joined_out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(joined_out, index=False)

    # separar passado/futuro (se existir is_past/result)
    has_past = "is_past" in df.columns
    df_past = df[df["is_past"]==1].copy() if has_past else pd.DataFrame()
    df_future = df[df["is_past"]==0].copy() if has_past else df.copy()
    has_labels = (not df_past.empty) and ("result" in df_past.columns) and df_past["result"].notna().any()

    out = df[["match_id","home","away"]].copy()

    if has_labels:
        ymap = {"H":0,"D":1,"A":2}; ytr = df_past["result"].map(ymap).to_numpy()
        Xtr = prepare_X(df_past); Xte = prepare_X(df_future) if not df_future.empty else np.zeros((0,len(FEATURES_MIN)))
        base = LogisticRegression(max_iter=2000, multi_class="multinomial")
        model = CalibratedClassifierCV(base, method="sigmoid", cv=5)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore"); model.fit(Xtr, ytr)
        try:
            p_tr = model.predict_proba(Xtr); ll = float(log_loss(ytr, p_tr))
            if USE_WANDB and run is not None: wandb.log({"logloss_train": ll})
        except Exception: pass
        if not df_future.empty:
            p = model.predict_proba(Xte)
            fut = df_future[["match_id","home","away"]].copy()
            fut["p_home"],fut["p_draw"],fut["p_away"] = p[:,0],p[:,1],p[:,2]
            out = pd.concat([out, fut], ignore_index=True).drop_duplicates("match_id", keep="last")
        else:
            out["p_home"]=np.nan; out["p_draw"]=np.nan; out["p_away"]=np.nan
    else:
        out["p_home"] = df.get("home_prob_market", pd.Series([np.nan]*len(df)))
        out["p_draw"] = df.get("draw_prob_market", pd.Series([np.nan]*len(df)))
        out["p_away"] = df.get("away_prob_market", pd.Series([np.nan]*len(df)))

    out["context_score"] = out["p_home"].astype(float)*1.0 + out["p_draw"].astype(float)*0.5
    report_path = cfg["paths"]["context_score_out"].replace("${rodada}", rodada)
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(report_path, index=False)
    print(f"[OK] context score → {report_path}")

    if USE_WANDB and run is not None:
        try: wandb.save(report_path)
        except Exception: pass
        try: wandb.finish()
        except Exception: pass

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
