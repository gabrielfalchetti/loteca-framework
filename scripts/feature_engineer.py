# -*- coding: utf-8 -*-
import argparse, sys, os
import pandas as pd
import numpy as np

def ewma_features(df, span=5):
    # formato: date,home,away,home_goals,away_goals
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    # long
    home = df[["date","home","home_goals","away_goals"]].rename(
        columns={"home":"team","home_goals":"gf","away_goals":"ga"}
    )
    away = df[["date","away","home_goals","away_goals"]].rename(
        columns={"away":"team","away_goals":"gf","home_goals":"ga"}
    )
    df_long = pd.concat([home, away], ignore_index=True)
    df_long["match"] = 1
    df_long["pts"] = np.where(df_long["gf"]>df_long["ga"],3,np.where(df_long["gf"]==df_long["ga"],1,0))

    def _ewm_grp(g):
        g = g.sort_values("date")
        e = g[["gf","ga","match","pts"]].ewm(alpha=1.0/span, adjust=False).mean()
        e["gf_ma"] = g["gf"].rolling(5, min_periods=1).mean()
        e["ga_ma"] = g["ga"].rolling(5, min_periods=1).mean()
        e["pts_ma"] = g["pts"].rolling(5, min_periods=1).mean()
        out = g[["date","team"]].join(e)
        return out

    out = df_long.groupby("team", group_keys=False).apply(_ewm_grp)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ewma", type=float, default=0.20)
    args = ap.parse_args()

    df = pd.read_csv(args.history)
    if df.empty or df.shape[0] <= 0:
        print("[features][CRITICAL] history is empty")
        sys.exit(2)

    feats = ewma_features(df, span=int(round(1.0/args.ewma)))  # alpha ~ 1/span
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    feats.to_parquet(args.out, index=False)
    print("[features][INFO] schema detectado: date=date home=home away=away gf=gf ga=ga xg_home=- xg_away=-")
    print(f"[features] gravado {args.out}")

if __name__ == "__main__":
    main()