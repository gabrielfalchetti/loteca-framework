# scripts/xg_bivariado.py
import argparse, sys, os
import pandas as pd
import numpy as np

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", dest="debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    cons = os.path.join(out_dir, "odds_consensus.csv")
    uni  = os.path.join(out_dir, "xg_univariate.csv")

    if not os.path.exists(cons):
        sys.exit("odds_consensus.csv not found")
    if not os.path.exists(uni):
        sys.exit("xg_univariate.csv not found")

    dfc = pd.read_csv(cons)
    dfu = pd.read_csv(uni)

    # Junta para gerar interações bivariadas coerentes com as odds
    df = dfc.merge(dfu, on=["team_home","team_away"], how="inner")

    # Interações simples: diferenças e razões — não “inventam” dados, só derivam dos existentes
    df["xg_diff_bi"] = df["xg_home_uni"] - df["xg_away_uni"]
    df["xg_ratio_bi"] = np.where(dfu["xg_away_uni"]>0,
                                 dfu["xg_home_uni"] / dfu["xg_away_uni"],
                                 np.nan)

    out = os.path.join(out_dir, "xg_bivariate.csv")
    df[["team_home","team_away","xg_home_uni","xg_away_uni","xg_diff_bi","xg_ratio_bi"]].to_csv(out, index=False)
    if args.debug:
        print(df.head())

if __name__ == "__main__":
    main()