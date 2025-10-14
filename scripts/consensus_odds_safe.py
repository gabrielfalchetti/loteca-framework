# -*- coding: utf-8 -*-
import argparse, os, sys, csv, pandas as pd
from _utils_norm import norm_name

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--strict", action="store_true")
    args = ap.parse_args()

    base = args.rodada
    wl = os.path.join(base, "matches_whitelist.csv")
    th = os.path.join(base, "odds_theoddsapi.csv")
    af = os.path.join(base, "odds_apifootball.csv")
    out = os.path.join(base, "odds_consensus.csv")

    wl_df = pd.read_csv(wl)
    wl_df["home_norm"] = wl_df["home"].astype(str).map(norm_name)
    wl_df["away_norm"] = wl_df["away"].astype(str).map(norm_name)

    def safe_read(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away"])

    th_df = safe_read(th)
    af_df = safe_read(af)

    src = pd.concat([th_df, af_df], ignore_index=True)
    if src.empty:
        print("[consensus]fontes: theodds=0  apifoot=0")
    else:
        print(f"[consensus]fontes: theodds={(len(th_df)>0)}  apifoot={(len(af_df)>0)}")

    # normalizar
    if not src.empty:
        src["home_norm"] = src["team_home"].astype(str).map(norm_name)
        src["away_norm"] = src["team_away"].astype(str).map(norm_name)
        # agregação por mediana
        agg = src.groupby(["home_norm","away_norm"], as_index=False).agg({
            "odds_home":"median","odds_draw":"median","odds_away":"median"
        })
    else:
        agg = src

    merged = wl_df.merge(agg, how="left", on=["home_norm","away_norm"])
    missing = merged[merged[["odds_home","odds_away"]].isna().any(axis=1)]
    if args.strict and not missing.empty:
        print("##[error][CRITICAL] Jogos sem odds após consenso (modo estrito ligado).", file=sys.stderr)
        print(missing[["match_id","home","away"]].to_string(index=False), file=sys.stderr)
        sys.exit(6)

    merged.rename(columns={"home":"team_home","away":"team_away"}, inplace=True)
    merged[["match_id","team_home","team_away","odds_home","odds_draw","odds_away"]].to_csv(out, index=False, encoding="utf-8")
    print(f"[consensus]OK -> {out}")

if __name__ == "__main__":
    main()