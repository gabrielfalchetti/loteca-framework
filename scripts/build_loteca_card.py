# -*- coding: utf-8 -*-
import argparse, os, csv, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kelly", required=True)
    ap.add_argument("--consensus", required=True)
    ap.add_argument("--matches", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    k = pd.read_csv(args.kelly)
    wl = pd.read_csv(args.matches)[["match_id","home","away"]].rename(columns={"home":"team_home","away":"team_away"})
    cons = pd.read_csv(args.consensus)[["match_id","odds_home","odds_draw","odds_away"]]

    card = wl.merge(k, on=["match_id","team_home","team_away"], how="left").merge(cons, on="match_id", how="left")
    # escolha: se pick vazio, marca "X" (empate) como placeholder
    card["pick"] = card["pick"].fillna("X")
    card = card.sort_values("match_id")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    card.to_csv(args.out, index=False, encoding="utf-8")
    print(f"[loteca.card] OK -> {args.out}")

if __name__ == "__main__":
    main()