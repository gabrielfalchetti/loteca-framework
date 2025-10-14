# -*- coding: utf-8 -*-
import argparse, os, sys, csv, pickle, pandas as pd
import numpy as np

def _safe_load_cal(path):
    with open(path, "rb") as f:
        return pickle.load(f)

def _apply_iso(ir, p):
    try:
        return float(ir.predict([p])[0])
    except Exception:
        return p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--cal", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.inp)
    cal = _safe_load_cal(args.cal)  # dict: home/draw/away -> IsotonicRegression

    out_rows = []
    for _, r in df.iterrows():
        ph, pd, pa = float(r["p_home"]), float(r["p_draw"]), float(r["p_away"])
        ph2 = _apply_iso(cal.get("home"), ph)
        pd2 = _apply_iso(cal.get("draw"), pd)
        pa2 = _apply_iso(cal.get("away"), pa)
        s = ph2+pd2+pa2
        if s <= 0: ph2,pd2,pa2 = ph,pd,pa; s = ph+pd+pa
        ph2, pd2, pa2 = ph2/s, pd2/s, pa2/s
        out_rows.append([r["match_id"], r["team_home"], r["team_away"], ph2, pd2, pa2])

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id","team_home","team_away","p_home_cal","p_draw_cal","p_away_cal"])
        w.writerows(out_rows)
    print(f"[calibrate] OK -> {args.out} (linhas={len(out_rows)})")

if __name__ == "__main__":
    main()