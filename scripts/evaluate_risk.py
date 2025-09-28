#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_risk.py — compara probabilities.csv com odds.csv e calcula edge/kelly.

Entradas:
  data/out/<RODADA>/probabilities.csv  (p1,px,p2)
  data/out/<RODADA>/odds.csv           (k1,kx,k2)

Saída:
  data/out/<RODADA>/risk_report.csv
"""

from __future__ import annotations
import argparse, os, sys, math
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

BR_TZ = timezone(timedelta(hours=-3))

def _norm(s: str) -> str:
    import unicodedata
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip().replace(".", " ")
    for suf in [" fc"," afc"," ac"," sc","-sp","-rj"," ec"," e.c."]:
        s = s.replace(suf, "")
    return " ".join(s.split())

def _read(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    return pd.read_csv(path)

def _kelly(p: float, odds_dec: float) -> float:
    # Kelly para odds decimais: b = odds-1; f* = (bp - q)/b
    try:
        b = float(odds_dec) - 1.0
        p = float(p)
        q = 1.0 - p
        if b <= 0: return 0.0
        f = (b*p - q) / b
        return max(0.0, float(f))
    except Exception:
        return 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--probs", required=True)
    ap.add_argument("--odds", required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--kelly_fraction", type=float, default=0.25, help="fração da Kelly (ex.: 0.25)")
    args = ap.parse_args()

    base_dir = os.path.join("data","out",args.rodada)
    os.makedirs(base_dir, exist_ok=True)
    out_path = args.out or os.path.join(base_dir, "risk_report.csv")

    probs = _read(args.probs)
    odds  = _read(args.odds)

    # chaves p/ merge (normalizadas e também literal como fallback)
    for df in (probs, odds):
        if "home_n" not in df.columns: df["home_n"] = df.get("home","").apply(_norm)
        if "away_n" not in df.columns: df["away_n"] = df.get("away","").apply(_norm)

    df = probs.merge(odds, on=["home_n","away_n"], how="left", suffixes=("","_odds"))

    # garante cols
    for c in ["p1","px","p2","k1","kx","k2"]:
        if c not in df.columns: df[c] = np.nan

    # edges e kelly
    rows = []
    for _, r in df.iterrows():
        rec = {
            "home": r.get("home",""),
            "away": r.get("away",""),
            "p1": r.get("p1", np.nan),
            "px": r.get("px", np.nan),
            "p2": r.get("p2", np.nan),
            "k1": r.get("k1", np.nan),
            "kx": r.get("kx", np.nan),
            "k2": r.get("k2", np.nan),
        }
        for outc, (p, k) in {
            "1": (rec["p1"], rec["k1"]),
            "X": (rec["px"], rec["kx"]),
            "2": (rec["p2"], rec["k2"]),
        }.items():
            try:
                p = float(p); k = float(k)
                imp = 1.0/k if k>0 else np.nan
                edge = p - imp if not (math.isnan(p) or math.isnan(imp)) else np.nan
                kelly = _kelly(p, k)
                stake = args.kelly_fraction * kelly  # fração da Kelly
            except Exception:
                imp = edge = kelly = stake = np.nan
            rec[f"imp_{outc}"] = imp
            rec[f"edge_{outc}"] = edge
            rec[f"kelly_{outc}"] = kelly
            rec[f"stake_{outc}"] = stake
        rows.append(rec)

    out = pd.DataFrame(rows, columns=[
        "home","away","p1","px","p2","k1","kx","k2",
        "imp_1","edge_1","kelly_1","stake_1",
        "imp_X","edge_X","kelly_X","stake_X",
        "imp_2","edge_2","kelly_2","stake_2",
    ])
    out["ts"] = datetime.now(BR_TZ).isoformat(timespec="seconds")
    out.to_csv(out_path, index=False)
    print(f"[risk] OK -> {out_path} ({len(out)} linhas)")

if __name__ == "__main__":
    main()
