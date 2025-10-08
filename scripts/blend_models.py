#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Faz blend entre probabilidades de mercado (implied do odds_consensus) e calibradas.
"""

from __future__ import annotations
import argparse
import pandas as pd
from pathlib import Path
import os

def load_market(rodada: Path) -> pd.DataFrame:
    c = pd.read_csv(rodada / "odds_consensus.csv")
    # implied probabilities (proporcional)
    c["ih"] = 1.0 / c["odds_home"]; c["ix"] = 1.0 / c["odds_draw"]; c["ia"] = 1.0 / c["odds_away"]
    s = c["ih"] + c["ix"] + c["ia"]
    c["m_home"] = c["ih"] / s; c["m_draw"] = c["ix"] / s; c["m_away"] = c["ia"] / s
    out = c[["match_id","team_home","team_away","m_home","m_draw","m_away"]].copy()
    return out

def load_calib(rodada: Path) -> pd.DataFrame | None:
    p = rodada / "calibrated_probs.csv"
    if not p.exists(): return None
    df = pd.read_csv(p)
    # normaliza headers
    cols = {c.lower(): c for c in df.columns}
    def pick(*names): 
        for n in names:
            if n in df.columns: return n
        for n in names:
            ln = n.lower()
            if ln in cols: return cols[ln]
        return None
    mh = pick("p_home","calib_home"); md = pick("p_draw","calib_draw"); ma = pick("p_away","calib_away")
    mid = pick("match_id")
    if not all([mh,md,ma,mid]): return None
    keep = df[[mid, mh, md, ma]].copy()
    keep.columns = ["match_id","c_home","c_draw","c_away"]
    return keep

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--w_calib", type=float, default=0.65)
    ap.add_argument("--w_market", type=float, default=0.35)
    args = ap.parse_args()
    rodada = Path(args.rodada)

    print(f"[blend] rodada: {rodada}")
    market = load_market(rodada)
    print("[blend] base de nomes vinda de odds_consensus.csv")
    print("[blend] market derivado de odds_consensus (implied)")
    print(market[["match_id","team_home","team_away","m_home","m_draw","m_away"]].head().to_string(index=False))

    calib = load_calib(rodada)
    used = "market"
    if calib is None:
        print(f"[blend] arquivo ausente: {rodada/'calibrated_probs.csv'}")
    else:
        used = "market+calib"

    if calib is None:
        out = market.copy()
        out.rename(columns={"m_home":"p_home","m_draw":"p_draw","m_away":"p_away"}, inplace=True)
        out["used_sources"] = "market"
        out["weights"] = "market:1.00"
    else:
        df = market.merge(calib, on="match_id", how="left")
        # onde calib não existir, usa market puro
        df["p_home"] = df["m_home"] * args.w_market + df["c_home"].fillna(df["m_home"]) * args.w_calib
        df["p_draw"] = df["m_draw"] * args.w_market + df["c_draw"].fillna(df["m_draw"]) * args.w_calib
        df["p_away"] = df["m_away"] * args.w_market + df["c_away"].fillna(df["m_away"]) * args.w_calib
        out = df[["match_id","team_home","team_away","p_home","p_draw","p_away"]].copy()
        out["used_sources"] = used
        out["weights"] = f"market:{args.w_market:.2f};calib:{args.w_calib:.2f}"

    out = out.sort_values("match_id")
    out.to_csv(rodada / "predictions_blend.csv", index=False)
    print(f"[blend] OK -> {rodada/'predictions_blend.csv'}")
    print(out.head().to_string(index=False))

if __name__ == "__main__":
    main()
