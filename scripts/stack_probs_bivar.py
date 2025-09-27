# scripts/stack_probs_bivar.py
# Ensemble 3-fontes (consenso odds + xG Poisson + Dixon-Coles) + calibração isotônica
from __future__ import annotations
import argparse, joblib
from pathlib import Path
import numpy as np
import pandas as pd

def _safe_probs(df, cols):
    P = df[list(cols)].to_numpy(float, copy=True)
    P = np.clip(P, 1e-9, 1.0)
    P = P / P.sum(axis=1, keepdims=True)
    return P

def _apply_isotonic(P: np.ndarray, models) -> np.ndarray:
    if not isinstance(models, dict): return P
    out = P.copy()
    keys = ["1","X","2"]
    for i,k in enumerate(keys):
        kind, mdl = models.get(k, ("identity", None))
        if kind == "isotonic" and mdl is not None:
            out[:,i] = mdl.predict(P[:,i])
    s = out.sum(axis=1, keepdims=True); s[s<=0]=1.0
    return out/s

def main():
    ap = argparse.ArgumentParser(description="Stack odds + xG + Dixon-Coles com calibração")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--w-consensus", type=float, default=0.50)
    ap.add_argument("--w-xg",        type=float, default=0.25)
    ap.add_argument("--w-bivar",     type=float, default=0.25)
    ap.add_argument("--calib-path",  default="models/calib_isotonic.pkl")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    od_path = base/"odds.csv"
    xg_path = base/"xg_features.csv"
    bv_path = base/"xg_bivar.csv"
    out_path = base/"joined_stacked_bivar.csv"

    for p in [od_path, xg_path, bv_path]:
        if not p.exists() or p.stat().st_size==0:
            raise RuntimeError(f"[stack_bivar] arquivo ausente/vazio: {p}")

    od = pd.read_csv(od_path).rename(columns=str.lower)
    xg = pd.read_csv(xg_path).rename(columns=str.lower)
    bv = pd.read_csv(bv_path).rename(columns=str.lower)

    need_od = {"match_id","p_home","p_draw","p_away"}
    need_xg = {"match_id","p1_xg","px_xg","p2_xg"}
    need_bv = {"match_id","p1_bv","px_bv","p2_bv"}
    if not need_od.issubset(od.columns): raise RuntimeError("[stack_bivar] odds.csv sem colunas necessárias")
    if not need_xg.issubset(xg.columns): raise RuntimeError("[stack_bivar] xg_features.csv sem colunas necessárias")
    if not need_bv.issubset(bv.columns): raise RuntimeError("[stack_bivar] xg_bivar.csv sem colunas necessárias")

    df = od.merge(xg[list(need_xg)], on="match_id", how="left").merge(bv[list(need_bv)|{"rho_hat"}], on="match_id", how="left")

    Pco = _safe_probs(df, ["p_home","p_draw","p_away"])
    Pxg = _safe_probs(df, ["p1_xg","px_xg","p2_xg"]) if {"p1_xg","px_xg","p2_xg"}.issubset(df.columns) else np.full_like(Pco, 1/3)
    Pbv = _safe_probs(df, ["p1_bv","px_bv","p2_bv"]) if {"p1_bv","px_bv","p2_bv"}.issubset(df.columns) else np.full_like(Pco, 1/3)

    wc = max(0.0, min(1.0, args.w_consensus))
    wx = max(0.0, min(1.0, args.w_xg))
    wb = max(0.0, min(1.0, args.w_bivar))
    if wc + wx + wb <= 0: wc, wx, wb = 0.5, 0.25, 0.25
    s = wc+wx+wb; wc,wx,wb = wc/s, wx/s, wb/s

    P = wc*Pco + wx*Pxg + wb*Pbv

    models=None
    cp = Path(args.calib_path)
    if cp.exists() and cp.stat().st_size>0:
        try: models = joblib.load(cp)
        except Exception: models=None
    if models: P = _apply_isotonic(P, models)

    out = df.copy()
    out["p_home_final"], out["p_draw_final"], out["p_away_final"] = P[:,0], P[:,1], P[:,2]
    out.to_csv(out_path, index=False)
    print(f"[stack_bivar] OK -> {out_path}")

if __name__ == "__main__":
    main()
