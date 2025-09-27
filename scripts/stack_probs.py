# scripts/stack_probs.py
# Ensemble (consenso odds + xG) + calibração isotônica opcional
from __future__ import annotations
import argparse, joblib
from pathlib import Path
import numpy as np
import pandas as pd

def _safe_probs(df, cols):
    P = df[list(cols)].to_numpy(float, copy=True)
    P = np.clip(P, 1e-9, 1.0)
    P /= P.sum(axis=1, keepdims=True)
    return P

def _apply_isotonic(P: np.ndarray, models) -> np.ndarray:
    """Aplica isotônica por classe (1,X,2); se não tiver, devolve P."""
    if not isinstance(models, dict): return P
    out = P.copy()
    keys = ["1","X","2"]
    for i,k in enumerate(keys):
        kind, mdl = models.get(k, ("identity", None))
        if kind == "isotonic" and mdl is not None:
            out[:,i] = mdl.predict(P[:,i])
        # identity -> mantém
    # re-normaliza
    s = out.sum(axis=1, keepdims=True)
    s[s<=0] = 1.0
    return out / s

def main():
    ap = argparse.ArgumentParser(description="Stacking de probabilidades com calibração opcional")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--w-consensus", type=float, default=0.6, help="peso para consenso de odds")
    ap.add_argument("--w-xg", type=float, default=0.4, help="peso para modelo xG")
    ap.add_argument("--calib-path", default="models/calib_isotonic.pkl", help="arquivo de modelos isotônicos (opcional)")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    odds_path = base/"odds.csv"
    xg_path   = base/"xg_features.csv"
    out_path  = base/"joined_stacked.csv"

    if not odds_path.exists() or odds_path.stat().st_size==0:
        raise RuntimeError(f"[stack] odds.csv ausente/vazio: {odds_path}")
    if not xg_path.exists() or xg_path.stat().st_size==0:
        raise RuntimeError(f"[stack] xg_features.csv ausente/vazio: {xg_path}")

    od = pd.read_csv(odds_path).rename(columns=str.lower)
    xg = pd.read_csv(xg_path).rename(columns=str.lower)

    # espera-se: odds.csv tem p_home,p_draw,p_away (consenso já ajustado), e match_id/home/away
    need_od = {"match_id","home","away","p_home","p_draw","p_away"}
    if not need_od.issubset(od.columns):
        raise RuntimeError(f"[stack] odds.csv sem colunas necessárias: {sorted(need_od)}")

    need_xg = {"match_id","p1_xg","px_xg","p2_xg"}
    if not need_xg.issubset(xg.columns):
        raise RuntimeError(f"[stack] xg_features.csv sem colunas necessárias: {sorted(need_xg)}")

    merged = od.merge(xg[["match_id","p1_xg","px_xg","p2_xg"]], on="match_id", how="left")
    # faltantes -> fallback 1/3
    for c in ["p1_xg","px_xg","p2_xg"]:
        if c not in merged or merged[c].isna().all():
            merged[c] = 1/3
    Pxg = _safe_probs(merged, ["p1_xg","px_xg","p2_xg"])
    Pco = _safe_probs(merged, ["p_home","p_draw","p_away"])

    wc = max(0.0, min(1.0, args.w_consensus))
    wx = max(0.0, min(1.0, args.w_xg))
    if wc + wx <= 0: wc, wx = 0.6, 0.4
    wsum = wc + wx
    wc, wx = wc/wsum, wx/wsum

    P = wc * Pco + wx * Pxg

    # calibração isotônica opcional
    models = None
    cp = Path(args.calib_path)
    if cp.exists() and cp.stat().st_size>0:
        try:
            models = joblib.load(cp)
        except Exception:
            models = None
    if models:
        P = _apply_isotonic(P, models)

    out = merged.copy()
    out["p_home_final"], out["p_draw_final"], out["p_away_final"] = P[:,0], P[:,1], P[:,2]
    out.to_csv(out_path, index=False)
    print(f"[stack] OK -> {out_path}")

if __name__ == "__main__":
    main()
