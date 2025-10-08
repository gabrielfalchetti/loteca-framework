#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
blend_models.py (v2)
- Blend market + calib
- (opcional) Ajuste contextual com context_features.csv (uni/bivariado/xg + weather + injuries + news)
Saídas:
- predictions_blend.csv (sem contexto)
- predictions_final.csv (com contexto, se --use-context true)
"""

from __future__ import annotations
import argparse
import pandas as pd
import numpy as np
from pathlib import Path

def load_market(rodada: Path) -> pd.DataFrame:
    c = pd.read_csv(rodada / "odds_consensus.csv")
    c["ih"] = 1.0 / c["odds_home"]; c["ix"] = 1.0 / c["odds_draw"]; c["ia"] = 1.0 / c["odds_away"]
    s = c["ih"] + c["ix"] + c["ia"]
    c["m_home"] = c["ih"] / s; c["m_draw"] = c["ix"] / s; c["m_away"] = c["ia"] / s
    out = c[["match_id","team_home","team_away","m_home","m_draw","m_away"]].copy()
    out = out.rename(columns={"team_home":"home","team_away":"away"})
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

def load_context(rodada: Path) -> pd.DataFrame | None:
    p = rodada / "context_features.csv"
    if not p.exists() or p.stat().st_size == 0:
        return None
    df = pd.read_csv(p)
    return df[["match_id","context_score","home","away"]].copy()

def blend_market_calib(market: pd.DataFrame, calib: pd.DataFrame | None,
                       w_market: float, w_calib: float) -> pd.DataFrame:
    if calib is None:
        out = market.copy()
        out["p_home"] = out["m_home"]
        out["p_draw"] = out["m_draw"]
        out["p_away"] = out["m_away"]
        out["used_sources"] = "market"
        out["weights"] = "market:1.00"
        return out[["match_id","home","away","p_home","p_draw","p_away","used_sources","weights"]]

    df = market.merge(calib, on="match_id", how="left")
    df["p_home"] = df["m_home"] * w_market + df["c_home"].fillna(df["m_home"]) * w_calib
    df["p_draw"] = df["m_draw"] * w_market + df["c_draw"].fillna(df["m_draw"]) * w_calib
    df["p_away"] = df["m_away"] * w_market + df["c_away"].fillna(df["m_away"]) * w_calib
    out = df[["match_id","home","away","p_home","p_draw","p_away"]].copy()
    out["used_sources"] = "market+calib"
    out["weights"] = f"market:{w_market:.2f};calib:{w_calib:.2f}"
    return out

def apply_context_adjust(df: pd.DataFrame, ctx: pd.DataFrame, strength: float = 0.15) -> pd.DataFrame:
    """
    Ajuste contextual suave:
    - context_score in [-1, 1] (positivo favorece HOME, negativo favorece AWAY)
    - strength: quanto do deslocamento aplicar (0.15 = 15% de "pull")
    Estratégia: re-pondera p_home e p_away em direção ao lado favorecido,
    preservando p_draw parcialmente via re-normalização.
    """
    z = df.merge(ctx[["match_id","context_score"]], on="match_id", how="left")
    z["context_score"] = z["context_score"].fillna(0.0).clip(-1.0, 1.0)

    # deslocamento direcionado
    # delta = strength * score * min(p_home, p_away)  (para não explodir)
    min_edge = np.minimum(z["p_home"], z["p_away"])
    delta = strength * z["context_score"] * min_edge

    # aplica: se score>0 puxa para home; se <0, puxa para away
    z["p_home_adj"] = (z["p_home"] + np.maximum(delta, 0.0)).clip(1e-6, 1-1e-6)
    z["p_away_adj"] = (z["p_away"] + np.maximum(-delta, 0.0)).clip(1e-6, 1-1e-6)

    # mantém draw como "reservatório" re-normalizando
    s = z["p_home_adj"] + z["p_away_adj"] + z["p_draw"]
    z["p_home_adj"] = z["p_home_adj"] / s
    z["p_draw_adj"] = z["p_draw"]     / s
    z["p_away_adj"] = z["p_away_adj"] / s

    out = z[["match_id","home","away","p_home_adj","p_draw_adj","p_away_adj","used_sources","weights","context_score"]].copy()
    out = out.rename(columns={"p_home_adj":"p_home","p_draw_adj":"p_draw","p_away_adj":"p_away"})
    out["used_sources"] = out["used_sources"] + "+context"
    out["weights"] = out["weights"] + f";context:{strength:.2f}"
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--w_calib", type=float, default=0.65)
    ap.add_argument("--w_market", type=float, default=0.35)
    ap.add_argument("--use-context", type=str, default="false", help="true/false")
    ap.add_argument("--context-strength", type=float, default=0.15)
    args = ap.parse_args()
    rodada = Path(args.rodada)

    print(f"[blend] rodada: {rodada}")

    market = load_market(rodada)
    calib = load_calib(rodada)

    # 1) Blend base
    out = blend_market_calib(market, calib, w_market=args.w_market, w_calib=args.w_calib)
    out.sort_values("match_id").to_csv(rodada / "predictions_blend.csv", index=False)
    print(f"[blend] OK -> {rodada/'predictions_blend.csv'}")

    # 2) Context (opcional)
    use_ctx = str(args.use_context).lower().strip() in ("1","true","yes","y")
    if use_ctx:
        ctx = load_context(rodada)
        if ctx is None or ctx.empty:
            print("[blend] context_features.csv ausente ou vazio — mantendo somente blend.")
            out_final = out.copy()
        else:
            out_final = apply_context_adjust(out, ctx, strength=args.context_strength)
    else:
        out_final = out.copy()

    out_final.sort_values("match_id").to_csv(rodada / "predictions_final.csv", index=False)
    print(f"[blend] OK -> {rodada/'predictions_final.csv'}")
    print(out_final.head().to_string(index=False))

if __name__ == "__main__":
    main()
