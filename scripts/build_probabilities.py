#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_probabilities.py — consolida probabilidades 1X2 de forma robusta.

Entradas (em ordem de preferência):
  data/out/<RODADA>/preds_bivar.csv        (de stacking bivariado)
  data/out/<RODADA>/preds_calibrated.csv   (de calibração isotônica)
  data/out/<RODADA>/features_base.csv      (fallback)

Saída:
  data/out/<RODADA>/probabilities.csv  (match_id,home,away,p1,px,p2,ts)
"""

from __future__ import annotations
import argparse, os, sys, math
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

BR_TZ = timezone(timedelta(hours=-3))

def _read(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[build_probs] AVISO: falha ao ler {path} -> {e}", file=sys.stderr)
        return pd.DataFrame()

def _select_source(base_dir: str) -> tuple[str, pd.DataFrame]:
    for name in ["preds_bivar.csv", "preds_calibrated.csv", "features_base.csv"]:
        df = _read(os.path.join(base_dir, name))
        if not df.empty:
            return name, df
    return "", pd.DataFrame()

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["match_id","home","away"]:
        if c not in out.columns:
            out[c] = list(range(1, len(out)+1)) if c=="match_id" else ""
    # tenta achar p1/px/p2 ou derivar de odds implícitas
    if not all(c in out.columns for c in ["p1","px","p2"]):
        # tenta odds k1,kx,k2 -> probs
        for c in ["p1","px","p2"]:
            out[c] = out[c] if c in out.columns else np.nan
        if all(c in out.columns for c in ["k1","kx","k2"]):
            def inv(o): 
                try:
                    o = float(o)
                    return 1.0/o if o>0 else np.nan
                except Exception:
                    return np.nan
            out["p1"] = out["p1"].fillna(out["k1"].apply(inv))
            out["px"] = out["px"].fillna(out["kx"].apply(inv))
            out["p2"] = out["p2"].fillna(out["k2"].apply(inv))
    # normaliza
    s = out[["p1","px","p2"]].astype(float).sum(axis=1)
    mask = s > 0
    out.loc[mask,"p1"] = out.loc[mask,"p1"]/s[mask]
    out.loc[mask,"px"] = out.loc[mask,"px"]/s[mask]
    out.loc[mask,"p2"] = out.loc[mask,"p2"]/s[mask]
    # clip de segurança
    for c in ["p1","px","p2"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").clip(lower=0.0, upper=1.0)
    return out[["match_id","home","away","p1","px","p2"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--in", dest="inp", default="")
    ap.add_argument("--odds", default="")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    base_dir = os.path.join("data","out",args.rodada)
    os.makedirs(base_dir, exist_ok=True)
    out_path = args.out or os.path.join(base_dir, "probabilities.csv")

    # escolhe fonte
    src_name, df = ("", pd.DataFrame())
    if args.inp.strip():
        df = _read(args.inp.strip())
        src_name = os.path.basename(args.inp.strip())
    if df.empty:
        src_name, df = _select_source(base_dir)
    if df.empty:
        raise RuntimeError("[build_probs] Nenhuma fonte encontrada (preds_bivar/preds_calibrated/features_base).")

    out = _ensure_cols(df)
    out["ts"] = datetime.now(BR_TZ).isoformat(timespec="seconds")
    out.to_csv(out_path, index=False)
    print(f"[build_probs] Fonte='{src_name}' -> {out_path} ({len(out)} linhas)")

if __name__ == "__main__":
    main()
