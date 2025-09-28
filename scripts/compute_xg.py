#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
compute_xg.py — gera xG por jogo de forma resiliente.

Entradas:
  data/out/<RODADA>/features_base.csv (opcional)
Saída:
  data/out/<RODADA>/xg.csv  (match_id,home,away,xg_home,xg_away,ts)
"""

from __future__ import annotations
import argparse, os, sys, math
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

BR_TZ = timezone(timedelta(hours=-3))

def _safe_read(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        print(f"[xg] AVISO: ausente {path} — usando defaults.", file=sys.stderr)
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[xg] AVISO: falha ao ler {path} -> {e}", file=sys.stderr)
        return pd.DataFrame()

def _default_xg_row(row) -> tuple[float,float]:
    # defaults conservadores (média BR)
    return 1.35, 1.15

def _xg_from_probs(row) -> tuple[float,float]:
    """Se existirem p1/px/p2 na base, gere um chute coerente para λ_home/λ_away."""
    p1 = row.get("p1", np.nan)
    px = row.get("px", np.nan)
    p2 = row.get("p2", np.nan)
    if any(map(lambda v: pd.isna(v), [p1,px,p2])):
        return _default_xg_row(row)
    # mapeamento simples: favoritismo -> assimetria de gols
    strength = (p1 - p2)  # [-1,1]
    base = 2.50  # gols médios de jogo
    spread = 0.60 * strength  # desloca entre -0.6 e +0.6
    hg = (base/2.0) + spread/2.0
    ag = (base/2.0) - spread/2.0
    # pisos
    hg = float(max(0.5, min(2.5, hg)))
    ag = float(max(0.5, min(2.5, ag)))
    return hg, ag

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    feats_path = os.path.join(out_dir,"features_base.csv")
    out_path = args.out or os.path.join(out_dir,"xg.csv")

    base = _safe_read(feats_path)
    if base.empty:
        # tenta cair em matches.csv
        m = _safe_read(os.path.join(out_dir,"matches.csv"))
        if m.empty:
            raise RuntimeError("[xg] Sem base para calcular — faltam features_base.csv e matches.csv.")
        base = m.copy()

    # garantias mínimas
    for c in ["match_id","home","away"]:
        if c not in base.columns:
            base[c] = list(range(1, len(base)+1)) if c=="match_id" else ""

    rows = []
    for _, r in base.iterrows():
        try:
            hg, ag = _xg_from_probs(r)
        except Exception:
            hg, ag = _default_xg_row(r)
        rows.append({
            "match_id": r.get("match_id", _),
            "home": r.get("home",""),
            "away": r.get("away",""),
            "xg_home": hg,
            "xg_away": ag,
            "ts": datetime.now(BR_TZ).isoformat(timespec="seconds")
        })
    df = pd.DataFrame(rows, columns=["match_id","home","away","xg_home","xg_away","ts"])
    df.to_csv(out_path, index=False)
    print(f"[xg] OK -> {out_path} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
