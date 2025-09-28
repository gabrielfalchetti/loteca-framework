#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, argparse
import pandas as pd
import numpy as np

def _wandb_init_safe(project: str | None, name: str, config: dict):
    try:
        import wandb  # type: ignore
        if not os.getenv("WANDB_API_KEY","").strip(): return None
        return wandb.init(project=project or "loteca", name=name, config=config, reinit=True)
    except Exception:
        return None

def _wandb_log_safe(run, data: dict):
    try:
        if run: run.log(data)
    except Exception:
        pass

def kelly(p: float, k: float) -> float:
    try:
        b = k - 1.0
        f = (p*b - (1.0 - p)) / b
        return max(0.0, min(1.0, f))
    except Exception:
        return 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--probs", required=True)
    ap.add_argument("--odds", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--kelly_scale", type=float, default=0.5)
    args = ap.parse_args()

    probs = pd.read_csv(args.probs)
    odds  = pd.read_csv(args.odds)

    df = probs.merge(odds[["match_id","k1","kx","k2"]], on="match_id", how="left")
    # edge = p*k - 1
    for outc, colp, colk in [("1","p1","k1"),("X","px","kx"),("2","p2","k2")]:
        df[f"edge_{outc}"] = df[colp]*df[colk] - 1.0
        df[f"stake_{outc}"] = df[colp].map(lambda p: kelly(float(p),  float(np.nan)) if False else 0.0)  # placeholder
        df[f"stake_{outc}"] = [kelly(float(p), float(k)) * args.kelly_scale if (pd.notna(p) and pd.notna(k) and k>1) else 0.0
                               for p,k in zip(df[colp], df[colk])]

    df.to_csv(args.out, index=False)
    print(f"[risk] OK -> {args.out} ({len(df)} linhas)")

    run = _wandb_init_safe(os.getenv("WANDB_PROJECT") or "loteca", f"risk_{args.rodada}", {"kelly_scale": args.kelly_scale})
    _wandb_log_safe(run, {"risk_rows": len(df)})
    try:
        if run: run.finish()
    except Exception:
        pass

if __name__ == "__main__":
    main()
