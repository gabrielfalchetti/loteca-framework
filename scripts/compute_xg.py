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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    base = os.path.join("data","out",args.rodada)
    feats = pd.read_csv(os.path.join(base,"features_base.csv"))
    # stub: heurística simples baseada nas odds (quanto menor a odd, maior xG esperado)
    def inv(o): 
        try: return 1.0/float(o) if float(o)>0 else np.nan
        except Exception: return np.nan
    feats["xg_home"] = feats["k1"].map(inv)
    feats["xg_away"] = feats["k2"].map(inv)
    feats["xg_draw"] = feats["kx"].map(inv)

    xg = feats[["match_id","home","away","xg_home","xg_draw","xg_away"]].copy()
    xg.to_csv(args.out, index=False)
    print(f"[xg] OK -> {args.out} ({len(xg)} linhas)")

    run = _wandb_init_safe(os.getenv("WANDB_PROJECT") or "loteca", f"xg_{args.rodada}", {})
    _wandb_log_safe(run, {"xg_rows": len(xg)})
    try:
        if run: run.finish()
    except Exception:
        pass

if __name__ == "__main__":
    main()
