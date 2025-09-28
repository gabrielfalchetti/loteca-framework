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
    ap.add_argument("--in", dest="preds_in", default="")
    ap.add_argument("--odds", required=False, default="")
    ap.add_argument("--out", required=True)
    ap.add_argument("--isotonic", action="store_true", help="aplica uma calibração isotônica simples se possível")
    args = ap.parse_args()

    base = os.path.join("data","out",args.rodada)
    # fonte principal: preds_bivar.csv (se existir) OU derivar de odds (1/k normalizado)
    if args.preds_in and os.path.isfile(args.preds_in):
        df = pd.read_csv(args.preds_in)
        # garantir colunas
        need = {"p1","px","p2"}
        if not need.issubset(df.columns):
            raise RuntimeError("preds_bivar.csv precisa de colunas p1, px, p2")
        probs = df.copy()
    else:
        odds_path = args.odds if args.odds else os.path.join(base,"odds.csv")
        if not os.path.isfile(odds_path):
            raise FileNotFoundError(odds_path)
        odds = pd.read_csv(odds_path)
        inv1 = 1.0/odds["k1"].astype(float)
        invx = 1.0/odds["kx"].astype(float)
        inv2 = 1.0/odds["k2"].astype(float)
        s = inv1+invx+inv2
        probs = odds[["match_id","home","away"]].copy()
        probs["p1"] = inv1/s
        probs["px"] = invx/s
        probs["p2"] = inv2/s

    # calibração isotônica (stub: identidade — ponto de extensão futura)
    if args.isotonic:
        # espaço para aplicar IsotonicRegression com histórico (não implementado por falta de dataset aqui)
        pass

    probs.to_csv(args.out, index=False)
    print(f"[build_probs] Fonte='{('preds_bivar.csv' if args.preds_in else 'features_base.csv')}' -> {args.out} ({len(probs)} linhas)")

    run = _wandb_init_safe(os.getenv("WANDB_PROJECT") or "loteca", f"build_probs_{args.rodada}", {})
    _wandb_log_safe(run, {"probs_rows": len(probs)})
    try:
        if run: run.finish()
    except Exception:
        pass

if __name__ == "__main__":
    main()
