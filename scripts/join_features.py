#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, argparse
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np

BR_TZ = timezone(timedelta(hours=-3))

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
    args = ap.parse_args()

    rodada = args.rodada
    out_dir = os.path.join("data","out",rodada); os.makedirs(out_dir, exist_ok=True)
    matches_in = os.path.join("data","in",rodada,"matches_source.csv")
    odds_in = os.path.join(out_dir, "odds.csv")

    if not os.path.isfile(matches_in): raise FileNotFoundError(matches_in)
    matches = pd.read_csv(matches_in)
    if "match_id" not in matches.columns: matches.insert(0,"match_id",range(1,len(matches)+1))

    odds = pd.read_csv(odds_in) if os.path.isfile(odds_in) else pd.DataFrame(columns=["match_id","k1","kx","k2"])

    # join básico (pode evoluir depois)
    feats = matches.merge(odds[["match_id","k1","kx","k2"]], on="match_id", how="left")
    feats["ts"] = datetime.now(BR_TZ).isoformat(timespec="seconds")

    matches_out = os.path.join(out_dir,"matches.csv")
    feats_out = os.path.join(out_dir,"features_base.csv")
    matches.to_csv(matches_out, index=False)
    feats.to_csv(feats_out, index=False)

    print(f"[join_features] matches -> {matches_out} ({len(matches)} linhas)")
    print(f"[join_features] features_base -> {feats_out} ({len(feats)} linhas)")
    print(f"[join_features] OK — rodada={rodada} ts={datetime.now(BR_TZ).isoformat(timespec='seconds')}")

    run = _wandb_init_safe(os.getenv("WANDB_PROJECT") or "loteca", f"join_features_{rodada}", {})
    _wandb_log_safe(run, {"matches_rows": len(matches), "features_rows": len(feats)})
    try:
        if run: run.finish()
    except Exception:
        pass

if __name__ == "__main__":
    main()
