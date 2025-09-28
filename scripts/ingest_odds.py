#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds.py — consolida odds de múltiplas fontes (TheOddsAPI, API-Football)
- left-join por (home_n, away_n) com proteção a ausências
- consenso simples por média harmônica das cotações disponíveis
- telemetria opcional (WANDB)

Saída:
  data/out/<RODADA>/odds.csv
"""

from __future__ import annotations
import os, sys, math, argparse, unicodedata
from typing import List
import pandas as pd
import numpy as np

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    import unicodedata
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip().replace(".", " ")
    return " ".join(s.split())

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

def load_matches(rodada: str) -> pd.DataFrame:
    p = os.path.join("data","in",rodada,"matches_source.csv")
    if not os.path.isfile(p):
        raise FileNotFoundError(f"[consensus] matches_source ausente: {p}")
    df = pd.read_csv(p)
    if "match_id" not in df.columns:
        df.insert(0, "match_id", range(1, len(df)+1))
    df["home_n"] = df["home"].map(_norm)
    df["away_n"] = df["away"].map(_norm)
    return df

def load_provider(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        return pd.DataFrame(columns=["match_id","home","away","k1","kx","k2","source"])
    df = pd.read_csv(path)
    for c in ["home","away"]:
        if c in df.columns:
            df[c+"_n"] = df[c].map(_norm)
    return df

def harm_mean(vals: List[float]) -> float | None:
    arr = [v for v in vals if v and v>0]
    if not arr: return None
    inv = sum(1.0/v for v in arr)
    if inv <= 0: return None
    return len(arr)/inv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    rodada = args.rodada
    out_dir = os.path.join("data","out",rodada); os.makedirs(out_dir, exist_ok=True)
    to_path = os.path.join(out_dir, "odds_theoddsapi.csv")
    af_path = os.path.join(out_dir, "odds_apifootball.csv")
    out_path = os.path.join(out_dir, "odds.csv")

    matches = load_matches(rodada)
    to_df = load_provider(to_path)
    af_df = load_provider(af_path)

    # merge por (home_n, away_n)
    merged = matches[["match_id","home","away","home_n","away_n"]].copy()

    for prov, df in [("TheOddsAPI", to_df), ("APIFootball", af_df)]:
        if df.empty: continue
        df2 = df.rename(columns={"k1": f"k1_{prov}", "kx": f"kx_{prov}", "k2": f"k2_{prov}"})
        merged = merged.merge(
            df2[["home_n","away_n",f"k1_{prov}",f"kx_{prov}",f"k2_{prov}"]],
            on=["home_n","away_n"], how="left"
        )

    # consenso (média harmônica por coluna)
    ks = []
    out_rows = []
    for _, r in merged.iterrows():
        k1_vals = [r.get("k1_TheOddsAPI"), r.get("k1_APIFootball")]
        kx_vals = [r.get("kx_TheOddsAPI"), r.get("kx_APIFootball")]
        k2_vals = [r.get("k2_TheOddsAPI"), r.get("k2_APIFootball")]
        k1 = harm_mean([float(x) for x in k1_vals if pd.notna(x)])
        kx = harm_mean([float(x) for x in kx_vals if pd.notna(x)])
        k2 = harm_mean([float(x) for x in k2_vals if pd.notna(x)])
        out_rows.append({"match_id": r["match_id"], "home": r["home"], "away": r["away"], "k1": k1, "kx": kx, "k2": k2})

    out = pd.DataFrame(out_rows, columns=["match_id","home","away","k1","kx","k2"])
    out.to_csv(out_path, index=False)
    print(f"[consensus] odds de consenso -> {out_path} (n={len(out)})")

    flag_to = 1 if (not to_df.empty) else 0
    flag_rapid = 1 if (not af_df.empty) else 0
    print(f"[audit] Odds usadas: TheOddsAPI={flag_to} RapidAPI={flag_rapid}")

    # W&B
    run = _wandb_init_safe(os.getenv("WANDB_PROJECT") or "loteca", f"consensus_{rodada}", {})
    _wandb_log_safe(run, {"consensus_rows": len(out), "theodds_used": flag_to, "rapidapi_used": flag_rapid})
    try:
        if run: run.finish()
    except Exception:
        pass

if __name__ == "__main__":
    main()
