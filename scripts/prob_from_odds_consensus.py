#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera predictions_calibrated.csv A PARTIR DAS ODDS REAIS do consenso.

Ideia:
- Usamos as odds "best" (melhores por lado) para apostar.
- Para PROBABILIDADES, usamos as "médias" (_mean_*) quando disponíveis (proxies de consenso entre casas).
  Isso evita que prob=1/odds (mesma fonte) gere Kelly=0.
- Se as médias não existirem, usamos as próprias odds e removemos o overround normalizando.

Saída: data/out/<rodada>/predictions_calibrated.csv com:
  match_key, prob_home, prob_draw, prob_away
"""

import argparse, os, sys, json
import numpy as np
import pandas as pd


def ensure_out_dir(rodada: str) -> str:
    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def implied_probs_from_odds(oh, od, oa) -> tuple:
    vals = [oh, od, oa]
    inv = [ (1.0/x) if (x and x>1.0) else np.nan for x in vals ]
    s = np.nansum(inv)
    if not (s and s>0):
        return (np.nan, np.nan, np.nan)
    p = [v/s if not (v is None or np.isnan(v)) else np.nan for v in inv]
    # completa faltante (se tiver exatamente 2)
    if sum([not np.isnan(x) for x in p]) == 2:
        miss = [i for i,x in enumerate(p) if np.isnan(x)]
        if miss:
            idx = miss[0]
            other = [i for i in range(3) if i!=idx]
            p[idx] = max(0.0, 1.0 - (p[other[0]] + p[other[1]]))
    return tuple(p)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = ensure_out_dir(args.rodada)
    p_ext = os.path.join(out_dir, "odds_consensus_ext.csv")
    p_core = os.path.join(out_dir, "odds_consensus.csv")

    if os.path.isfile(p_ext):
        df = pd.read_csv(p_ext)
    elif os.path.isfile(p_core):
        df = pd.read_csv(p_core)
    else:
        print("[prob] ERRO: odds_consensus(.csv/_ext.csv) não encontrados.")
        sys.exit(10)

    # le colunas
    def col(name): return name if name in df.columns else None

    # preferimos médias, se existirem
    mean_h = col("_mean_home"); mean_d = col("_mean_draw"); mean_a = col("_mean_away")
    use_means = all([c is not None for c in (mean_h, mean_d, mean_a)])

    probs = []
    for _, r in df.iterrows():
        # odds "média" para gerar prob. Se não tiver, usa as odds_core.
        oh = r[mean_h] if use_means else r.get("odds_home", np.nan)
        od = r[mean_d] if use_means else r.get("odds_draw", np.nan)
        oa = r[mean_a] if use_means else r.get("odds_away", np.nan)

        ph, pdw, pa = implied_probs_from_odds(oh, od, oa)
        # Clip e normalização final
        arr = np.array([ph, pdw, pa], dtype="float64")
        if np.isfinite(arr).sum() >= 2:
            arr = np.clip(arr, 0.0, 1.0)
            s = np.nansum(arr)
            if s and s > 0:
                arr = arr / s
        probs.append((arr[0], arr[1], arr[2]))

    out = pd.DataFrame({
        "match_key": df["match_key"].astype(str),
        "prob_home": [p[0] for p in probs],
        "prob_draw": [p[1] for p in probs],
        "prob_away": [p[2] for p in probs],
    })

    # Mantém apenas linhas com pelo menos 2 probabilidades válidas
    mask = out[["prob_home","prob_draw","prob_away"]].notna().sum(axis=1) >= 2
    out = out[mask].copy()

    if out.empty:
        print("[prob] ERRO: não foi possível derivar probabilidades a partir das odds.")
        sys.exit(10)

    p_out = os.path.join(out_dir, "predictions_calibrated.csv")
    out.to_csv(p_out, index=False)
    if args.debug:
        print(f"[prob] OK -> {p_out} ({len(out)} linhas)")
        print(f"[prob] AMOSTRA: {out.head(5).to_dict(orient='records')}")
    else:
        print(f"[prob] OK -> {p_out} ({len(out)} linhas)")

if __name__ == "__main__":
    main()