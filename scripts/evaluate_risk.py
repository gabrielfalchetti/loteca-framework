#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, argparse, pandas as pd, numpy as np

def to_str_id(s):
    if pd.isna(s):
        return ""
    return str(s).strip()

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--out", default=None)
    return ap.parse_args()

def main():
    args = parse_args()
    rodada = args.rodada
    outdir = f"data/out/{rodada}"
    os.makedirs(outdir, exist_ok=True)

    f_probs = f"{outdir}/probabilities_calibrated.csv"
    if not os.path.exists(f_probs):
        f_probs = f"{outdir}/probabilities.csv"  # fallback

    if not os.path.exists(f_probs):
        print(f"[risk] AVISO: não encontrei probabilities*.csv em {outdir}. Saída vazia.")
        pd.DataFrame(columns=[
            "match_id","risk_flag","reason"
        ]).to_csv(args.out or f"{outdir}/risk_report.csv", index=False)
        return

    probs = pd.read_csv(f_probs)
    feats_path = f"{outdir}/features_base.csv"
    feats = pd.read_csv(feats_path) if os.path.exists(feats_path) else pd.DataFrame(columns=["match_id"])

    # normaliza tipos
    probs["match_id"] = probs["match_id"].apply(to_str_id)
    feats["match_id"] = feats["match_id"].apply(to_str_id)

    df = probs.merge(feats[["match_id","k1","kx","k2"]], on="match_id", how="left")

    # Exemplo simples de checagem: odds ausentes -> risco
    df["risk_flag"] = np.where(df[["k1","kx","k2"]].isna().any(axis=1), 1, 0)
    df["reason"] = np.where(df["risk_flag"]==1, "Sem odds completas (k1/kx/k2) no join_features", "")

    out = args.out or f"{outdir}/risk_report.csv"
    df[["match_id","risk_flag","reason"]].to_csv(out, index=False)
    print(f"[risk] OK -> {out} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
