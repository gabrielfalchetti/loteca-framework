#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera features bivariadas combinando odds de consenso + predições de mercado +
features univariadas. Saída: <rodada>/features_bivariado.csv

Entradas esperadas:
- <rodada>/features_univariado.csv (gerado na etapa anterior)
- <rodada>/predictions_market.csv  (para edges/combinações)
"""

import argparse
import os
import sys
import pandas as pd
import numpy as np


def die(msg: str, code: int = 22):
    print(f"[bivariado][ERRO] {msg}", file=sys.stderr)
    sys.exit(code)


def read_required(path: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        die(f"Arquivo obrigatório ausente ou vazio: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"Falha lendo {path}: {e}")
    if df.empty:
        die(f"Arquivo sem linhas: {path}")
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    uni_path = os.path.join(rodada, "features_univariado.csv")
    uni = read_required(uni_path)

    # predictions_market é opcional mas altamente recomendado
    pred_path = os.path.join(rodada, "predictions_market.csv")
    preds = pd.DataFrame()
    if os.path.exists(pred_path) and os.path.getsize(pred_path) > 0:
        preds = pd.read_csv(pred_path)
        # normaliza nomes
        rename = {}
        for a, b in [("p_home", "prob_home"), ("p_draw", "prob_draw"), ("p_away", "prob_away")]:
            if a in preds.columns and "prob_" + a.split("_")[1] not in preds.columns:
                rename[a] = b
        if rename:
            preds = preds.rename(columns=rename)

    # merge básico por match_id (left)
    df = uni.copy()
    if not preds.empty and "match_id" in preds.columns:
        keep_cols = ["match_id", "prob_home", "prob_draw", "prob_away"]
        keep_cols = [c for c in keep_cols if c in preds.columns]
        df = df.merge(preds[keep_cols], on="match_id", how="left", suffixes=("", "_mkt"))

    # combinações bivariadas (exemplos robustos)
    def safe_sub(a, b):
        return (a.fillna(0) - b.fillna(0))

    if {"imp_p_home", "mkt_prob_home"}.issubset(df.columns):
        df["bivar_edge_home_abs"] = safe_sub(df["mkt_prob_home"], df["imp_p_home"]).abs()
    if {"imp_p_draw", "mkt_prob_draw"}.issubset(df.columns):
        df["bivar_edge_draw_abs"] = safe_sub(df["mkt_prob_draw"], df["imp_p_draw"]).abs()
    if {"imp_p_away", "mkt_prob_away"}.issubset(df.columns):
        df["bivar_edge_away_abs"] = safe_sub(df["mkt_prob_away"], df["imp_p_away"]).abs()

    # razões e diferenças entre preços e probabilidades (quando disponíveis)
    if {"odds_home", "odds_away"}.issubset(df.columns):
        df["bivar_odds_ratio_ha"] = df["odds_home"] / df["odds_away"]
        df["bivar_odds_diff_ha"] = df["odds_home"] - df["odds_away"]

    if {"imp_p_home", "imp_p_away"}.issubset(df.columns):
        df["bivar_impdiff_home_away"] = df["imp_p_home"] - df["imp_p_away"]

    # robustez: remove linhas onde odds essenciais inválidas
    if "odds_home" in df.columns:
        df = df[df["odds_home"] > 1.0]
    if "odds_away" in df.columns:
        df = df[df["odds_away"] > 1.0]
    if "odds_draw" in df.columns:
        df = df[df["odds_draw"] > 1.0]

    out_path = os.path.join(rodada, "features_bivariado.csv")
    if df.empty:
        die("Nenhuma linha válida para gerar features_bivariado.csv")

    df.to_csv(out_path, index=False)
    if args.debug:
        print(f"[bivariado] gravado {out_path} ({len(df)} linhas)")
        print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()