#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera features bivariadas e xG (proxy) a partir do univariado.

Entradas (obrigatória):
  <OUT_DIR>/features_univariado.csv   (com colunas: match_key, home, away,
                                      odd_home, odd_draw, odd_away,
                                      imp_home, imp_draw, imp_away,
                                      overround, fair_p_home, fair_p_draw, fair_p_away,
                                      gap_home_away, gap_top_second,
                                      logit_imp_home, logit_imp_draw, logit_imp_away,
                                      fav_label, entropy_bits, value_home, value_draw, value_away)

Saídas:
  <OUT_DIR>/features_bivariado.csv
  <OUT_DIR>/features_xg.csv
"""

import argparse
import os
import sys
import json
from typing import Tuple
import pandas as pd
import numpy as np

EXIT_CODE = 22

def log(msg: str) -> None:
    print(f"[bivariado-xg] {msg}")

def die(msg: str, code: int = EXIT_CODE) -> None:
    log(msg)
    sys.exit(code)

def require_file(path: str) -> None:
    if not os.path.exists(path):
        die(f"arquivo obrigatório não encontrado: {path}")

def load_univariado(out_dir: str) -> pd.DataFrame:
    path = os.path.join(out_dir, "features_univariado.csv")
    require_file(path)
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"falha ao ler {path}: {e}")
    if df.empty or df.shape[1] == 0:
        die(f"{path} vazio/sem colunas")
    # checa colunas mínimas
    must = [
        "match_key","home","away",
        "odd_home","odd_draw","odd_away",
        "imp_home","imp_draw","imp_away",
        "overround","fair_p_home","fair_p_draw","fair_p_away",
        "gap_home_away","gap_top_second",
        "logit_imp_home","logit_imp_draw","logit_imp_away",
        "fav_label","entropy_bits",
        "value_home","value_draw","value_away"
    ]
    missing = [c for c in must if c not in df.columns]
    if missing:
        die(f"features_univariado.csv faltando colunas obrigatórias: {missing}")
    return df

def build_bivariado(df_uni: pd.DataFrame) -> pd.DataFrame:
    df = df_uni.copy()

    # Interações / diferenças
    df["diff_ph_pa"] = df["imp_home"] - df["imp_away"]
    # evita div zero; clip em epsilon
    eps = 1e-12
    df["ratio_ph_pa"] = (df["imp_home"].clip(eps) / df["imp_away"].clip(eps)).replace([np.inf, -np.inf], np.nan)
    df["diff_fair_home_away"] = df["fair_p_home"] - df["fair_p_away"]
    df["gap_value_home_away"] = df["value_home"] - df["value_away"]
    df["entropy_x_gap"] = df["entropy_bits"] * df["gap_top_second"]
    df["overround_x_entropy"] = df["overround"] * df["entropy_bits"]

    out_cols = [
        "match_key","home","away",
        "diff_ph_pa","ratio_ph_pa",
        "diff_fair_home_away","gap_value_home_away",
        "entropy_x_gap","overround_x_entropy"
    ]
    return df[out_cols]

def approx_team_xg(p_home: float, p_draw: float, p_away: float) -> Tuple[float, float]:
    """
    Proxy simples de xG por time a partir do viés de prob. (p_home - p_away).
    Mantém soma ~base por jogo.
    """
    base_total_goals = 1.6
    bias = float(p_home) - float(p_away)
    xg_home = base_total_goals/2.0 + 0.8*bias
    xg_away = base_total_goals - xg_home
    # garante limites mínimos
    return max(xg_home, 0.05), max(xg_away, 0.05)

def build_xg(df_uni: pd.DataFrame) -> pd.DataFrame:
    xgh, xga = [], []
    for _, r in df_uni.iterrows():
        h, a = approx_team_xg(r["imp_home"], r["imp_draw"], r["imp_away"])
        xgh.append(h); xga.append(a)
    out = pd.DataFrame({
        "match_key": df_uni["match_key"],
        "home": df_uni["home"],
        "away": df_uni["away"],
        "xg_home_proxy": xgh,
        "xg_away_proxy": xga,
        "xg_diff_proxy": np.array(xgh) - np.array(xga)
    })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/<RID>)")
    ap.add_argument("--season", default="", help="Temporada (opcional, apenas para meta)")
    ap.add_argument("--debug", action="store_true", default=False)
    args = ap.parse_args()

    out_dir = args.rodada
    if not os.path.isdir(out_dir):
        die(f"OUT_DIR inexistente: {out_dir}")

    # 1) carrega univariado
    df_uni = load_univariado(out_dir)

    # 2) bivariado
    df_bi = build_bivariado(df_uni)
    out_bi = os.path.join(out_dir, "features_bivariado.csv")
    df_bi.to_csv(out_bi, index=False)
    if not os.path.exists(out_bi) or os.path.getsize(out_bi) == 0:
        die("features_bivariado.csv não gerado")

    # 3) xG (proxy)
    df_xg = build_xg(df_uni)
    out_xg = os.path.join(out_dir, "features_xg.csv")
    df_xg.to_csv(out_xg, index=False)
    if not os.path.exists(out_xg) or os.path.getsize(out_xg) == 0:
        die("features_xg.csv não gerado")

    # 4) meta
    meta = {
        "rows": int(df_uni.shape[0]),
        "season": args.season,
        "sources": {
            "features_univariado": os.path.relpath(os.path.join(out_dir, "features_univariado.csv"))
        },
        "outputs": {
            "features_bivariado": os.path.relpath(out_bi),
            "features_xg": os.path.relpath(out_xg)
        }
    }
    with open(os.path.join(out_dir, "features_bivariado_xg_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    log(f"OK -> {out_bi} (bivariado), {out_xg} (xg)")

    if args.debug:
        try:
            print("\n[bivariado] preview:")
            print(df_bi.head(10).to_string(index=False))
            print("\n[xg] preview:")
            print(df_xg.head(10).to_string(index=False))
        except Exception:
            pass

if __name__ == "__main__":
    main()