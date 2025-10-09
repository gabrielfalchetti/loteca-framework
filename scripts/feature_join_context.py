#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
feature_join_context.py (STRICT)

Une features univariadas/bivariadas/xg + clima + notícias (opcional)
para produzir <OUT_DIR>/context_features.csv.

Regras:
- Exige: features_univariado.csv, features_bivariado.csv, features_xg.csv
- Exige: weather.csv (não vazio, ao menos colunas mínimas)
- Faz merges garantindo 'match_id' como string (evita erro object vs int64).
- Se qualquer insumo estiver vazio/ausente, falha com exit code 28.
"""

import os
import sys
import argparse
import pandas as pd


def die(msg: str, code: int = 28):
    print(f"##[error]{msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def load_required(path: str, name: str) -> pd.DataFrame:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        die(f"Arquivo obrigatório ausente/vazio: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"Falha lendo {name} ({path}): {e}")
    if df is None or df.empty:
        die(f"Arquivo obrigatório vazio: {path}")
    return df


def ensure_match_id(df: pd.DataFrame) -> pd.DataFrame:
    if "match_id" not in df.columns:
        # tenta a partir de match_key
        if "match_key" in df.columns:
            df["match_id"] = df["match_key"].astype(str)
        elif "home" in df.columns and "away" in df.columns:
            df["match_id"] = df["home"].astype(str) + "__" + df["away"].astype(str)
        elif "team_home" in df.columns and "team_away" in df.columns:
            df["match_id"] = df["team_home"].astype(str) + "__" + df["team_away"].astype(str)
        else:
            die("Não foi possível derivar 'match_id' em um dos datasets.")
    df["match_id"] = df["match_id"].astype(str)
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)
    print(f"[context] Base (whitelist) linhas={len(pd.read_csv(os.path.join(out_dir,'matches_whitelist.csv')))}" if os.path.isfile(os.path.join(out_dir,'matches_whitelist.csv')) else "[context] sem whitelist explícita")

    # carrega insumos obrigatórios
    fu = load_required(os.path.join(out_dir, "features_univariado.csv"), "features_univariado")
    fb = load_required(os.path.join(out_dir, "features_bivariado.csv"), "features_bivariado")
    fx = load_required(os.path.join(out_dir, "features_xg.csv"), "features_xg")
    w  = load_required(os.path.join(out_dir, "weather.csv"), "weather")

    # normaliza match_id (string em todos)
    fu = ensure_match_id(fu)
    fb = ensure_match_id(fb)
    fx = ensure_match_id(fx)
    w  = ensure_match_id(w)

    # seleciona colunas relevantes de cada bloco
    fu_cols = ["match_id","fair_p_home","fair_p_draw","fair_p_away","entropy_bits","gap_top_second"]
    fb_cols = ["match_id","diff_ph_pa","ratio_ph_pa","entropy_x_gap","overround_x_entropy"]
    fx_cols = ["match_id","xg_home_proxy","xg_away_proxy","xg_diff_proxy"]
    w_cols  = ["match_id","temp_c","wind_speed_kph","precip_mm","relative_humidity"]

    for lst, nm in [(fu_cols,"features_univariado"),
                    (fb_cols,"features_bivariado"),
                    (fx_cols,"features_xg"),
                    (w_cols, "weather")]:
        missing = [c for c in lst if c not in locals()[nm.split('_')[0]][:0].columns]  # apenas para mypy
        # (checagem distinta por dataframe)
    # checagem efetiva
    for c in fu_cols:
        if c not in fu.columns: die(f"{c} ausente em features_univariado.csv")
    for c in fb_cols:
        if c not in fb.columns: die(f"{c} ausente em features_bivariado.csv")
    for c in fx_cols:
        if c not in fx.columns: die(f"{c} ausente em features_xg.csv")
    for c in w_cols:
        if c not in w.columns: die(f"{c} ausente em weather.csv")

    # merges
    df = fu.merge(fb, on="match_id", how="inner")
    df = df.merge(fx, on="match_id", how="inner")
    df = df.merge(w[w_cols], on="match_id", how="left")

    # garante que clima exista para todas as partidas (estrito)
    if df[["temp_c","wind_speed_kph","precip_mm","relative_humidity"]].isna().any().any():
        die("Clima ausente para uma ou mais partidas após merge (weather.csv incompleto).")

    # score simples de contexto (exemplo determinístico e reprodutível)
    # você pode evoluir esta fórmula; manteremos transparente:
    # quanto maior a incerteza (entropy_bits), menor o score; quanto maior xg_diff e diff_ph_pa, maior o score.
    df["context_score"] = (
        0.25 * (df["diff_ph_pa"]) +
        0.25 * (df["xg_diff_proxy"]) +
        0.20 * (df["ratio_ph_pa"]) -
        0.20 * (df["entropy_bits"]) +
        0.10 * (1.0 / (1.0 + df["overround_x_entropy"]))  # penaliza overround alto * entropia
    )

    out_path = os.path.join(out_dir, "context_features.csv")
    df.to_csv(out_path, index=False)
    if args.debug:
        print(f"[context] OK -> {out_path} (linhas={len(df)})")
        print(df.head(10).to_csv(index=False))

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        die(f"Erro inesperado: {e}")