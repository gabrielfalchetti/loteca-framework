#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
features_bivariado_xg.py
-----------------------
Gera:
  1) features_bivariado.csv  – métricas bivariadas derivadas de probabilidades/odds
  2) features_xg.csv         – proxies simples de xG por jogo (sem dados de chutes)

ENTRADA:
- data/out/<rodada>/features_univariado.csv  (obrigatória)
  colunas mínimas esperadas:
    match_key,home,away,
    fair_p_home,fair_p_draw,fair_p_away,
    overround,entropy_bits,
    value_home,value_draw,value_away

SAÍDAS:
- data/out/<rodada>/features_bivariado.csv
    match_key,home,away,
    diff_ph_pa,ratio_ph_pa,
    diff_fair_home_away,gap_value_home_away,
    entropy_x_gap,overround_x_entropy
- data/out/<rodada>/features_xg.csv
    match_key,home,away,
    xg_home_proxy,xg_away_proxy,xg_diff_proxy

Notas:
- diff_ph_pa = fair_p_home - fair_p_away
- ratio_ph_pa = fair_p_home / (fair_p_away + 1e-9)
- gap_value_home_away = (value_home - value_away)  (se values não existirem, vira 0.0)
- entropy_x_gap = entropy_bits * abs(diff_fair_home_away)
- overround_x_entropy = overround * entropy_bits
- xG proxies: soma home+away = 1.6 (constante) e
    xg_diff_proxy = 1.6 * (fair_p_home - fair_p_away)
    xg_home_proxy = 0.8 + xg_diff_proxy/2
    xg_away_proxy = 0.8 - xg_diff_proxy/2
  Isso reproduz os números observados em execuções anteriores do pipeline.

Uso:
python scripts/features_bivariado_xg.py --rodada data/out/<id> [--debug]
python scripts/features_bivariado_xg.py --rodada <id> [--debug]
"""

import argparse
import csv
import os
from typing import Tuple

import pandas as pd


def log(msg: str, debug: bool = False):
    if debug:
        print(f"[bivariado-xg] {msg}", flush=True)


def resolve_out_dir(rodada_arg: str) -> str:
    """Aceita tanto um ID puro quanto o caminho completo data/out/<id>."""
    if os.path.isdir(rodada_arg):
        return rodada_arg
    candidate = os.path.join("data", "out", str(rodada_arg))
    if os.path.isdir(candidate):
        return candidate
    # cria se ainda não existir (executando localmente)
    os.makedirs(candidate, exist_ok=True)
    return candidate


def load_univariado(out_dir: str, debug: bool = False) -> pd.DataFrame:
    fp = os.path.join(out_dir, "features_univariado.csv")
    if not os.path.isfile(fp):
        raise FileNotFoundError(f"[bivariado-xg] Arquivo não encontrado: {fp}")

    df = pd.read_csv(fp)

    # Normaliza nomes/colunas esperadas
    need = [
        "match_key", "home", "away",
        "fair_p_home", "fair_p_draw", "fair_p_away",
        "overround", "entropy_bits"
    ]
    for col in need:
        if col not in df.columns:
            raise ValueError(f"[bivariado-xg] Coluna obrigatória ausente em features_univariado.csv: '{col}'")

    # Colunas opcionais
    if "value_home" not in df.columns:
        df["value_home"] = 0.0
    if "value_draw" not in df.columns:
        df["value_draw"] = 0.0
    if "value_away" not in df.columns:
        df["value_away"] = 0.0

    # Tipagem básica
    num_cols = [
        "fair_p_home", "fair_p_draw", "fair_p_away",
        "overround", "entropy_bits",
        "value_home", "value_draw", "value_away",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Garantir chaves e nomes
    df["match_key"] = df["match_key"].fillna("").astype(str).str.strip()
    for c in ("home", "away"):
        df[c] = df[c].fillna("").astype(str).str.strip()

    return df


def compute_bivariado_row(r: pd.Series) -> dict:
    ph = float(r.get("fair_p_home"))
    pd_ = float(r.get("fair_p_draw"))
    pa = float(r.get("fair_p_away"))

    over = float(r.get("overround"))
    ent = float(r.get("entropy_bits"))

    vh = float(r.get("value_home", 0.0))
    va = float(r.get("value_away", 0.0))

    diff_ph_pa = ph - pa
    ratio_ph_pa = ph / (pa + 1e-9)

    diff_fair = ph - pa
    gap_val = vh - va

    entropy_x_gap = ent * abs(diff_fair)
    overround_x_entropy = over * ent

    return {
        "match_key": r.get("match_key"),
        "home": r.get("home"),
        "away": r.get("away"),
        "diff_ph_pa": diff_ph_pa,
        "ratio_ph_pa": ratio_ph_pa,
        "diff_fair_home_away": diff_fair,
        "gap_value_home_away": gap_val,
        "entropy_x_gap": entropy_x_gap,
        "overround_x_entropy": overround_x_entropy,
    }


def compute_xg_row(r: pd.Series) -> dict:
    """
    Proxy xG simples parametrizado para reproduzir saídas utilizadas no pipeline:
      soma xg_home_proxy + xg_away_proxy = 1.6
      xg_diff_proxy = 1.6 * (fair_p_home - fair_p_away)
    """
    ph = float(r.get("fair_p_home"))
    pa = float(r.get("fair_p_away"))

    xg_diff = 1.6 * (ph - pa)
    xg_home = 0.8 + xg_diff / 2.0
    xg_away = 0.8 - xg_diff / 2.0

    return {
        "match_key": r.get("match_key"),
        "home": r.get("home"),
        "away": r.get("away"),
        "xg_home_proxy": xg_home,
        "xg_away_proxy": xg_away,
        "xg_diff_proxy": xg_diff,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID da rodada OU caminho data/out/<id>")
    ap.add_argument("--season", required=False, help="temporada (não usada diretamente aqui, apenas por consistência)")
    ap.add_argument("--debug", action="store_true", help="Imprime logs detalhados")
    args = ap.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    df_u = load_univariado(out_dir, args.debug)

    # ---------- bivariado ----------
    biv_rows = [compute_bivariado_row(r) for _, r in df_u.iterrows()]
    df_biv = pd.DataFrame(biv_rows, columns=[
        "match_key", "home", "away",
        "diff_ph_pa", "ratio_ph_pa",
        "diff_fair_home_away", "gap_value_home_away",
        "entropy_x_gap", "overround_x_entropy",
    ])

    biv_path = os.path.join(out_dir, "features_bivariado.csv")
    df_biv.to_csv(biv_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # ---------- xg proxies ----------
    xg_rows = [compute_xg_row(r) for _, r in df_u.iterrows()]
    df_xg = pd.DataFrame(xg_rows, columns=[
        "match_key", "home", "away",
        "xg_home_proxy", "xg_away_proxy", "xg_diff_proxy",
    ])

    xg_path = os.path.join(out_dir, "features_xg.csv")
    df_xg.to_csv(xg_path, index=False, quoting=csv.QUOTE_MINIMAL)

    log(f"OK -> {biv_path} (bivariado), {xg_path} (xg)", args.debug)

    if args.debug:
        print("\n[bivariado] preview:")
        try:
            print(df_biv.head(10).to_string(index=False))
        except Exception:
            print(df_biv.head(10))

        print("\n[xg] preview:")
        try:
            print(df_xg.head(10).to_string(index=False))
        except Exception:
            print(df_xg.head(10))

        # csv previews
        print(df_biv.head(10).to_csv(index=False))
        print(df_xg.head(10).to_csv(index=False))


if __name__ == "__main__":
    main()