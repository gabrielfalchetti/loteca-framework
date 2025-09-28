#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds.py — Framework Loteca v4.3.RC1+
Coleta/ingesta de odds por rodada com proteção contra dados ausentes,
normalização de nomes e consenso com devig proporcional.

Uso:
  python scripts/ingest_odds.py --rodada 2025-09-27_1213 [--debug]

Entradas esperadas:
  data/in/<RODADA>/matches_source.csv  (colunas mín.: match_id, home, away [,date])

Saídas:
  data/out/<RODADA>/odds_theoddsapi.csv
  data/out/<RODADA>/odds_apifootball.csv
  data/out/<RODADA>/odds.csv                (consenso)
  (logs no stdout)
"""

from __future__ import annotations
import argparse
import os
import sys
import math
import unicodedata
from typing import List, Tuple

import numpy as np
import pandas as pd


EXPECTED_ODDS_COLS = [
    # Chaves padronizadas (normalizadas)
    "home_n", "away_n",
    # Metadados
    "home", "away", "book", "ts",
    # Odds 1X2
    "k1", "kx", "k2",
    # Linha de gols (se houver)
    "total_line", "over", "under",
]


# ------------------------ Utilidades ------------------------ #

def normalize_name(s: str) -> str:
    """Normaliza nomes de clubes (sem acento, minúsculo, trim)."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
    s = s.replace(" fc", "").replace(" ac", "").replace(" afc", "").replace(" sc", "").replace(".", " ")
    s = " ".join(s.split())
    return s


def ensure_columns(df: pd.DataFrame, cols: List[str] = EXPECTED_ODDS_COLS) -> pd.DataFrame:
    """Garante que o DataFrame tenha todas as colunas esperadas; adiciona NaN onde faltar."""
    if df is None:
        df = pd.DataFrame()
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = np.nan
    ordered = list(dict.fromkeys(cols + list(out.columns)))
    return out.loc[:, ordered]


def implied_probs_from_odds(row: pd.Series) -> Tuple[float, float, float]:
    """Converte k1,kx,k2 em probabilidades implícitas simples (sem devig), retorna (p1, px, p2)."""
    k1, kx, k2 = row.get("k1"), row.get("kx"), row.get("k2")
    def inv(o):
        try:
            return 1.0 / float(o) if (o and float(o) > 1e-12) else np.nan
        except Exception:
            return np.nan
    return inv(k1), inv(kx), inv(k2)


def proportional_devig(p1: float, px: float, p2: float) -> Tuple[float, float, float]:
    """Remove vigorish proporcionalmente (p_i' = p_i / sum_p)."""
    arr = np.array([p1, px, p2], dtype=float)
    if np.all(np.isnan(arr)):
        return (np.nan, np.nan, np.nan)
    s = np.nansum(arr)
    if s <= 0 or math.isclose(s, 0.0):
        return (np.nan, np.nan, np.nan)
    return tuple(arr / s)


def probs_to_odds(p1: float, px: float, p2: float) -> Tuple[float, float, float]:
    """Converte probabilidades em odds (1/p)."""
    def invp(p):
        try:
            return 1.0 / float(p) if (p and float(p) > 1e-12) else np.nan
        except Exception:
            return np.nan
    return invp(p1), invp(px), invp(p2)


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# ------------------------ I/O: Matches ------------------------ #

def load_matches(rodada: str) -> pd.DataFrame:
    in_dir = os.path.join("data", "in", rodada)
    path = os.path.join(in_dir, "matches_source.csv")
    if not os.path.isfile(path):
        print(f"Error: Crie {path} com colunas: match_id,home,away[,date].", file=sys.stderr)
        sys.exit(2)

    matches = pd.read_csv(path)
    for col in ["match_id", "home", "away"]:
        if col not in matches.columns:
            print(f"Error: {path} precisa da coluna '{col}'.", file=sys.stderr)
            sys.exit(2)

    matches["home_n"] = matches["home"].apply(normalize_name)
    matches["away_n"] = matches["away"].apply(normalize_name)

    base_cols = ["match_id", "home", "away", "home_n", "away_n"]
    extra_cols = [c for c in matches.columns if c not in base_cols]
    matches = matches.loc[:, base_cols + extra_cols]
    return matches


# ------------------------ Provedores (resilientes) ------------------------ #

def read_existing_provider_csv(out_dir: str, fname: str) -> pd.DataFrame:
    path = os.path.join(out_dir, fname)
    if os.path.isfile(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def provider_theoddsapi(matches: pd.DataFrame, out_dir: str, debug: bool = False) -> pd.DataFrame:
    fname = "odds_theoddsapi.csv"
    df = read_existing_provider_csv(out_dir, fname)
    if df.empty:
        df = pd.DataFrame(columns=["home", "away", "book", "k1", "kx", "k2", "total_line", "over", "under", "ts"])
    df["home_n"] = df["home"].apply(normalize_name)
    df["away_n"] = df["away"].apply(normalize_name)
    df = ensure_columns(df)
    path = os.path.join(out_dir, fname)
    df.to_csv(path, index=False)
    if df.empty:
        print("[theoddsapi] Aviso: nenhuma odd coletada para este sport.")
    else:
        print(f"[theoddsapi] OK -> {path} ({len(df)} linhas)")
    return df


def provider_apifootball(matches: pd.DataFrame, out_dir: str, debug: bool = False) -> pd.DataFrame:
    fname = "odds_apifootball.csv"
    df = read_existing_provider_csv(out_dir, fname)
    if df.empty:
        df = pd.DataFrame(columns=["home", "away", "book", "k1", "kx", "k2", "total_line", "over", "under", "ts"])
    df["home_n"] = df["home"].apply(normalize_name)
    df["away_n"] = df["away"].apply(normalize_name)
    df = ensure_columns(df)
    path = os.path.join(out_dir, fname)
    df.to_csv(path, index=False)
    print(f"[apifootball] OK -> {path} ({len(df)} linhas)")
    return df


# ------------------------ Consenso ------------------------ #

def build_consensus(matches: pd.DataFrame, providers: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Consenso por (home_n, away_n):
      1) odds -> probs implícitas
      2) devig proporcional por linha
      3) média por par (ignorando NaN)
      4) probs -> odds
    """
    outs = []
    for df in providers:
        if df is None or df.empty:
            continue
        keep = ["home", "away", "home_n", "away_n", "k1", "kx", "k2", "book", "ts"]
        keep = [c for c in keep if c in df.columns]
        outs.append(df.loc[:, keep].copy())

    if not outs:
        base = matches.loc[:, ["match_id", "home", "away", "home_n", "away_n"]].copy()
        for c in ["k1", "kx", "k2", "p1", "px", "p2"]:
            base[c] = np.nan
        return base

    all_outs = pd.concat(outs, ignore_index=True, sort=False)

    p_cols = []
    for idx in range(len(all_outs)):
        p1, px, p2 = implied_probs_from_odds(all_outs.iloc[idx])
        p1d, pxd, p2d = proportional_devig(p1, px, p2)
        p_cols.append((p1d, pxd, p2d))
    p_arr = np.array(p_cols)
    all_outs["p1"] = p_arr[:, 0]
    all_outs["px"] = p_arr[:, 1]
    all_outs["p2"] = p_arr[:, 2]

    agg = (all_outs
           .groupby(["home_n", "away_n"], dropna=False)[["p1", "px", "p2"]]
           .mean()
           .reset_index())

    cons = agg.copy()
    o_cols = []
    for idx in range(len(cons)):
        k1c, kxc, k2c = probs_to_odds(cons.loc[idx, "p1"], cons.loc[idx, "px"], cons.loc[idx, "p2"])
        o_cols.append((k1c, kxc, k2c))
    o_arr = np.array(o_cols)
    cons["k1"] = o_arr[:, 0]
    cons["kx"] = o_arr[:, 1]
    cons["k2"] = o_arr[:, 2]

    # >>> MERGE SEGURO: usa a MESMA chave nos dois lados <<<
    merged = matches.merge(cons, on=["home_n", "away_n"], how="left")

    if "match_id" in merged.columns:
        merged = merged.sort_values(by=["match_id"], kind="stable")

    out_cols = [c for c in ["match_id", "home", "away", "home_n", "away_n", "k1", "kx", "k2", "p1", "px", "p2"] if c in merged.columns]
    merged = merged.loc[:, out_cols]
    return merged


# ------------------------ Main ------------------------ #

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Identificador da rodada, ex.: 2025-09-27_1213")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    rodada = args.rodada
    out_dir = os.path.join("data", "out", rodada)
    safe_mkdir(out_dir)

    matches = load_matches(rodada)

    theodds_df = provider_theoddsapi(matches, out_dir, debug=args.debug)
    apifoot_df = provider_apifootball(matches, out_dir, debug=args.debug)

    consensus = build_consensus(matches, [theodds_df, apifoot_df])
    consensus_path = os.path.join(out_dir, "odds.csv")
    consensus.to_csv(consensus_path, index=False)
    print(f"[consensus] odds de consenso -> {consensus_path} (n={len(consensus)})")

    flag_theodds = 1 if (theodds_df is not None and len(theodds_df) > 0) else 0
    flag_rapid = 1 if (apifoot_df is not None and len(apifoot_df) > 0) else 0
    print(f"[audit] Odds usadas: TheOddsAPI={flag_theodds} RapidAPI={flag_rapid}")


if __name__ == "__main__":
    main()
