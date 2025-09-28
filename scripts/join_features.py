#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
join_features.py — Framework Loteca v4.3
Gera dataset base da rodada a partir de matches_source e odds.

Entradas:
  data/in/<RODADA>/matches_source.csv
    colunas mínimas: match_id, home, away [, date]

  data/out/<RODADA>/odds.csv                 (opcional; se ausente, segue com NaN)

Saídas:
  data/out/<RODADA>/matches.csv              (padronizado + chaves normalizadas)
  data/out/<RODADA>/features_base.csv        (matches + odds prontos para modelagem)
"""

from __future__ import annotations
import argparse
import os
import sys
import math
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import List

import numpy as np
import pandas as pd

BR_TZ = timezone(timedelta(hours=-3))

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip().replace(".", " ")
    for suf in [" fc", " afc", " ac", " sc", "-sp", "-rj", " ec", " e.c."]:
        s = s.replace(suf, "")
    return " ".join(s.split())

def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def load_matches_source(rodada: str) -> pd.DataFrame:
    path = os.path.join("data", "in", rodada, "matches_source.csv")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"[join_features] arquivo não encontrado: {path}")
    df = pd.read_csv(path)
    # colunas mínimas
    if "home" not in df.columns or "away" not in df.columns:
        raise RuntimeError("[join_features] matches_source precisa das colunas 'home' e 'away'")
    if "match_id" not in df.columns:
        # cria match_id simples se não existir
        df = df.copy()
        df.insert(0, "match_id", range(1, len(df) + 1))
    if "date" not in df.columns:
        df["date"] = ""
    # normalização
    df["home_n"] = df["home"].apply(_norm)
    df["away_n"] = df["away"].apply(_norm)
    # ordena por match_id se existir
    if "match_id" in df.columns:
        df = df.sort_values("match_id").reset_index(drop=True)
    return df

def load_odds(rodada: str) -> pd.DataFrame:
    path = os.path.join("data", "out", rodada, "odds.csv")
    if not os.path.isfile(path):
        print(f"[join_features] AVISO: odds.csv ausente em {path} — seguindo sem odds.", file=sys.stderr)
        return pd.DataFrame(columns=["home","away","k1","kx","k2","total_line","over","under","book"])
    df = pd.read_csv(path)
    # normaliza chaves para merge
    if "home" not in df.columns or "away" not in df.columns:
        print(f"[join_features] AVISO: odds.csv sem colunas home/away — ignorando odds.", file=sys.stderr)
        return pd.DataFrame(columns=["home","away","k1","kx","k2","total_line","over","under","book"])
    df["home_n"] = df["home"].apply(_norm)
    df["away_n"] = df["away"].apply(_norm)
    # se faltar as colunas de odds, garante-as
    for c in ["k1","kx","k2","total_line","over","under","book"]:
        if c not in df.columns:
            df[c] = np.nan
    # remove duplicatas por par normalizado (mantém a primeira)
    df = df.drop_duplicates(subset=["home_n","away_n"])
    return df[["home_n","away_n","k1","kx","k2","total_line","over","under","book"]]

def build_matches(df_src: pd.DataFrame, rodada: str) -> pd.DataFrame:
    out_dir = os.path.join("data", "out", rodada)
    safe_mkdir(out_dir)
    matches_path = os.path.join(out_dir, "matches.csv")
    cols = ["match_id","home","away","date","home_n","away_n"]
    # garante apenas colunas essenciais + quaisquer extras úteis
    keep = [c for c in cols if c in df_src.columns] + [c for c in df_src.columns if c not in cols]
    matches = df_src[keep].copy()
    matches.to_csv(matches_path, index=False)
    print(f"[join_features] matches -> {matches_path} ({len(matches)} linhas)")
    return matches

def build_features(matches: pd.DataFrame, odds: pd.DataFrame, rodada: str) -> pd.DataFrame:
    # merge por home_n/away_n (left)
    base = matches.merge(
        odds,
        on=["home_n", "away_n"],
        how="left",
        suffixes=("", "_odds")
    )

    # features derivadas simples (placeholder para modelos)
    # - margem implícita e probabilidades de mercado (se odds presentes)
    def _safe_prob(o):
        try:
            return 1.0 / float(o) if o and not math.isnan(float(o)) and float(o) > 0 else np.nan
        except Exception:
            return np.nan

    base["p1_raw"] = base["k1"].apply(_safe_prob)
    base["px_raw"] = base["kx"].apply(_safe_prob)
    base["p2_raw"] = base["k2"].apply(_safe_prob)

    # normaliza pela soma se todas existem
    with np.errstate(invalid="ignore"):
        s = base[["p1_raw","px_raw","p2_raw"]].sum(axis=1)
        for col in ["p1","px","p2"]:
            base[col] = np.nan
        mask = s > 0
        base.loc[mask, "p1"] = base.loc[mask, "p1_raw"] / s[mask]
        base.loc[mask, "px"] = base.loc[mask, "px_raw"] / s[mask]
        base.loc[mask, "p2"] = base.loc[mask, "p2_raw"] / s[mask]

    # ordena e salva
    out_dir = os.path.join("data", "out", rodada)
    feats_path = os.path.join(out_dir, "features_base.csv")
    base.to_csv(feats_path, index=False)
    print(f"[join_features] features_base -> {feats_path} ({len(base)} linhas)")
    return base

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Identificador da rodada, ex.: 2025-09-27_1213")
    args = ap.parse_args()

    rodada = args.rodada.strip()
    if not rodada:
        raise RuntimeError("[join_features] argumento --rodada vazio")

    # carrega entradas
    df_src = load_matches_source(rodada)
    odds   = load_odds(rodada)

    # produz saídas
    matches = build_matches(df_src, rodada)
    _ = build_features(matches, odds, rodada)

    print(f"[join_features] OK — rodada={rodada} ts={datetime.now(BR_TZ).isoformat(timespec='seconds')}")

if __name__ == "__main__":
    main()
