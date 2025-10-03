#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/publish_kelly.py

- Lê configurações de env (BANKROLL, KELLY_FRACTION, KELLY_CAP, MIN/MAX_STAKE, ROUND_TO, KELLY_TOP_N).
- Prefere odds de data/out/<RODADA>/odds_consensus.csv.
- Se odds_consensus não existir ou estiver vazio, tenta data/out/<RODADA>/odds_theoddsapi.csv;
  se não existir, tenta data/in/<RODADA>/odds_theoddsapi.csv (e copia para /out).
- Normaliza colunas e filtra odds > 1.0.
- Busca probabilidades em qualquer arquivo predictions_*.csv encontrado no out_dir
  (ordem de preferência: predictions_stacked, predictions_calibrated, predictions_xg_bi, predictions_xg_uni).
- Só calcula Kelly quando houver (prob_* e odds_*) para o mesmo outcome.
- Gera data/out/<RODADA>/kelly_stakes.csv. Se nenhum par elegível, sai com código 10 com mensagem clara.
"""

import argparse, os, sys, re, json, math, shutil
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
import pandas as pd
import numpy as np


# --------------------------- util e config ---------------------------

@dataclass
class KellyConfig:
    bankroll: float = 1000.0
    kelly_fraction: float = 0.5
    kelly_cap: float = 0.10
    min_stake: float = 0.0
    max_stake: float = 0.0  # 0 = sem teto
    round_to: float = 1.0
    top_n: int = 14

def getenv_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default

def getenv_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def load_config() -> KellyConfig:
    cfg = KellyConfig(
        bankroll=getenv_float("BANKROLL", 1000.0),
        kelly_fraction=getenv_float("KELLY_FRACTION", 0.5),
        kelly_cap=getenv_float("KELLY_CAP", 0.10),
        min_stake=getenv_float("MIN_STAKE", 0.0),
        max_stake=getenv_float("MAX_STAKE", 0.0),
        round_to=getenv_float("ROUND_TO", 1.0),
        top_n=getenv_int("KELLY_TOP_N", 14),
    )
    return cfg

def ensure_out_dir(rodada: str) -> str:
    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def copy_if_exists(src: str, dst: str) -> Optional[str]:
    if os.path.isfile(src):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        try:
            shutil.copy2(src, dst)
            return dst
        except Exception:
            return src
    return None

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    def norm(c: str) -> str:
        c = str(c).strip().lower()
        c = re.sub(r"[ \t\.\-/]+", "_", c)
        c = re.sub(r"[\(\)\[\]\{\}]+", "", c)
        return c
    out = df.copy()
    out.columns = [norm(c) for c in df.columns]
    return out

def build_match_key(df: pd.DataFrame, th: str, ta: str) -> pd.Series:
    return (
        df[th].astype(str).str.strip().str.lower()
        + "__vs__" +
        df[ta].astype(str).str.strip().str.lower()
    )

# --------------------------- leitura de odds ---------------------------

HOME_ALS = ["odds_home","home_odds","price_home","home_price","