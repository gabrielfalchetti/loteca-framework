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

Observações:
- Este script NÃO chama APIs externas. Ele é resiliente mesmo se algum provedor
  não gerar linhas. Você pode plugar produtores específicos depois.
- Caso já exista um CSV de provedor em data/out/<RODADA>/, ele é lido e usado.
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
    # normalizações adicionais comuns (ajuste conforme seu dataset)
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
    # Reordena: esperadas primeiro, depois quaisquer extras (sem duplicar)
    ordered = list(dict.fromkeys(cols + list(out.columns)))
    return out.loc[:, ordered]


def implied_probs_from_odds(row: pd.Series) -> Tuple[float, float, float]:
    """Converte k1,kx,k2 em probabilidades implícitas simples (sem devig), retorna (p1, px, p2)."""
    k1, kx, k2 = row.get("k1"), row.get("kx"), row.
