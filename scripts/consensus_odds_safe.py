#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds_consensus.csv a partir do TheOddsAPI apenas (RapidAPI IGNORADO).
- Procura odds_theoddsapi.csv em data/in/<RODADA>/ e data/out/<RODADA>/ (nesta ordem).
- Normaliza colunas (aliases), converte odds americanas -> decimal, limpa valores "[]", "", None.
- Considera válida a linha com pelo menos MIN_ODDS_COLS odds > 1.0 (default=1).
- Salva em data/out/<RODADA>/odds_consensus.csv.
"""

from __future__ import annotations
import os
import sys
import unicodedata
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import pandas as pd
import numpy as np


def log(msg: str) -> None:
    print(f"[consensus-safe] {msg}")


def norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def build_match_key(home: str, away: str) -> str:
    return f"{norm_key(home)}__vs__{norm_key(away)}"


def american_to_decimal(x: float) -> Optional[float]:
    """Converte cotação americana para decimal."""
    try:
        v = float(x)
    except Exception:
        return np.nan
    if v > 0:
        return 1.0 + (v / 100.0)
    if v < 0:
        return 1.0 + (100.0 / abs(v))
    return np.nan


def coerce_odds_series(raw: pd.Series) -> pd.Series:
    """
    Converte uma série de odds (str/float/int/[]) em decimal (float).
    Trata strings vazias, '[]', valores nulos, americanos (+120/-150) e decimais.
    """
    # Começa com float
    dec = pd.Series(index=raw.index, dtype="float64")

    # 1) valores já numéricos
    is_num = raw.apply(lambda v: isinstance(v, (int, float))) & raw.notna()
    dec.loc[is_num] = pd.to_numeric(raw[is_num], errors="coerce")

    # 2) strings
    is_str = raw.apply(lambda v: isinstance(v, str))
    if is_str.any():
        s = raw[is_str].fillna("").str.strip()

        # limpa '[]' e vazios
        mask_empty = s.eq("") | s.eq("[]") | s.eq("[ ]")
        if mask_empty.any():
            dec.loc[mask_empty.index[mask_empty]] = np.nan

        # remove colchetes acidentais '[2.15]' -> '2.15'
        s_clean = s.str.replace("[", "", regex=False).str.replace("]", "", regex=False)

        # americanos: começam com '+' ou '-' e NÃO contêm ponto (evita confundir -150.5)
        is_am = s_clean.str.match(r"^[\+\-]\d+$")
        if is_am.any():
            dec.loc[is_am.index[is_am]] = s_clean[is_am].map(american_to_decimal)

        # decimais: números válidos com possível ponto
        remaining = ~mask_empty & ~is_am
        if remaining.any():
            dec.loc[remaining.index[remaining]] = pd.to_numeric(
                s_clean[remaining], errors="coerce"
            )

    # 3) qualquer outra coisa (listas, dicts etc.) -> NaN
    others = ~(is_num | is_str)
    if others.any():
        dec.loc[others.index[others]] = np.nan

    # odds menores ou iguais a 1.0 são inválidas/no value
    dec = dec.where(dec > 1.0, np.nan)
    return dec


# ---- mapeamento de aliases ----

HOME_ALS = [
    "team_home",
    "home",
    "home_team",
    "mandante",
]
AWAY_ALS = [
    "team_away",
    "away",
    "away_team",
    "visitante",
]
MATCH_ALS = [
    "match_key",
    "match_id",
    "partida",
]

ODDS_HOME_ALS = [
    "odds_home",
    "home_odds",
    "price_home",
    "home_price",
    "h2h_home",
]
ODDS_DRAW_ALS = [
    "odds_draw",
    "draw_odds",
    "price_draw",
    "draw_price",
    "h2h_draw",
]
ODDS_AWAY_ALS = [
    "odds_away",
    "away_odds",
    "price_away",
    "away_price",
    "h2h_away",
]


def find_first(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in cols:
            return cols[c]
    return None


def normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    lower_map = {c: c.lower() for c in df.columns}
    df = df.rename(columns=lower_map)

    home = find_first(df, HOME_ALS)
    away = find_first(df, AWAY_ALS)
    match_key = find_first(df, MATCH_ALS)

    o_h = find_first(df, ODDS_HOME_ALS)
    o_d = find_first(df, ODDS_DRAW_ALS)
    o_a = find_first(df, ODDS_AWAY_ALS)

    mapping = {
        "team_home": home,
        "team_away": away,
        "match_key": match_key,
        "odds_home": o_h,
        "odds_draw": o_d,
        "odds_away": o_a,
    }

    # cria match_key se faltar
    df_out = df.copy()
    if match_key is None and home and away:
        df_out["match_key"] = df_out.apply(
            lambda r: build_match_key(str(r[home]), str(r[away])), axis=1
        )
        mapping["match_key"] = "match_key"

    # mantém apenas colunas relevantes + quaisquer extras originais
    keep = [c for c in mapping.values() if c is not None]
    if "match_key" not in keep:
        keep.append("match_key")  # caso recém-criada
    keep = list(dict.fromkeys(keep))  # uniq, preserva ordem
    return df_out, mapping


def read_csv_if_exists(path: Path) -> Optional[pd.DataFrame]:
    if path.exists():
        try:
            df = pd.read_csv(path)
            return df
        except Exception as e:
            log(f"ERRO ao ler {path}: {e}")
            return None
    return None


def main() -> None:
    rodada = os.environ.get("RODADA") or (
        sys.argv[sys.argv.index("--rodada") + 1] if "--rodada" in sys.argv else None
    )
    if not rodada:
        print("Uso: python -m scripts.consensus_odds_safe --rodada <YYYY-MM-DD_HHMM>")
        sys.exit(2)

    min_odds_cols = os.environ.get("MIN_ODDS_COLS")
    try:
        min_odds_cols = int(min_odds_cols) if min_odds_cols is not None else 1
    except Exception:
        min_odds_cols = 1

    base_in = Path("data/in") / rodada
    base_out = Path("data/out") / rodada
    base_out.mkdir(parents=True, exist_ok=True)

    # prioridade: IN depois OUT
    candidate_paths = [
        base_in / "odds_theoddsapi.csv",
        base_out / "odds_theoddsapi.csv",
    ]

    src_df = None
    src_used = None
    for p in candidate_paths:
        df = read_csv_if_exists(p)
        if df is not None:
            src_df = df
            src_used = p
            break

    if src_df is None:
        log(f"AVISO: arquivo não encontrado: {candidate_paths[0]}")
        log(f"AVISO: arquivo não encontrado: {candidate_paths[1]}")
        log("ERRO: nenhuma fonte de odds encontrada (TheOddsAPI). Abortando.")
        sys.exit(10)

    # normalização de colunas
    df_norm, mapping = normalize_columns(src_df)

    # precisa ter home/away e match_key
    for req in ("team_home", "team_away", "match_key"):
        if mapping.get(req) is None and req not in df_norm.columns:
            log("ERRO: colunas básicas ausentes (team_home/team_away/match_key).")
            sys.exit(10)

    # odds: cria colunas mesmo se ausentes (ficam NaN)
    for out_name, aliases in [
        ("odds_home", ODDS_HOME_ALS),
        ("odds_draw", ODDS_DRAW_ALS),
        ("odds_away", ODDS_AWAY_ALS),
    ]:
        src_col = find_first(df_norm, aliases)
        if src_col is None:
            df_norm[out_name] = np.nan
        else:
            df_norm[out_name] = coerce_odds_series(df_norm[src_col])

    # filtra válidas
    odds_cols = ["odds_home", "odds_draw", "odds_away"]
    cnt_valid = df_norm[odds_cols].gt(1.0).sum(axis=1)
    df_valid = df_norm[cnt_valid >= min_odds_cols].copy()

    log(
        f"lido {src_used.name} -> {len(df_norm)} linhas; válidas: {len(df_valid)}"
    )
    if len(df_valid) == 0:
        # mostra motivos
        reason_counts = {
            "menos_de_duas_odds" if min_odds_cols >= 2 else "menos_de_uma_odd": int(
                (cnt_valid < min_odds_cols).sum()
            )
        }
        log(f"motivos inválidos theoddsapi: {reason_counts}")
        log(
            f"ERRO: nenhuma linha de odds válida. Abortando."
        )
        sys.exit(10)

    # colunas finais
    final_cols = []
    # garantir presença das três básicas
    for c in ("team_home", "team_away", "match_key"):
        if c in df_valid.columns:
            final_cols.append(c)
        else:
            # mapear do original
            src_col = mapping.get(c)
            if src_col and src_col in df_valid.columns:
                df_valid.rename(columns={src_col: c}, inplace=True)
                final_cols.append(c)

    final_cols += odds_cols
    final_cols = list(dict.fromkeys(final_cols))

    out_path = base_out / "odds_consensus.csv"
    df_valid.to_csv(out_path, index=False, columns=final_cols)

    log(
        f"OK -> {out_path} ({len(df_valid)} linhas) | mapping theoddsapi: "
        f"team_home='{mapping.get('team_home')}', "
        f"team_away='{mapping.get('team_away')}', "
        f"match_key='{mapping.get('match_key')}', "
        f"odds_home='odds_home', odds_draw='odds_draw', odds_away='odds_away'"
    )


if __name__ == "__main__":
    main()