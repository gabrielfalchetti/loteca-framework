#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/consensus_odds_safe.py

Gera odds de consenso a partir dos provedores disponíveis (TheOddsAPI e/ou API-Football/RapidAPI).
Funciona se existir pelo menos UM provedor com odds válidas (> 1.0). Se os dois existirem,
faz a média por partida. Salva em data/out/<RODADA>/odds_consensus.csv.

Uso:
  python -m scripts.consensus_odds_safe --rodada 2025-09-27_1213 [--debug]
"""

import argparse
import json
import os
import sys
from typing import List, Optional

import pandas as pd


EXIT_NO_VALID_ODDS = 10


def log(msg: str, debug: bool = False):
    # imprime sempre; usar if debug para mensagens muito verbosas
    print(msg)


def _coerce_float_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _strip_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


def _has_any_valid_odds(df: Optional[pd.DataFrame]) -> bool:
    if df is None or df.empty:
        return False
    for c in ("odds_home", "odds_draw", "odds_away"):
        if c in df.columns:
            if (df[c] > 1.0).any():
                return True
    return False


def _valid_rows(df: pd.DataFrame) -> pd.DataFrame:
    # Considera linha válida se pelo menos duas casas (home/draw/away) tiverem odds > 1.0
    needed = ["odds_home", "odds_draw", "odds_away"]
    present = [c for c in needed if c in df.columns]
    if len(present) < 2:
        return df.iloc[0:0]  # sem colunas suficientes
    m = (df[present] > 1.0).sum(axis=1) >= 2
    return df[m].copy()


def _ensure_join_key(df: pd.DataFrame) -> pd.DataFrame:
    # Usa match_key se existir; senão, cria a partir de team_home x team_away
    out = df.copy()
    if "match_key" in out.columns and out["match_key"].notna().any():
        out["__join_key"] = out["match_key"].astype(str).str.strip()
    else:
        # normaliza nomes de times
        _strip_cols(out, ["team_home", "team_away"])
        out["__join_key"] = (out.get("team_home", "").astype(str).str.strip()
                             + " x "
                             + out.get("team_away", "").astype(str).str.strip())
    return out


def _load_provider_csv(path: str, debug: bool = False) -> Optional[pd.DataFrame]:
    if not os.path.isfile(path):
        log(f"[consensus-safe] AVISO: arquivo não encontrado: {path}", debug=debug)
        return None
    try:
        df = pd.read_csv(path)
    except Exception as e:
        log(f"[consensus-safe] AVISO: falha ao ler {path}: {e}", debug=debug)
        return None

    # normalização mínima
    base_cols = ["team_home", "team_away", "match_key", "odds_home", "odds_draw", "odds_away"]
    missing = [c for c in ["team_home", "team_away"] if c not in df.columns]
    if missing:
        log(f"[consensus-safe] AVISO: {path} sem colunas essenciais: {missing}", debug=debug)
        return None

    # cria colunas de odds se não existirem (como NaN)
    for c in ["odds_home", "odds_draw", "odds_away"]:
        if c not in df.columns:
            df[c] = float("nan")

    df = _coerce_float_cols(df, ["odds_home", "odds_draw", "odds_away"])
    df = _strip_cols(df, ["team_home", "team_away", "match_key"])
    df = _ensure_join_key(df)

    valid = _valid_rows(df)
    log(f"[consensus-safe] lido {os.path.basename(path)} -> {len(df)} linhas; válidas: {len(valid)}", debug=debug)
    return valid if not valid.empty else None


def _consensus_concat_and_mean(parts: List[pd.DataFrame], debug: bool = False) -> pd.DataFrame:
    # Concatena todas e agrega por __join_key fazendo mean de odds_* e pegando o primeiro team_home/away/match_key
    concat = pd.concat(parts, ignore_index=True)

    # Se ambos providers trouxerem o mesmo jogo com nomes levemente diferentes mas mesmo match_key,
    # a agregação por __join_key (derivado de match_key ou home x away) resolve.
    key = "__join_key"
    num_cols = ["odds_home", "odds_draw", "odds_away"]

    # Para preservar nomes de times e match_key, pegamos o "primeiro" por chave
    first_meta = (concat
                  .sort_values(by=[key])
                  .drop_duplicates(subset=[key], keep="first")
                  [[key, "team_home", "team_away", "match_key"]]
                  )

    # Média das odds por chave
    means = (concat
             .groupby(key, as_index=False)[num_cols]
             .mean(numeric_only=True)
             )

    merged = pd.merge(means, first_meta, on=key, how="left")
    cols = ["team_home", "team_away", "match_key", "odds_home", "odds_draw", "odds_away"]
    merged = merged[cols]

    # Reaplica filtro de validade (pode haver médias <= 1.0 se algum provedor mandar lixo)
    merged = _valid_rows(merged)

    return merged.reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Identificador da rodada (ex: 2025-09-27_1213)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    path_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    path_apifoot = os.path.join(out_dir, "odds_apifootball.csv")
    out_path = os.path.join(out_dir, "odds_consensus.csv")

    parts: List[pd.DataFrame] = []
    df_theodds = _load_provider_csv(path_theodds, debug=args.debug)
    if df_theodds is not None and _has_any_valid_odds(df_theodds):
        parts.append(df_theodds)

    df_apifoot = _load_provider_csv(path_apifoot, debug=args.debug)
    if df_apifoot is not None and _has_any_valid_odds(df_apifoot):
        parts.append(df_apifoot)

    total_raw = sum([0 if d is None else len(d) for d in [df_theodds, df_apifoot]])
    log(f"[consensus-safe] consenso bruto: {total_raw} linhas; "
        f"válidas (>=2 odds > 1.0): {sum(0 if d is None else len(d) for d in parts)}", debug=args.debug)

    if not parts:
        log("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(EXIT_NO_VALID_ODDS)

    # Se tiver 1 provedor -> passa adiante; se tiver 2+ -> média
    if len(parts) == 1:
        consensus = parts[0].copy()
        # Mantém as colunas e a ordem esperada
        consensus = consensus[["team_home", "team_away", "match_key", "odds_home", "odds_draw", "odds_away"]]
        log(f"[consensus-safe] OK (1 provedor) -> {out_path} ({len(consensus)} linhas)")
    else:
        consensus = _consensus_concat_and_mean(parts, debug=args.debug)
        log(f"[consensus-safe] OK (média de {len(parts)} provedores) -> {out_path} ({len(consensus)} linhas)")

    consensus.to_csv(out_path, index=False)


if __name__ == "__main__":
    main()