#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds de consenso a partir dos provedores disponíveis e
FALHA (exit 10) se não houver nenhuma linha com odds reais.

Critérios de "odds reais":
  - Pelo menos 2 das colunas entre {odds_home, odds_draw, odds_away} > 1.0.

Entrada:
  data/out/<RODADA>/odds_theoddsapi.csv   (opcional)
  data/out/<RODADA>/odds_apifootball.csv  (opcional)

Saída:
  data/out/<RODADA>/odds_consensus.csv

Uso:
  python -m scripts.consensus_odds_safe --rodada 2025-09-27_1213 [--debug]
"""

import argparse
import os
import sys
from typing import List
import pandas as pd
import numpy as np


def _debug(msg: str, flag: bool):
    if flag:
        print(f"[consensus-safe] {msg}")


def read_csv_safe(path: str, debug: bool) -> pd.DataFrame:
    if not os.path.isfile(path):
        _debug(f"AVISO: arquivo não encontrado: {path}", debug)
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        _debug(f"lido {path} -> {len(df)} linhas", debug)
        return df
    except Exception as e:
        print(f"[consensus-safe] AVISO: falha ao ler {path}: {e}")
        return pd.DataFrame()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    # Mapeamentos comuns de nomes -> odds_*
    ren = {
        "home": "odds_home", "1": "odds_home", "home_win": "odds_home",
        "price_home": "odds_home", "h": "odds_home",

        "draw": "odds_draw", "x": "odds_draw", "tie": "odds_draw",
        "price_draw": "odds_draw", "d": "odds_draw",

        "away": "odds_away", "2": "odds_away", "away_win": "odds_away",
        "price_away": "odds_away", "a": "odds_away",
    }

    # Normaliza header (lower/strip)
    cols_norm = {}
    for c in out.columns:
        key = str(c).strip().lower()
        cols_norm[c] = ren.get(key, c)
    out = out.rename(columns=cols_norm)

    # Garante as 3 colunas, forçando numérico
    for c in ("odds_home", "odds_draw", "odds_away"):
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # Campos básicos se existirem
    # (não forçamos, apenas padronizamos se houver)
    basics_map = {
        "team_home": "team_home",
        "team_away": "team_away",
        "match_key": "match_key",
        "league": "league",
        "date": "date",
        "kickoff": "kickoff",
    }
    for c in list(out.columns):
        lc = str(c).strip().lower()
        if lc in basics_map and c != basics_map[lc]:
            out = out.rename(columns={c: basics_map[lc]})

    return out


def filter_real_odds(df: pd.DataFrame, debug: bool) -> pd.DataFrame:
    if df.empty:
        return df

    def row_ok(r) -> bool:
        vals = []
        for c in ("odds_home", "odds_draw", "odds_away"):
            v = r.get(c)
            if pd.notna(v) and np.isfinite(v) and v > 1.0:
                vals.append(v)
        # Exigimos pelo menos duas odds > 1.0
        return len(vals) >= 2

    mask = df.apply(row_ok, axis=1)
    kept = int(mask.sum())
    _debug(f"linhas com >=2 odds > 1.0: {kept}", debug)
    return df.loc[mask].reset_index(drop=True)


def consensus_merge(frames: List[pd.DataFrame], debug: bool) -> pd.DataFrame:
    # Estratégia simples: concat e deixa o upstream decidir merge/duplicatas.
    if not frames:
        return pd.DataFrame()
    cat = pd.concat([f for f in frames if not f.empty], ignore_index=True, sort=False)
    if cat.empty:
        return cat
    # Remover duplicatas se existir uma chave clara
    key_candidates = [c for c in ["match_key"] if c in cat.columns]
    if key_candidates:
        cat = cat.drop_duplicates(subset=key_candidates + ["odds_home", "odds_draw", "odds_away"], keep="first")
    else:
        cat = cat.drop_duplicates(keep="first")
    return cat.reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    # Entradas possíveis
    p_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    p_apifoot  = os.path.join(out_dir, "odds_apifootball.csv")

    df_theodds = normalize_columns(read_csv_safe(p_theodds, args.debug))
    df_apifoot = normalize_columns(read_csv_safe(p_apifoot, args.debug))

    if df_theodds.empty and df_apifoot.empty:
        print("[consensus-safe] AVISO: nenhum provedor retornou odds. CSV vazio gerado.")
        # Escreve arquivo vazio por compat, mas FALHA logo abaixo (exit 10)
        out_path = os.path.join(out_dir, "odds_consensus.csv")
        pd.DataFrame().to_csv(out_path, index=False)
        sys.exit(10)

    merged = consensus_merge([df_theodds, df_apifoot], args.debug)
    merged = normalize_columns(merged)  # reassegura nomes
    filtered = filter_real_odds(merged, args.debug)

    out_path = os.path.join(out_dir, "odds_consensus.csv")
    filtered.to_csv(out_path, index=False)

    n_all = len(merged)
    n_valid = len(filtered)
    print(f"[consensus-safe] consenso bruto: {n_all} linhas; válidas (>=2 odds > 1.0): {n_valid}")

    if n_valid <= 0:
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.", file=sys.stderr)
        sys.exit(10)

    print(f"[consensus-safe] OK -> {out_path} ({n_valid} linhas)")
    sys.exit(0)


if __name__ == "__main__":
    main()