#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import json
from typing import Tuple, List
import pandas as pd
import numpy as np

def log(msg: str):
    print(f"[consensus-safe] {msg}", flush=True)

def resolve_out_dir(rodada: str) -> str:
    """
    Se 'rodada' for um caminho (contiver '/' ou começar por 'data/'), usa como está.
    Caso contrário, assume que é um identificador e resolve para data/out/<rodada>.
    """
    if rodada is None or str(rodada).strip() == "":
        raise ValueError("valor vazio para --rodada")
    r = rodada.strip()
    if r.startswith("data/") or (os.sep in r):
        return r
    return os.path.join("data", "out", r)

def load_csv_if_exists(path: str, provider: str, debug: bool=False) -> pd.DataFrame:
    if not os.path.exists(path):
        log(f"AVISO: arquivo não encontrado: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        if debug:
            log(f"lido {os.path.basename(path)} -> {len(df)} linhas")
        df["__provider"] = provider
        return df
    except Exception as e:
        log(f"ERRO ao ler {path}: {e}")
        return pd.DataFrame()

def normalize_columns(df: pd.DataFrame, provider: str, debug: bool=False) -> pd.DataFrame:
    """
    Normaliza colunas esperadas:
      - match_key, team_home, team_away, odds_home, odds_draw, odds_away
    Mantém somente essas (se existirem).
    """
    if df.empty:
        return df
    # Mapeamento direto esperado
    expected = ["match_key", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
    # Alguns provedores podem vir com nomes levemente diferentes (fallback leve)
    aliases = {
        "home": "team_home",
        "away": "team_away",
        "odd_home": "odds_home",
        "odd_draw": "odds_draw",
        "odd_away": "odds_away"
    }

    cols = {c: c for c in df.columns}
    for k, v in aliases.items():
        if k in df.columns and v not in df.columns:
            cols[v] = k  # permitir selecionar pelo alias

    # Reorganiza
    keep = []
    for c in expected:
        if c in df.columns:
            keep.append(c)
        elif c in cols and cols[c] in df.columns:
            # já está
            keep.append(cols[c])
        else:
            # se faltar match_key, tenta montar simples (home__vs__away)
            if c == "match_key" and ("team_home" in df.columns and "team_away" in df.columns):
                df["match_key"] = (
                    df["team_home"].astype(str).str.strip().str.lower()
                    + "__vs__" +
                    df["team_away"].astype(str).str.strip().str.lower()
                )
                keep.append("match_key")

    # Garante existência das odds, mesmo que vazias
    for oc in ["odds_home", "odds_draw", "odds_away"]:
        if oc not in df.columns:
            df[oc] = np.nan
            keep.append(oc)

    # Garante time_home/away
    for tc in ["team_home", "team_away"]:
        if tc not in df.columns:
            df[tc] = np.nan
            keep.append(tc)

    # Garante match_key
    if "match_key" not in df.columns:
        # último recurso
        df["match_key"] = (
            df.get("team_home", pd.Series(dtype=str)).astype(str).str.strip().str.lower()
            + "__vs__" +
            df.get("team_away", pd.Series(dtype=str)).astype(str).str.strip().str.lower()
        )
        keep.append("match_key")

    # Seleciona colunas finais na ordem correta
    final_cols = ["match_key", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
    df = df[final_cols].copy()
    df["__provider"] = provider
    return df

def pick_best_row(rows: pd.DataFrame) -> pd.Series:
    """
    Escolhe a melhor linha entre provedores para um mesmo match_key.
    Critérios:
      1) mais odds não-nulas (>1.0)
      2) maior média de odds disponíveis (proxy de melhor preço)
    """
    def score_row(r: pd.Series) -> Tuple[int, float]:
        vals = []
        for c in ["odds_home", "odds_draw", "odds_away"]:
            v = r.get(c, np.nan)
            ok = (pd.notna(v)) and (float(v) > 1.0)
            vals.append(float(v) if ok else np.nan)
        count = np.sum(~np.isnan(vals))
        mean = np.nanmean(vals) if count > 0 else -1.0
        return int(count), float(mean)

    best_idx = None
    best_score = (-1, -1.0)
    for idx, r in rows.iterrows():
        s = score_row(r)
        if s > best_score:
            best_score = s
            best_idx = idx
    return rows.loc[best_idx]

def build_consensus(dfs: List[pd.DataFrame], debug: bool=False) -> pd.DataFrame:
    base = pd.concat([df for df in dfs if not df.empty], ignore_index=True)
    if base.empty:
        return base

    # Garante tipos numéricos em odds
    for c in ["odds_home", "odds_draw", "odds_away"]:
        base[c] = pd.to_numeric(base[c], errors="coerce")

    # Agrupa por match_key e escolhe a melhor linha
    out_rows = []
    for mk, group in base.groupby("match_key", dropna=False):
        best = pick_best_row(group)
        out_rows.append(best)

    out = pd.DataFrame(out_rows).reset_index(drop=True)

    # Ordena por nome do jogo para estabilidade
    out = out.sort_values(by=["team_home", "team_away"], na_position="last", kind="mergesort")
    return out

def main():
    parser = argparse.ArgumentParser(description="Gera odds de consenso a partir das fontes disponíveis.")
    parser.add_argument("--rodada", required=True, help="Identificador da rodada (ex: 2025-10-04_1214) OU um caminho de saída (ex: data/out/XYZ)")
    parser.add_argument("--debug", action="store_true", help="Modo verboso")
    args = parser.parse_args()

    try:
        out_dir = resolve_out_dir(args.rodada)
    except Exception as e:
        log(f"ERRO: {e}")
        sys.exit(1)

    os.makedirs(out_dir, exist_ok=True)
    if args.debug:
        log(f"out_dir: {out_dir}")

    # caminhos das fontes (SEM prefixar novamente 'data/out')
    f_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    f_apifoot = os.path.join(out_dir, "odds_apifootball.csv")

    # carrega
    df1 = load_csv_if_exists(f_theodds, provider="theoddsapi", debug=args.debug)
    df2 = load_csv_if_exists(f_apifoot, provider="apifootball", debug=args.debug)

    # se nenhuma fonte existir: erro
    if df1.empty and df2.empty:
        log("ERRO: nenhuma fonte de odds disponível.")
        sys.exit(1)

    # normaliza
    df1 = normalize_columns(df1, "theoddsapi", debug=args.debug)
    df2 = normalize_columns(df2, "apifootball", debug=args.debug)

    # constrói consenso (aceita 1 ou 2 fontes)
    consensus = build_consensus([df1, df2], debug=args.debug)

    if consensus.empty:
        log("ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(1)

    out_path = os.path.join(out_dir, "odds_consensus.csv")
    consensus.to_csv(out_path, index=False)
    log(f"OK -> {out_path} ({len(consensus)} linhas) | mapping: padrão (match_key, team_home, team_away, odds_home, odds_draw, odds_away)")

if __name__ == "__main__":
    main()