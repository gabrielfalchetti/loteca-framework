#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
consensus_odds_safe.py
----------------------
Lê múltiplas fontes de odds já salvas em data/out/<rodada> e produz
um consenso por partida.

Entrada (se existirem):
- odds_theoddsapi.csv
  colunas esperadas: team_home, team_away, match_key, odds_home, odds_draw, odds_away, ...
- odds_apifootball.csv
  colunas esperadas: team_home, team_away, match_key, odds_home, odds_draw, odds_away
- odds_manual.csv (opcional)
  colunas esperadas: team_home, team_away, match_key, odds_home, odds_draw, odds_away

Saída:
- odds_consensus.csv com colunas:
  match_id, match_key, team_home, team_away, odds_home, odds_draw, odds_away, source=consensus

Observações:
- Tolerante a arquivos ausentes ou vazios.
- Normaliza nomes (strip), cria match_key quando ausente e match_id no formato "Home__Away".
- Agrega por (team_home, team_away, match_key) tirando a MÉDIA das odds.
"""

import os
import sys
import csv
import argparse
import pandas as pd


REQ_COLS = ["team_home", "team_away", "match_key", "odds_home", "odds_draw", "odds_away"]


def log(msg: str):
    print(f"[consensus] {msg}", flush=True)


def norm_team(s):
    if s is None:
        return ""
    return str(s).strip()


def build_match_key(home: str, away: str) -> str:
    h = (home or "").strip().lower().replace(" ", "-")
    a = (away or "").strip().lower().replace(" ", "-")
    return f"{h}__vs__{a}"


def build_match_id(home: str, away: str) -> str:
    return f"{(home or '').strip()}__{(away or '').strip()}"


def ensure_numeric(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def normalize_df(df_raw: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Padroniza colunas, cria match_key se ausente e mantém apenas REQ_COLS.
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=REQ_COLS)

    df = df_raw.copy()

    # Renomeações tolerantes
    ren = {}
    # Suporte a "home"/"away"
    if "home" in df.columns and "team_home" not in df.columns:
        ren["home"] = "team_home"
    if "away" in df.columns and "team_away" not in df.columns:
        ren["away"] = "team_away"
    # odds podem vir como odd_*
    if "odd_home" in df.columns and "odds_home" not in df.columns:
        ren["odd_home"] = "odds_home"
    if "odd_draw" in df.columns and "odds_draw" not in df.columns:
        ren["odd_draw"] = "odds_draw"
    if "odd_away" in df.columns and "odds_away" not in df.columns:
        ren["odd_away"] = "odds_away"

    df = df.rename(columns=ren)

    # Garante teams normalizados
    for col in ["team_home", "team_away"]:
        if col in df.columns:
            df[col] = df[col].map(norm_team)

    # Cria match_key se não existir
    if "match_key" not in df.columns:
        df["match_key"] = df.apply(
            lambda r: build_match_key(r.get("team_home", ""), r.get("team_away", "")),
            axis=1
        )
    else:
        # normaliza match_key
        df["match_key"] = df["match_key"].fillna("").map(str).str.strip()
        # se vier vazio, recomputa
        mask_empty = df["match_key"].eq("")
        if mask_empty.any():
            df.loc[mask_empty, "match_key"] = df.loc[mask_empty].apply(
                lambda r: build_match_key(r.get("team_home", ""), r.get("team_away", "")),
                axis=1
            )

    # Garante numéricos nas odds
    df = ensure_numeric(df, ["odds_home", "odds_draw", "odds_away"])

    # Mantém só o que interessa
    keep = [c for c in REQ_COLS if c in df.columns]
    df = df[keep].copy()

    # Preenche itens faltantes para não quebrar concat
    for c in REQ_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # Remove linhas sem equipes ou sem odds válidas
    df = df[
        (df["team_home"].astype(str).str.len() > 0) &
        (df["team_away"].astype(str).str.len() > 0)
    ].copy()

    any_odds = df[["odds_home", "odds_draw", "odds_away"]].notna().any(axis=1)
    df = df[any_odds].copy()

    df["__source"] = source_name
    return df


def read_optional(csv_path: str, source_name: str) -> pd.DataFrame:
    if not os.path.isfile(csv_path):
        return pd.DataFrame(columns=REQ_COLS)
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        log(f"AVISO: erro ao ler {csv_path}: {e}")
        return pd.DataFrame(columns=REQ_COLS)
    return normalize_df(df, source_name)


def resolve_out_dir(rodada_arg: str) -> str:
    # aceita tanto "data/out/<id>" quanto apenas "<id>"
    if os.path.isdir(rodada_arg):
        return rodada_arg
    candidate = os.path.join("data", "out", str(rodada_arg))
    if os.path.isdir(candidate):
        return candidate
    # se não existir, cria
    os.makedirs(candidate, exist_ok=True)
    return candidate


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID da rodada OU caminho data/out/<id>")
    args = ap.parse_args()

    out_dir = resolve_out_dir(args.rodada)

    # caminhos de entrada
    fp_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    fp_apifoot = os.path.join(out_dir, "odds_apifootball.csv")
    fp_manual  = os.path.join(out_dir, "odds_manual.csv")  # opcional

    df_t = read_optional(fp_theodds, "theoddsapi")
    df_a = read_optional(fp_apifoot, "apifootball")
    df_m = read_optional(fp_manual,  "manual")

    # Concatena apenas os não vazios
    dfs = [d for d in (df_t, df_a, df_m) if not d.empty]
    if len(dfs) == 0:
        # nada para agregar; salva esqueleto vazio com colunas finais
        out_cols = ["match_id", "match_key", "team_home", "team_away",
                    "odds_home", "odds_draw", "odds_away", "source"]
        out_path = os.path.join(out_dir, "odds_consensus.csv")
        pd.DataFrame(columns=out_cols).to_csv(out_path, index=False)
        log(f"AVISO: nenhuma fonte de odds encontrada. Gerado vazio -> {out_path}")
        return

    df_all = pd.concat(dfs, ignore_index=True)

    # Agrega (média) por partida
    grp_cols = ["team_home", "team_away", "match_key"]
    for c in grp_cols:
        if c not in df_all.columns:
            df_all[c] = pd.NA

    df_all = df_all.dropna(subset=["team_home", "team_away", "match_key"])
    # garante string
    for c in grp_cols:
        df_all[c] = df_all[c].astype(str)

    # média ignorando NaNs
    agg_df = (
        df_all
        .groupby(grp_cols, as_index=False)[["odds_home", "odds_draw", "odds_away"]]
        .mean()
    )

    # Cria match_id e fonte
    agg_df["match_id"] = agg_df.apply(lambda r: build_match_id(r["team_home"], r["team_away"]), axis=1)
    agg_df["source"] = "consensus"

    # Reordena colunas
    out = agg_df[[
        "match_id", "match_key", "team_home", "team_away",
        "odds_home", "odds_draw", "odds_away", "source"
    ]].copy()

    # Salva
    out_path = os.path.join(out_dir, "odds_consensus.csv")
    os.makedirs(out_dir, exist_ok=True)
    out.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    log(f"OK -> {out_path}")
    try:
        # amostra para logs
        print(out.head(10).to_csv(index=False))
    except Exception:
        pass


if __name__ == "__main__":
    main()