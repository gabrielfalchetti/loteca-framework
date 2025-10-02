#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds de consenso a partir dos provedores disponíveis.
Agora funciona mesmo que apenas UM provedor tenha retornado odds válidas.

Entrada esperada (se existirem):
  data/out/<RODADA>/odds_theoddsapi.csv
  data/out/<RODADA>/odds_apifootball.csv

Saída:
  data/out/<RODADA>/odds_consensus.csv

Uma linha é considerada "válida" se tiver pelo menos 2 odds numéricas > 1.0
(em geral 1x2 → home/draw/away).
"""

import argparse
import json
import os
import sys
from typing import List, Optional

import pandas as pd


REQUIRED_BASE_COLS = ["match_key", "team_home", "team_away"]
ODDS_COLS = ["odds_home", "odds_draw", "odds_away"]


def _log(msg: str) -> None:
    print(f"[consensus-safe] {msg}")


def _exists(p: str) -> bool:
    return os.path.isfile(p) and os.path.getsize(p) > 0


def _read_provider_csv(path: str, provider: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # padroniza nomes em minúsculo
    df.columns = [c.strip().lower() for c in df.columns]
    # garante colunas base
    for c in REQUIRED_BASE_COLS:
        if c not in df.columns:
            raise ValueError(f"{provider}: coluna obrigatória ausente: {c}")

    # odds → numéricas (ponto decimal). vírgulas viram ponto, strings viram NaN
    for c in ODDS_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(
                df[c].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )
        else:
            df[c] = pd.NA

    # __join_key para merge robusto
    def _mk_key(r):
        h = str(r.get("team_home", "")).strip().lower()
        a = str(r.get("team_away", "")).strip().lower()
        mk = str(r.get("match_key", "")).strip().lower()
        if mk:
            return mk
        return f"{h}__vs__{a}"

    df["__join_key"] = df.apply(_mk_key, axis=1)
    df["__prov"] = provider
    # indica se linha tem pelo menos 2 odds > 1
    df["__valid"] = (
        ((df["odds_home"] > 1).astype("Int64").fillna(0))
        + ((df["odds_draw"] > 1).astype("Int64").fillna(0))
        + ((df["odds_away"] > 1).astype("Int64").fillna(0))
    ) >= 2

    return df


def _consensus(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    # une por __join_key mantendo a primeira ocorrência dos metadados
    base = (
        pd.concat(dfs, ignore_index=True)
        .sort_values(["__join_key"])
        .reset_index(drop=True)
    )

    # mantém primeira info de times/chave por jogo
    meta = (
        base.sort_values(["__join_key", "__prov"])
        .drop_duplicates("__join_key", keep="first")[["__join_key", *REQUIRED_BASE_COLS]]
    )

    # agrega odds por média entre provedores que trouxeram valor
    agg = (
        base.groupby("__join_key")[ODDS_COLS]
        .mean(numeric_only=True)
        .reset_index()
    )

    out = meta.merge(agg, on="__join_key", how="left")

    # marca válidas (>= 2 odds > 1)
    out["__valid"] = (
        ((out["odds_home"] > 1).astype("Int64").fillna(0))
        + ((out["odds_draw"] > 1).astype("Int64").fillna(0))
        + ((out["odds_away"] > 1).astype("Int64").fillna(0))
    ) >= 2

    # ordena bonito
    out = out[["match_key", "team_home", "team_away", *ODDS_COLS, "__valid"]]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Identificador da rodada/pasta")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    # paths de provedores
    p_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    p_apifoot = os.path.join(out_dir, "odds_apifootball.csv")

    providers_loaded = []
    dfs: List[pd.DataFrame] = []

    if _exists(p_theodds):
        try:
            df = _read_provider_csv(p_theodds, "theoddsapi")
            _log(f"lido odds_theoddsapi.csv -> {len(df)} linhas; válidas: {int(df['__valid'].sum())}")
            providers_loaded.append("theoddsapi")
            dfs.append(df)
        except Exception as e:
            _log(f"AVISO: erro lendo odds_theoddsapi.csv: {e}")

    if _exists(p_apifoot):
        try:
            df = _read_provider_csv(p_apifoot, "apifootball")
            _log(f"lido odds_apifootball.csv -> {len(df)} linhas; válidas: {int(df['__valid'].sum())}")
            providers_loaded.append("apifootball")
            dfs.append(df)
        except Exception as e:
            _log(f"AVISO: erro lendo odds_apifootball.csv: {e}")

    if not dfs:
        _log("AVISO: nenhum CSV de odds encontrado em data/out/<RODADA>.")
        _log("consenso bruto: 0 linhas; válidas (>=2 odds > 1.0): 0")
        _log("ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    cons = _consensus(dfs)
    total = len(cons)
    valid = int(cons["__valid"].sum())

    if valid == 0:
        _log(f"consenso bruto: {total} linhas; válidas (>=2 odds > 1.0): 0")
        _log("ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    # mantém só válidas
    cons = cons[cons["__valid"]].drop(columns=["__valid"]).reset_index(drop=True)

    out_path = os.path.join(out_dir, "odds_consensus.csv")
    cons.to_csv(out_path, index=False)
    _log(f"OK -> {out_path} ({len(cons)} linhas)")
    if args.debug:
        dbg = {
            "providers": providers_loaded,
            "linhas_total": total,
            "linhas_validas": len(cons),
        }
        print(json.dumps({"consensus": dbg}, ensure_ascii=False))

if __name__ == "__main__":
    main()