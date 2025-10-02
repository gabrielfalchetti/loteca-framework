#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds de consenso a partir dos provedores disponíveis.
⚠️ Agora funciona mesmo com UM provedor só e normaliza automaticamente:
- probabilidades (0<p<1, somando ≈1) -> odds decimais (1/p)
- odds americanas (+120/-150) -> odds decimais
- vírgula decimal e sinais de % nas células

Entrada (se existirem):
  data/out/<RODADA>/odds_theoddsapi.csv
  data/out/<RODADA>/odds_apifootball.csv

Saída:
  data/out/<RODADA>/odds_consensus.csv

Uma linha é considerada "válida" se tiver ≥2 odds numéricas > 1.0.
"""

import argparse
import json
import os
import sys
from typing import List

import pandas as pd

REQUIRED_BASE_COLS = ["match_key", "team_home", "team_away"]
ODDS_COLS = ["odds_home", "odds_draw", "odds_away"]


def _log(msg: str) -> None:
    print(f"[consensus-safe] {msg}")


def _exists(p: str) -> bool:
    return os.path.isfile(p) and os.path.getsize(p) > 0


def _to_numeric_series(s: pd.Series) -> pd.Series:
    # troca vírgula por ponto e remove % antes de converter
    return pd.to_numeric(
        s.astype(str).str.replace(",", ".", regex=False).str.replace("%", "", regex=False),
        errors="coerce",
    )


def _normalize_row_to_decimal(row: pd.Series) -> pd.Series:
    """Recebe uma linha com odds_* (podem estar como prob/american/decimal) e retorna em odds decimais."""
    vals = [row[c] for c in ODDS_COLS]
    nums = [v for v in vals if pd.notna(v)]

    if len(nums) >= 2:
        # 1) detectar PROBABILIDADES: 0<p<1 e soma ≈ 1
        if all(0 < v < 1 for v in nums):
            s = sum(nums)
            if 0.95 <= s <= 1.05:
                for c in ODDS_COLS:
                    v = row[c]
                    row[c] = (1.0 / v) if pd.notna(v) and v > 0 else pd.NA
                return row

        # 2) detectar AMERICAN odds: valores como +120/-150 (|v| >= 100 costuma indicar american)
        # Usa regra por-coluna: converte apenas onde fizer sentido
        american_votes = sum(1 for v in nums if abs(v) >= 100)
        if american_votes >= 2:
            for c in ODDS_COLS:
                v = row[c]
                if pd.isna(v):
                    continue
                if v >= 100:          # +120 => 1 + 120/100 = 2.20
                    row[c] = 1.0 + (v / 100.0)
                elif v <= -100:       # -150 => 1 + 100/150 = 1.6667
                    row[c] = 1.0 + (100.0 / abs(v))
                # caso contrário deixamos como está
            return row

    # 3) já está em decimal ou não deu para inferir — mantém
    return row


def _read_provider_csv(path: str, provider: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]

    # checa colunas base
    for c in REQUIRED_BASE_COLS:
        if c not in df.columns:
            raise ValueError(f"{provider}: coluna obrigatória ausente: {c}")

    # garante colunas de odds e converte para numérico
    for c in ODDS_COLS:
        if c in df.columns:
            df[c] = _to_numeric_series(df[c])
        else:
            df[c] = pd.NA

    # cria chave de junção
    def _mk_key(r):
        h = str(r.get("team_home", "")).strip().lower()
        a = str(r.get("team_away", "")).strip().lower()
        mk = str(r.get("match_key", "")).strip().lower()
        return mk if mk else f"{h}__vs__{a}"

    df["__join_key"] = df.apply(_mk_key, axis=1)
    df["__prov"] = provider

    # normaliza linha-a-linha para odds decimais
    df[ODDS_COLS] = df.apply(_normalize_row_to_decimal, axis=1)[ODDS_COLS]

    # marca válidas (≥2 odds > 1.0)
    df["__valid"] = (
        ((df["odds_home"] > 1).astype("Int64").fillna(0))
        + ((df["odds_draw"] > 1).astype("Int64").fillna(0))
        + ((df["odds_away"] > 1).astype("Int64").fillna(0))
    ) >= 2

    return df


def _consensus(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    base = pd.concat(dfs, ignore_index=True).sort_values("__join_key").reset_index(drop=True)

    meta = (
        base.sort_values(["__join_key", "__prov"])
        .drop_duplicates("__join_key", keep="first")[["__join_key", *REQUIRED_BASE_COLS]]
    )

    agg = base.groupby("__join_key")[ODDS_COLS].mean(numeric_only=True).reset_index()

    out = meta.merge(agg, on="__join_key", how="left")

    out["__valid"] = (
        ((out["odds_home"] > 1).astype("Int64").fillna(0))
        + ((out["odds_draw"] > 1).astype("Int64").fillna(0))
        + ((out["odds_away"] > 1).astype("Int64").fillna(0))
    ) >= 2

    return out[["match_key", "team_home", "team_away", *ODDS_COLS, "__valid"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    p_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    p_apifoot = os.path.join(out_dir, "odds_apifootball.csv")

    dfs: List[pd.DataFrame] = []

    if _exists(p_theodds):
        try:
            df = _read_provider_csv(p_theodds, "theoddsapi")
            _log(f"lido odds_theoddsapi.csv -> {len(df)} linhas; válidas: {int(df['__valid'].sum())}")
            dfs.append(df)
        except Exception as e:
            _log(f"AVISO: erro lendo odds_theoddsapi.csv: {e}")

    if _exists(p_apifoot):
        try:
            df = _read_provider_csv(p_apifoot, "apifootball")
            _log(f"lido odds_apifootball.csv -> {len(df)} linhas; válidas: {int(df['__valid'].sum())}")
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

    _log(f"consenso bruto: {total} linhas; válidas (>=2 odds > 1.0): {valid}")

    if valid == 0:
        _log("ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    cons = cons[cons["__valid"]].drop(columns=["__valid"]).reset_index(drop=True)

    out_path = os.path.join(out_dir, "odds_consensus.csv")
    cons.to_csv(out_path, index=False)
    _log(f"OK -> {out_path} ({len(cons)} linhas)")

    if args.debug:
        dbg = {"linhas_total": total, "linhas_validas": len(cons)}
        print(json.dumps({"consensus": dbg}, ensure_ascii=False))


if __name__ == "__main__":
    main()