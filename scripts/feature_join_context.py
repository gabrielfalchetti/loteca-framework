#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Junta features de contexto para modelagem/kelly.

Entradas (estritas):
- <rodada>/matches_whitelist.csv  [match_id,home,away]  (mapeadas para team_home/team_away)
- <rodada>/features_univariado.csv  [match_id, ...]  (OBRIGATÓRIO)
- Pelo menos uma entre:
    * <rodada>/features_bivariado.csv [match_id, ...]
    * <rodada>/features_xg.csv        [match_id, ...]

O arquivo resultante:
- <rodada>/context_features.csv
"""

import argparse
import os
import sys
import pandas as pd


def die(msg: str, code: int = 28):
    print(f"[context][ERRO] {msg}", file=sys.stderr)
    sys.exit(code)


def read_required_csv(path: str, name: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        die(f"Arquivo obrigatório ausente/vazio: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"Falha ao ler {name}: {e}")
    if df.empty:
        die(f"{name} sem linhas úteis: {path}")
    return df


def read_optional_csv(path: str) -> pd.DataFrame | None:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    try:
        df = pd.read_csv(path)
    except Exception:
        return None
    return df if not df.empty else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada.")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    uni_path = os.path.join(rodada, "features_univariado.csv")
    bi_path  = os.path.join(rodada, "features_bivariado.csv")
    xg_path  = os.path.join(rodada, "features_xg.csv")
    out_path = os.path.join(rodada, "context_features.csv")

    wl = read_required_csv(wl_path, "whitelist")
    # normaliza nomes (alguns passos anteriores usam home/away; outros team_home/team_away)
    if {"team_home", "team_away"}.issubset(wl.columns):
        wl = wl.rename(columns={"team_home": "home", "team_away": "away"})
    req_wl = {"match_id", "home", "away"}
    missing = req_wl - set(wl.columns)
    if missing:
        die(f"Whitelist precisa conter colunas {sorted(req_wl)}; faltando {sorted(missing)}")

    uni = read_required_csv(uni_path, "features_univariado.csv")
    if "match_id" not in uni.columns:
        die("features_univariado.csv precisa conter 'match_id'")

    bi = read_optional_csv(bi_path)
    xg = read_optional_csv(xg_path)

    if bi is None and xg is None:
        die("Pelo menos um dos arquivos precisa existir: features_bivariado.csv OU features_xg.csv")

    if bi is not None and "match_id" not in bi.columns:
        die("features_bivariado.csv precisa conter 'match_id'")
    if xg is not None and "match_id" not in xg.columns:
        die("features_xg.csv precisa conter 'match_id'")

    # começa a base pela whitelist (garante ordem/escopo dos jogos)
    base = wl.copy()
    print(f"[context] Base (whitelist) linhas={len(base)}")

    # join com univariado
    df = base.merge(uni, on="match_id", how="left", suffixes=("", "_uni"))
    if df["match_id"].isna().any():
        die("Após merge com univariado houve perda de integridade em 'match_id'")

    # join com bivariado (se houver)
    if bi is not None:
        cols_overlap = set(df.columns).intersection(set(bi.columns)) - {"match_id"}
        bi_join = bi.rename(columns={c: f"{c}_bi" for c in cols_overlap})
        df = df.merge(bi_join, on="match_id", how="left")

    # join com xg (se houver)
    if xg is not None:
        cols_overlap = set(df.columns).intersection(set(xg.columns)) - {"match_id"}
        xg_join = xg.rename(columns={c: f"{c}_xg" for c in cols_overlap})
        df = df.merge(xg_join, on="match_id", how="left")

    # sanity check de cobertura
    if df.isna().all(axis=1).any():
        # identifica linhas completamente vazias além de match_id/home/away
        non_key = [c for c in df.columns if c not in {"match_id", "home", "away"}]
        mask = df[non_key].isna().all(axis=1)
        vazios = df.loc[mask, ["match_id", "home", "away"]]
        if not vazios.empty:
            die(f"Linhas sem features após junção (amostra):\n{vazios.head(5).to_string(index=False)}")

    # grava
    os.makedirs(rodada, exist_ok=True)
    df.to_csv(out_path, index=False)
    if args.debug:
        print(f"[context] gravado {out_path} ({len(df)} linhas)")
        print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()