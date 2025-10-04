#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera previsões puramente a partir das odds de mercado (baseline).
Leitura preferencial: data/out/<rodada>/odds_consensus.csv
Fallbacks (se necessário): data/out/<rodada>/odds_theoddsapi.csv

Saída: data/out/<rodada>/predictions_market.csv
Colunas: match_key, team_home, team_away, prob_home, prob_draw, prob_away, pred, pred_conf
"""

import argparse
import json
import os
import sys
from typing import Tuple, Optional

import pandas as pd
import numpy as np


def log(msg: str, debug: bool):
    if debug:
        print(f"[predict] {msg}")


def implied_probs_overround(row: pd.Series) -> Tuple[float, float, float]:
    """
    Converte odds decimais em probabilidades implícitas com correção de overround.
    Retorna (prob_home, prob_draw, prob_away). Se não houver 3 odds válidas (>1), retorna NaN.
    """
    oh, od, oa = row.get("odds_home"), row.get("odds_draw"), row.get("odds_away")
    if any(pd.isna([oh, od, oa])) or (oh is None or oh <= 1) or (od is None or od <= 1) or (oa is None or oa <= 1):
        return (np.nan, np.nan, np.nan)
    inv = np.array([1.0 / oh, 1.0 / od, 1.0 / oa], dtype=float)
    s = inv.sum()
    if s <= 0:
        return (np.nan, np.nan, np.nan)
    p = inv / s
    return (float(p[0]), float(p[1]), float(p[2]))


def ensure_columns(df: pd.DataFrame, mapping_hint: Optional[str] = None) -> pd.DataFrame:
    """
    Garante que o DataFrame possua as colunas padronizadas:
    match_key, team_home, team_away, odds_home, odds_draw, odds_away

    Se já estiverem presentes, retorna como está.
    Se vier de theoddsapi.csv, tenta mapear.
    """
    needed = {"match_key", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"}
    if needed.issubset(df.columns):
        return df[list(needed)]

    # tentativas de mapeamento comuns
    cand_maps = []

    # map padrão (já ok)
    cand_maps.append({
        "match_key": "match_key",
        "team_home": "team_home",
        "team_away": "team_away",
        "odds_home": "odds_home",
        "odds_draw": "odds_draw",
        "odds_away": "odds_away",
    })

    # alguns CSVs podem vir com nomes alternativos
    cand_maps.append({
        "match_key": "__join_key" if "__join_key" in df.columns else "match_key",
        "team_home": "home_team" if "home_team" in df.columns else "team_home",
        "team_away": "away_team" if "away_team" in df.columns else "team_away",
        "odds_home": "home_odds" if "home_odds" in df.columns else "odds_home",
        "odds_draw": "draw_odds" if "draw_odds" in df.columns else "odds_draw",
        "odds_away": "away_odds" if "away_odds" in df.columns else "odds_away",
    })

    # mapping do theoddsapi “seguro”
    cand_maps.append({
        "match_key": "match_key",
        "team_home": "team_home",
        "team_away": "team_away",
        "odds_home": "odds_home",
        "odds_draw": "odds_draw",
        "odds_away": "odds_away",
    })

    for m in cand_maps:
        if set(m.values()).issubset(df.columns):
            out = df[list(m.values())].copy()
            out.columns = list(m.keys())
            return out

    # não foi possível mapear — retorna apenas as colunas existentes (e o chamador lida)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Identificador da rodada, ex: 2025-09-27_1213")
    parser.add_argument("--debug", action="store_true", help="Logs verbosos")
    args = parser.parse_args()

    rodada = args.rodada
    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)

    # fontes (em ordem de preferência)
    consensus_path = os.path.join(out_dir, "odds_consensus.csv")
    theoddsapi_path = os.path.join(out_dir, "odds_theoddsapi.csv")

    source = None
    df = pd.DataFrame()

    # tenta consensus primeiro
    if os.path.exists(consensus_path):
        try:
            df = pd.read_csv(consensus_path)
            source = "odds_consensus.csv"
            log(f"lido {source} -> {len(df)} linhas", args.debug)
        except Exception as e:
            print(f"[predict] ERRO ao ler {consensus_path}: {e}")

    # fallback: theoddsapi
    if df.empty and os.path.exists(theoddsapi_path):
        try:
            df = pd.read_csv(theoddsapi_path)
            source = "odds_theoddsapi.csv"
            log(f"lido {source} -> {len(df)} linhas", args.debug)
        except Exception as e:
            print(f"[predict] ERRO ao ler {theoddsapi_path}: {e}")

    out_path = os.path.join(out_dir, "predictions_market.csv")

    # se nada foi encontrado, ainda assim geramos um CSV vazio bem formatado
    if df.empty:
        print(f"[predict] AVISO: nenhuma fonte de odds encontrada em {out_dir}. Gerando vazio.")
        pd.DataFrame(columns=[
            "match_key", "team_home", "team_away",
            "prob_home", "prob_draw", "prob_away", "pred", "pred_conf"
        ]).to_csv(out_path, index=False)
        return 0

    # garante colunas padronizadas (ou falha suave)
    df = ensure_columns(df)

    required = {"match_key", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"}
    missing = required - set(df.columns)
    if missing:
        print(f"[predict] ERRO: colunas ausentes para predição: {sorted(missing)}")
        # escreve arquivo vazio para manter pipeline saudável
        pd.DataFrame(columns=[
            "match_key", "team_home", "team_away",
            "prob_home", "prob_draw", "prob_away", "pred", "pred_conf"
        ]).to_csv(out_path, index=False)
        return 0

    # remove linhas sem pelo menos 2 odds válidas > 1 (para termos base mínima)
    def valid_row(r) -> bool:
        vals = [r["odds_home"], r["odds_draw"], r["odds_away"]]
        ok = sum([isinstance(v, (int, float)) and v and v > 1 for v in vals])
        return ok >= 2

    df = df[df.apply(valid_row, axis=1)].copy()
    if df.empty:
        print("[predict] AVISO: sem linhas válidas (>=2 odds > 1). Gerando vazio.")
        pd.DataFrame(columns=[
            "match_key", "team_home", "team_away",
            "prob_home", "prob_draw", "prob_away", "pred", "pred_conf"
        ]).to_csv(out_path, index=False)
        return 0

    # calcula probabilidades implícitas
    probs = df.apply(implied_probs_overround, axis=1, result_type="expand")
    probs.columns = ["prob_home", "prob_draw", "prob_away"]
    df_pred = pd.concat([df[["match_key", "team_home", "team_away"]], probs], axis=1)

    # escolhe a maior probabilidade como palpite
    def pick_pred(r: pd.Series):
        arr = np.array([r["prob_home"], r["prob_draw"], r["prob_away"]], dtype=float)
        if np.any(np.isnan(arr)):
            return pd.Series({"pred": np.nan, "pred_conf": np.nan})
        i = int(np.argmax(arr))
        return pd.Series({"pred": ["HOME", "DRAW", "AWAY"][i], "pred_conf": float(arr[i])})

    picks = df_pred.apply(pick_pred, axis=1)
    df_pred = pd.concat([df_pred, picks], axis=1)

    # salva
    df_pred.to_csv(out_path, index=False)

    # logs
    amostra = df_pred.head(5).to_dict(orient="records")
    print(f"[predict] AMOSTRA (top 5): {json.dumps(amostra, ensure_ascii=False)}")
    print(f"[predict] OK -> {out_path} ({len(df_pred)} linhas; válidas p/ predição: {df_pred['pred'].notna().sum()})")
    if source:
        log(f"fonte utilizada: {source}", args.debug)

    return 0


if __name__ == "__main__":
    sys.exit(main())