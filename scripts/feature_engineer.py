#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
feature_engineer.py
-------------------
Gera features (covariáveis) a partir do histórico de resultados.

Interface (compatível com o workflow):
    python -m scripts.feature_engineer \
        --history data/history/results.csv \
        --out data/history/features.parquet \
        --ewma 0.20

Entrada (CSV) — produzido pelo update_history.py:
    date, league, season, home, away, home_goals, away_goals, source, match_id

Saída (Parquet):
    Uma linha por equipe por partida, com features *sem vazamento* (shift=1):
        date, league, season, match_id, team, opponent, home_away,
        gf, ga, points,
        ewma_gf, ewma_ga, ewma_points,
        last5_gf_mean, last5_ga_mean, last5_points_mean

Observações:
- Sem xG ‘de verdade’, geramos features pelas contagens de gols e pontos.
- EWMA usa alpha indicado por --ewma e é calculado por equipe (reset por season).
- Todos os agregados são calculados ANTES do jogo (shift=1), evitando vazamento.

Requisitos:
    pandas, pyarrow (para gravar parquet)
"""

from __future__ import annotations

import argparse
import os
import sys
import pandas as pd


def _int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def long_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte partidas (home/away) para formato longo (uma linha por equipe).
    """
    rows = []

    for _, r in df.iterrows():
        # linha do mandante (time = home)
        rows.append(
            {
                "date": r["date"],
                "league": r["league"],
                "season": r["season"],
                "match_id": r["match_id"],
                "team": r["home"],
                "opponent": r["away"],
                "home_away": "H",
                "gf": _int(r["home_goals"], 0),
                "ga": _int(r["away_goals"], 0),
            }
        )
        # linha do visitante (time = away)
        rows.append(
            {
                "date": r["date"],
                "league": r["league"],
                "season": r["season"],
                "match_id": r["match_id"],
                "team": r["away"],
                "opponent": r["home"],
                "home_away": "A",
                "gf": _int(r["away_goals"], 0),
                "ga": _int(r["home_goals"], 0),
            }
        )

    out = pd.DataFrame(rows)
    # pontos (3 vitória, 1 empate, 0 derrota)
    out["points"] = (out["gf"] > out["ga"]).astype(int) * 3 + (out["gf"] == out["ga"]).astype(int) * 1
    return out


def add_rolling_features(df_long: pd.DataFrame, alpha: float) -> pd.DataFrame:
    """
    Calcula EWMA e médias móveis (janela curta) por equipe/season,
    usando SHIFT=1 para evitar vazamento (só info pré-jogo).
    """
    df = df_long.copy()
    # ordenação por time no tempo
    df["date_ts"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(by=["season", "team", "date_ts", "match_id"]).reset_index(drop=True)

    def _group_calc(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        # shift para que a observação do próprio jogo fique fora dos agregados
        gf = g["gf"].shift(1)
        ga = g["ga"].shift(1)
        pts = g["points"].shift(1)

        # EWMA (exponencial)
        g["ewma_gf"] = gf.ewm(alpha=alpha, adjust=False, min_periods=1).mean()
        g["ewma_ga"] = ga.ewm(alpha=alpha, adjust=False, min_periods=1).mean()
        g["ewma_points"] = pts.ewm(alpha=alpha, adjust=False, min_periods=1).mean()

        # Rolling janela 5 (média simples)
        g["last5_gf_mean"] = gf.rolling(window=5, min_periods=1).mean()
        g["last5_ga_mean"] = ga.rolling(window=5, min_periods=1).mean()
        g["last5_points_mean"] = pts.rolling(window=5, min_periods=1).mean()

        # Preenche inicio de temporada (sem histórico) com 0 de maneira conservadora
        for c in [
            "ewma_gf",
            "ewma_ga",
            "ewma_points",
            "last5_gf_mean",
            "last5_ga_mean",
            "last5_points_mean",
        ]:
            g[c] = g[c].fillna(0.0)

        return g

    df = df.groupby(["season", "team"], group_keys=False).apply(_group_calc)

    # Limpeza
    df = df.drop(columns=["date_ts"])
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="CSV com resultados históricos (results.csv)")
    ap.add_argument("--out", required=True, help="Arquivo parquet de saída")
    ap.add_argument("--ewma", type=float, default=0.20, help="Alpha do EWMA (0..1). Padrão=0.20")
    args = ap.parse_args()

    hist_path = args.history
    out_path = args.out
    alpha = float(args.ewma)

    if not os.path.exists(hist_path):
        print(f"[features][ERROR] Histórico não encontrado: {hist_path}")
        # ainda assim, gera um parquet vazio com schema esperado para não quebrar o pipeline
        empty = pd.DataFrame(
            columns=[
                "date",
                "league",
                "season",
                "match_id",
                "team",
                "opponent",
                "home_away",
                "gf",
                "ga",
                "points",
                "ewma_gf",
                "ewma_ga",
                "ewma_points",
                "last5_gf_mean",
                "last5_ga_mean",
                "last5_points_mean",
            ]
        )
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        empty.to_parquet(out_path, index=False)
        print(f"[features][WARN] Saída vazia gerada (schema) em: {out_path}")
        return 0

    # Carrega histórico
    try:
        df_hist = pd.read_csv(hist_path, dtype=str)
    except Exception as e:
        print(f"[features][ERROR] Falha ao ler {hist_path}: {e}")
        # Gera parquet vazio com schema
        empty = pd.DataFrame(
            columns=[
                "date",
                "league",
                "season",
                "match_id",
                "team",
                "opponent",
                "home_away",
                "gf",
                "ga",
                "points",
                "ewma_gf",
                "ewma_ga",
                "ewma_points",
                "last5_gf_mean",
                "last5_ga_mean",
                "last5_points_mean",
            ]
        )
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        empty.to_parquet(out_path, index=False)
        print(f"[features][WARN] Saída vazia gerada (schema) em: {out_path}")
        return 0

    # Normaliza colunas essenciais
    needed = [
        "date",
        "league",
        "season",
        "home",
        "away",
        "home_goals",
        "away_goals",
        "match_id",
    ]
    for c in needed:
        if c not in df_hist.columns:
            df_hist[c] = "" if c not in ("home_goals", "away_goals") else 0

    # Converte gols para inteiro
    df_hist["home_goals"] = pd.to_numeric(df_hist["home_goals"], errors="coerce").fillna(0).astype(int)
    df_hist["away_goals"] = pd.to_numeric(df_hist["away_goals"], errors="coerce").fillna(0).astype(int)

    # Tenta ordenar por data para estabilidade
    df_hist["_d"] = pd.to_datetime(df_hist["date"], errors="coerce")
    df_hist = df_hist.sort_values(by=["_d", "league", "home", "away"]).drop(columns=["_d"])

    # Wide -> Long
    df_long = long_format(
        df_hist[
            [
                "date",
                "league",
                "season",
                "home",
                "away",
                "home_goals",
                "away_goals",
                "match_id",
            ]
        ]
    )

    # Agregados/EWMA sem vazamento
    df_feat = add_rolling_features(df_long, alpha=alpha)

    # Salva parquet
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    df_feat.to_parquet(out_path, index=False)
    print(f"[features][OK] Features salvas em: {out_path} (linhas={len(df_feat)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())