#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera features históricas para modelagem dinâmica.

Entrada (--history):
  CSV com pelo menos: data do jogo, times (home/away) e gols (home/away).
  O script aceita automaticamente vários nomes de colunas:
    - data:   date | match_date | fixture_date
    - mandante: home | home_team | hometeam | team_home
    - visitante: away | away_team | awayteam | team_away
    - gols mandante: gf | home_goals | home_score | goals_home | score_home | fthg
    - gols visitante: ga | away_goals | away_score | goals_away | score_away | ftag
    - xG (opcional):  xg_home/xg_away | home_xg/away_xg | xg_for/xg_against

Saída (--out):
  Parquet com linhas no formato “longo” (um registro por time por jogo), contendo:
    date, team, is_home, gf, ga, gd, xg_for?, xg_against?,
    ewma_gf, ewma_ga, ewma_gd, ewma_xg_for?, ewma_xg_against?, matches_cnt

Uso:
  python -m scripts.feature_engineer \
    --history data/history/results.csv \
    --out data/history/features.parquet \
    --ewma 0.20
"""

from __future__ import annotations
import sys
import argparse
import pandas as pd
from typing import Dict, Optional, Tuple

def log(level: str, msg: str) -> None:
    print(f"[features][{level}] {msg}")

# --------- Utilidades de normalização de colunas --------- #
def _norm(s: str) -> str:
    return s.strip().lower().replace(" ", "_").replace("-", "_")

def find_col(df: pd.DataFrame, candidates) -> Optional[str]:
    cmap = { _norm(c): c for c in df.columns }
    for cand in candidates:
        cn = _norm(cand)
        if cn in cmap:
            return cmap[cn]
    return None

def detect_schema(df: pd.DataFrame) -> Dict[str, str]:
    """
    Tenta mapear nomes de colunas da base para um esquema padrão.
    Retorna dicionário com chaves: date, home, away, gf, ga, xg_home?, xg_away?
    Lança ValueError se campos essenciais não puderem ser detectados.
    """
    # data
    c_date = find_col(df, ["date", "match_date", "fixture_date"])
    if not c_date:
        raise ValueError("history precisa de coluna de data (ex.: 'date').")

    # times
    c_home = find_col(df, ["home", "home_team", "hometeam", "team_home"])
    c_away = find_col(df, ["away", "away_team", "awayteam", "team_away"])
    if not c_home or not c_away:
        raise ValueError("history precisa das colunas de times (ex.: 'home' e 'away').")

    # gols
    c_gf = find_col(df, ["gf", "home_goals", "home_score", "goals_home", "score_home", "fthg"])
    c_ga = find_col(df, ["ga", "away_goals", "away_score", "goals_away", "score_away", "ftag"])
    if not c_gf or not c_ga:
        raise ValueError(
            "history must have home/away goals columns (e.g., "
            "'home_goals'/'away_goals' ou 'home_score'/'away_score' ou 'gf'/'ga')."
        )

    # xG (opcional)
    c_xg_h = find_col(df, ["xg_home", "home_xg", "xg_for"])
    c_xg_a = find_col(df, ["xg_away", "away_xg", "xg_against"])
    return {
        "date": c_date,
        "home": c_home,
        "away": c_away,
        "gf": c_gf,
        "ga": c_ga,
        "xg_home": c_xg_h or "",
        "xg_away": c_xg_a or "",
    }

# --------- Construção do formato longo e EWMA --------- #
def long_format(df: pd.DataFrame, cols: Dict[str, str]) -> pd.DataFrame:
    base_cols = [cols["date"], cols["home"], cols["away"], cols["gf"], cols["ga"]]
    tmp = df[base_cols].copy()

    # renomeia para padrão interno
    tmp.columns = ["date", "home", "away", "gf", "ga"]

    # garante tipos
    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce", utc=True)
    tmp = tmp.dropna(subset=["date", "home", "away"])
    tmp["gf"] = pd.to_numeric(tmp["gf"], errors="coerce")
    tmp["ga"] = pd.to_numeric(tmp["ga"], errors="coerce")
    tmp = tmp.dropna(subset=["gf", "ga"])

    # opcional xG
    if cols.get("xg_home"):
        tmp["xg_home"] = pd.to_numeric(df[cols["xg_home"]], errors="coerce")
    else:
        tmp["xg_home"] = pd.NA
    if cols.get("xg_away"):
        tmp["xg_away"] = pd.to_numeric(df[cols["xg_away"]], errors="coerce")
    else:
        tmp["xg_away"] = pd.NA

    # formato longo: uma linha por time por jogo
    home_rows = pd.DataFrame({
        "date": tmp["date"],
        "team": tmp["home"].astype(str).str.strip(),
        "is_home": 1,
        "gf": tmp["gf"],
        "ga": tmp["ga"],
        "gd": tmp["gf"] - tmp["ga"],
        "xg_for": tmp["xg_home"],
        "xg_against": tmp["xg_away"],
    })
    away_rows = pd.DataFrame({
        "date": tmp["date"],
        "team": tmp["away"].astype(str).str.strip(),
        "is_home": 0,
        "gf": tmp["ga"],
        "ga": tmp["gf"],
        "gd": tmp["ga"] - tmp["gf"],
        "xg_for": tmp["xg_away"],
        "xg_against": tmp["xg_home"],
    })
    out = pd.concat([home_rows, away_rows], ignore_index=True)
    out = out.sort_values(["team", "date"]).reset_index(drop=True)
    return out

def add_ewm_features(df_long: pd.DataFrame, ewma_alpha: float) -> pd.DataFrame:
    # por time, ordenado por data
    def _ewm_grp(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date")
        # contador de partidas (cumulativo)
        g["matches_cnt"] = range(1, len(g) + 1)

        # EWM sem “vazar” o jogo atual (shift antes do ewm)
        for col in ["gf", "ga", "gd", "xg_for", "xg_against"]:
            if col in g.columns:
                s = g[col].astype(float)
                g[f"ewma_{col}"] = (
                    s.shift(1).ewm(alpha=ewma_alpha, adjust=False, ignore_na=True).mean()
                )

        return g

    out = df_long.groupby("team", group_keys=False).apply(_ewm_grp)
    # após primeira partida do time, ainda pode haver NaN em ewma_*; substitui por médias simples até o ponto
    for col in ["gf", "ga", "gd", "xg_for", "xg_against"]:
        e = f"ewma_{col}"
        if e in out.columns:
            out[e] = out[e].fillna(method="ffill")
    return out

# --------- Main --------- #
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True, help="CSV com histórico de jogos finalizados")
    ap.add_argument("--out", required=True, help="Arquivo .parquet de saída")
    ap.add_argument("--ewma", type=float, default=0.20, help="Alpha do EWMA (0..1), p.ex. 0.20")
    args = ap.parse_args()

    try:
        df = pd.read_csv(args.history)
    except Exception as e:
        log("CRITICAL", f"falha lendo history: {e}")
        return 2

    if df.empty:
        log("CRITICAL", "history is empty")
        return 2

    # Detecta esquema e normaliza
    try:
        cols = detect_schema(df)
        log("INFO", f"schema detectado: date={cols['date']} home={cols['home']} away={cols['away']} "
                   f"gf={cols['gf']} ga={cols['ga']} "
                   f"xg_home={cols.get('xg_home') or '-'} xg_away={cols.get('xg_away') or '-'}")
    except ValueError as ve:
        log("CRITICAL", f"\"{ve}\"")
        return 2

    # Long format + EWMA
    df_long = long_format(df, cols)
    if df_long.empty:
        log("CRITICAL", "depois da normalizacao, nenhum jogo valido restou")
        return 2

    features = add_ewm_features(df_long, ewma_alpha=args.ewma)

    # Ordena e salva
    features = features.sort_values(["date", "team"]).reset_index(drop=True)

    try:
        # requer pyarrow/fastparquet no ambiente
        features.to_parquet(args.out, index=False)
    except Exception as e:
        log("CRITICAL", f"falha salvando parquet: {e}")
        return 2

    log("INFO", f"features salvas em {args.out} — linhas={len(features)} times={features['team'].nunique()}")
    return 0

if __name__ == "__main__":
    sys.exit(main())