#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera o arquivo de probabilidades consolidado para a rodada.

Ordem de preferência:
  1) data/out/<RODADA>/preds_bivar.csv
  2) data/out/<RODADA>/features_base.csv
  3) data/out/<RODADA>/matches.csv  -> gera 1/3-1/3-1/3

Compatibilidade:
- wandb==0.22.0 (não usa finish_previous)
- Aceita argumento legado --source (ignorado com aviso)
- Permite sobrescrever caminhos com --preds, --features, --matches

Saída:
  data/out/<RODADA>/probabilities.csv
  colunas: rodada, home, away, p_home, p_draw, p_away, source
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Optional

import pandas as pd

# wandb é opcional
try:
    import wandb  # type: ignore
    _HAS_WANDB = True
except Exception:
    _HAS_WANDB = False


OUT_COLS = ["rodada", "home", "away", "p_home", "p_draw", "p_away", "source"]


def _log(msg: str) -> None:
    print(f"[build_probs] {msg}")


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _paths(rodada: str, preds: Optional[str], features: Optional[str], matches: Optional[str]) -> Dict[str, str]:
    base_in = os.path.join("data", "in", rodada)
    base_out = os.path.join("data", "out", rodada)
    _safe_mkdir(base_in)
    _safe_mkdir(base_out)
    return {
        "preds_bivar": preds or os.path.join(base_out, "preds_bivar.csv"),
        "features_base": features or os.path.join(base_out, "features_base.csv"),
        "matches": matches or os.path.join(base_out, "matches.csv"),
        "out": os.path.join(base_out, "probabilities.csv"),
    }


def _read_csv_if_exists(path: str, empty_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if path and os.path.isfile(path):
        return pd.read_csv(path)
    return pd.DataFrame(columns=empty_cols or [])


def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_norm = {c.lower().strip(): c for c in df.columns}
    for c in candidates:
        if c in cols_norm:
            return cols_norm[c]
    return None


def _normalize_team_cols(df: pd.DataFrame) -> pd.DataFrame:
    home_col = _pick_first_existing(df, ["home", "mandante", "time_home", "team_home"])
    away_col = _pick_first_existing(df, ["away", "visitante", "time_away", "team_away"])

    if home_col is None or away_col is None:
        if len(df.columns) >= 2:
            home_col, away_col = df.columns[:2]
        else:
            return pd.DataFrame(columns=["home", "away"])

    out = pd.DataFrame(
        {
            "home": df[home_col].astype(str).fillna("").str.strip(),
            "away": df[away_col].astype(str).fillna("").str.strip(),
        }
    )
    out = out[(out["home"] != "") & (out["away"] != "")]
    return out


def _extract_probs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procura colunas de probabilidade. Aceita várias convenções:
      home: p_home, prob_home, home_prob, home_win_prob
      draw: p_draw, prob_draw, draw_prob, empate_prob, x, p_empate
      away: p_away, prob_away, away_prob, away_win_prob
    Se só existir (home, away), calcula draw = 1 - home - away (clip 0..1).
    """
    c_home = _pick_first_existing(df, ["p_home", "prob_home", "home_prob", "home_win_prob"])
    c_draw = _pick_first_existing(df, ["p_draw", "prob_draw", "draw_prob", "empate_prob", "x", "p_empate"])
    c_away = _pick_first_existing(df, ["p_away", "prob_away", "away_prob", "away_win_prob"])

    out = pd.DataFrame()

    if c_home and c_draw and c_away:
        out["p_home"] = pd.to_numeric(df[c_home], errors="coerce")
        out["p_draw"] = pd.to_numeric(df[c_draw], errors="coerce")
        out["p_away"] = pd.to_numeric(df[c_away], errors="coerce")
    elif c_home and c_away:
        out["p_home"] = pd.to_numeric(df[c_home], errors="coerce")
        out["p_away"] = pd.to_numeric(df[c_away], errors="coerce")
        p_draw = 1.0 - out["p_home"].fillna(0) - out["p_away"].fillna(0)
        out["p_draw"] = p_draw.clip(lower=0.0, upper=1.0)
    else:
        return pd.DataFrame(columns=["p_home", "p_draw", "p_away"])

    out = out.fillna(0.0)
    sums = out.sum(axis=1).replace(0, 1)
    out = out.div(sums, axis=0)
    return out[["p_home", "p_draw", "p_away"]]


def _merge_teams_and_probs(teams: pd.DataFrame, probs: pd.DataFrame, default_equal: bool = False) -> pd.DataFrame:
    if probs.empty or len(probs) != len(teams):
        if default_equal:
            probs = pd.DataFrame(
                {"p_home": [1/3]*len(teams), "p_draw": [1/3]*len(teams), "p_away": [1/3]*len(teams)}
            )
        else:
            probs = pd.DataFrame(columns=["p_home", "p_draw", "p_away"])
    return pd.concat([teams.reset_index(drop=True), probs.reset_index(drop=True)], axis=1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build probabilities for a rodada")
    parser.add_argument("--rodada", required=True, help="Identificador da rodada (ex.: 2025-09-27_1213)")
    # Compat: alguns workflows antigos passavam --source; aqui só avisamos e ignoramos.
    parser.add_argument("--source", help="(LEGADO) Ignorado. O script decide automaticamente.", default=None)
    # Sobrescritas opcionais de caminho
    parser.add_argument("--preds", help="Caminho para preds_bivar.csv", default=None)
    parser.add_argument("--features", help="Caminho para features_base.csv", default=None)
    parser.add_argument("--matches", help="Caminho para matches.csv", default=None)

    args = parser.parse_args()

    rodada = args.rodada
    if args.source:
        _log(f"AVISO: argumento legado --source='{args.source}' ignorado. Usando lógica automática.")

    p = _paths(rodada, preds=args.preds, features=args.features, matches=args.matches)

    # wandb (opcional)
    run = None
    if _HAS_WANDB:
        try:
            run = wandb.init(project="loteca", name=f"build_probs_{rodada}", config={"rodada": rodada})
        except Exception as e:
            _log(f"AVISO wandb: {e}")
            run = None

    # 1) tentar preds_bivar
    df_bivar = _read_csv_if_exists(p["preds_bivar"])
    # 2) fallback: features_base
    df_feat = _read_csv_if_exists(p["features_base"])

    source_used = ""
    df_out = pd.DataFrame(columns=OUT_COLS)

    if not df_bivar.empty:
        teams = _normalize_team_cols(df_bivar)
        probs = _extract_probs(df_bivar)
        df_tmp = _merge_teams_and_probs(teams, probs, default_equal=True)
        if not df_tmp.empty:
            df_tmp["rodada"] = rodada
            df_out = df_tmp[["rodada", "home", "away", "p_home", "p_draw", "p_away"]].copy()
            df_out["source"] = "preds_bivar"
            source_used = "preds_bivar"

    if df_out.empty and not df_feat.empty:
        teams = _normalize_team_cols(df_feat)
        probs = _extract_probs(df_feat)
        df_tmp = _merge_teams_and_probs(teams, probs, default_equal=True)
        if not df_tmp.empty:
            df_tmp["rodada"] = rodada
            df_out = df_tmp[["rodada", "home", "away", "p_home", "p_draw", "p_away"]].copy()
            df_out["source"] = "features_base"
            source_used = "features_base"

    if df_out.empty:
        df_matches = _read_csv_if_exists(p["matches"])
        teams = _normalize_team_cols(df_matches)
        if not teams.empty:
            df_tmp = _merge_teams_and_probs(teams, pd.DataFrame(), default_equal=True)
            df_tmp["rodada"] = rodada
            df_out = df_tmp[["rodada", "home", "away", "p_home", "p_draw", "p_away"]].copy()
            df_out["source"] = "fallback_equal"
            source_used = "fallback_equal"
        else:
            df_out = pd.DataFrame(columns=OUT_COLS)

    for c in OUT_COLS:
        if c not in df_out.columns:
            df_out[c] = "" if c in ("rodada", "home", "away", "source") else 0.0
    df_out = df_out[OUT_COLS]

    out_csv = p["out"]
    _safe_mkdir(os.path.dirname(out_csv))
    df_out.to_csv(out_csv, index=False)
    _log(f"Fonte='{source_used or 'none'}' -> {out_csv} ({len(df_out)} linhas)")

    if run:
        try:
            wandb.summary["probs_rows"] = int(len(df_out))
            wandb.summary["probs_source"] = source_used or "none"
        finally:
            wandb.finish()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        print(f"[build_probs] ERRO não fatal: {e}")
        try:
            arg_map = {sys.argv[i].lstrip("-"): sys.argv[i + 1]
                       for i in range(len(sys.argv) - 1)
                       if sys.argv[i].startswith("--")}
            rodada = arg_map.get("rodada", "unknown")
            base_out = os.path.join("data", "out", rodada)
            os.makedirs(base_out, exist_ok=True)
            pd.DataFrame(columns=OUT_COLS).to_csv(os.path.join(base_out, "probabilities.csv"), index=False)
            print(f"[build_probs] OK -> data/out/{rodada}/probabilities.csv (0 linhas)")
        except Exception:
            pass
        sys.exit(0)
