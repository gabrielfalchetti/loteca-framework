#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Avaliação de risco e valor (edge/Kelly) por jogo.

Resiliência:
- Une por match_id; se não houver, une por (home, away).
- Se probabilities.csv não tiver match_id, tenta achar em matches.csv; caso contrário, cria sintético "home__away".
- Gera relatório mesmo sem odds.

Entradas padrão:
  data/out/<RODADA>/probabilities.csv
  data/out/<RODADA>/odds.csv
  data/out/<RODADA>/matches.csv   (opcional; usado para resgatar match_id)

Saída:
  data/out/<RODADA>/risk_report.csv
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List, Optional

import numpy as np
import pandas as pd

# wandb opcional
try:
    import wandb  # type: ignore
    _HAS_WANDB = True
except Exception:
    _HAS_WANDB = False


def _log(msg: str) -> None:
    print(f"[risk] {msg}")


def _safe_read_csv(path: str, empty_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if path and os.path.isfile(path):
        try:
            return pd.read_csv(path)
        except Exception as e:
            _log(f"AVISO: falha lendo '{path}': {e}")
    return pd.DataFrame(columns=empty_cols or [])


def _pick(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower().strip(): c for c in df.columns}
    for k in candidates:
        if k in cols:
            return cols[k]
    return None


def _norm_teams(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna dataframe com colunas 'home','away' normalizadas (sem duplicar)."""
    if df.empty:
        return pd.DataFrame(columns=["home", "away"])
    # Se já existem, só normaliza os valores
    existing_h = _pick(df, ["home", "mandante", "time_home", "team_home"])
    existing_a = _pick(df, ["away", "visitante", "time_away", "team_away"])
    if existing_h is not None and existing_a is not None:
        out = pd.DataFrame(
            {
                "home": df[existing_h].astype(str).fillna("").str.strip(),
                "away": df[existing_a].astype(str).fillna("").str.strip(),
            }
        )
        return out[(out["home"] != "") & (out["away"] != "")]
    # Caso contrário, tenta primeiras duas colunas
    if len(df.columns) >= 2:
        h, a = df.columns[:2]
        out = pd.DataFrame(
            {
                "home": df[h].astype(str).fillna("").str.strip(),
                "away": df[a].astype(str).fillna("").str.strip(),
            }
        )
        return out[(out["home"] != "") & (out["away"] != "")]
    return pd.DataFrame(columns=["home", "away"])


def _norm_probs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["p_home", "p_draw", "p_away"])
    c_h = _pick(df, ["p_home", "prob_home", "home_prob", "home_win_prob"])
    c_x = _pick(df, ["p_draw", "prob_draw", "draw_prob", "empate_prob", "x", "p_empate"])
    c_a = _pick(df, ["p_away", "prob_away", "away_prob", "away_win_prob"])
    out = pd.DataFrame(columns=["p_home", "p_draw", "p_away"])
    if c_h is not None:
        out["p_home"] = pd.to_numeric(df[c_h], errors="coerce")
    if c_x is not None:
        out["p_draw"] = pd.to_numeric(df[c_x], errors="coerce")
    if c_a is not None:
        out["p_away"] = pd.to_numeric(df[c_a], errors="coerce")
    # Se faltar p_draw, derive a partir de home/away
    if ("p_draw" not in out) or out["p_draw"].isna().all():
        if "p_home" in out and "p_away" in out:
            p_draw = 1.0 - out["p_home"].fillna(0) - out["p_away"].fillna(0)
            out["p_draw"] = p_draw.clip(0, 1)
    # Normaliza linha para somar 1
    if not out.empty:
        out = out.fillna(0.0)
        s = out.sum(axis=1).replace(0, 1.0)
        out = out.div(s, axis=0)
    return out[["p_home", "p_draw", "p_away"]]


def _ensure_match_id(df_probs: pd.DataFrame, matches_path: str) -> pd.DataFrame:
    """
    Garante coluna match_id no df_probs SEM duplicar home/away:
      1) Se já existir, preserva.
      2) Tenta buscar em matches.csv via (home, away).
      3) Cria sintético "home__away".
    """
    df_probs = df_probs.copy()

    # Garante que há colunas home/away (sem duplicar)
    need_home = "home" not in df_probs.columns
    need_away = "away" not in df_probs.columns
    if need_home or need_away:
        teams_from_self = _norm_teams(df_probs)
        if need_home and "home" in teams_from_self:
            df_probs["home"] = teams_from_self["home"]
        if need_away and "away" in teams_from_self:
            df_probs["away"] = teams_from_self["away"]

    if "match_id" in df_probs.columns:
        return df_probs

    # Tenta matches.csv
    df_matches = _safe_read_csv(matches_path)
    if not df_matches.empty:
        teams_m = _norm_teams(df_matches)
        df_m = pd.DataFrame({
            "home": teams_m.get("home", pd.Series([], dtype=str)),
            "away": teams_m.get("away", pd.Series([], dtype=str)),
        })
        id_col = _pick(df_matches, ["match_id", "id", "fixture_id"])
        if id_col is not None:
            df_m["match_id"] = df_matches[id_col]
        else:
            df_m["match_id"] = df_m["home"].astype(str) + "__" + df_m["away"].astype(str)
        tmp = df_probs.merge(df_m[["home", "away", "match_id"]], on=["home", "away"], how="left")
        df_probs["match_id"] = tmp["match_id"]

    # Completa match_id sintético se ainda faltar
    df_probs["match_id"] = df_probs["match_id"].fillna(df_probs["home"].astype(str) + "__" + df_probs["away"].astype(str))

    return df_probs


def _norm_odds(df_odds: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza para: ['match_id','home','away','k1','kx','k2'] sem criar colunas duplicadas.
    """
    if df_odds.empty:
        return pd.DataFrame(columns=["match_id", "home", "away", "k1", "kx", "k2"])

    # Identifica colunas já existentes de times (sem concatenar nada)
    existing_h = _pick(df_odds, ["home", "mandante", "time_home", "team_home"])
    existing_a = _pick(df_odds, ["away", "visitante", "time_away", "team_away"])

    if existing_h is not None and existing_a is not None:
        home_series = df_odds[existing_h].astype(str).fillna("").str.strip()
        away_series = df_odds[existing_a].astype(str).fillna("").str.strip()
    else:
        teams = _norm_teams(df_odds)
        home_series = teams.get("home", pd.Series([], dtype=str))
        away_series = teams.get("away", pd.Series([], dtype=str))

    out = pd.DataFrame({"home": home_series, "away": away_series})

    # Odds
    c_h = _pick(df_odds, ["k1", "home_odds", "odds_home", "o_home", "home_k", "khome", "kh"])
    c_x = _pick(df_odds, ["kx", "draw_odds", "odds_draw", "o_draw", "draw_k", "kdraw"])
    c_a = _pick(df_odds, ["k2", "away_odds", "odds_away", "o_away", "away_k", "kaway"])

    out["k1"] = pd.to_numeric(df_odds[c_h], errors="coerce") if c_h else np.nan
    out["kx"] = pd.to_numeric(df_odds[c_x], errors="coerce") if c_x else np.nan
    out["k2"] = pd.to_numeric(df_odds[c_a], errors="coerce") if c_a else np.nan

    # match_id
    id_col = _pick(df_odds, ["match_id", "id", "fixture_id"])
    if id_col:
        out["match_id"] = df_odds[id_col]
    else:
        out["match_id"] = out["home"].astype(str) + "__" + out["away"].astype(str)

    # Apenas as colunas padronizadas — sem duplicatas
    return out[["match_id", "home", "away", "k1", "kx", "k2"]]


def _implied(k: pd.Series) -> pd.Series:
    with np.errstate(divide="ignore", invalid="ignore"):
        p = 1.0 / k.astype(float)
    return p.replace([np.inf, -np.inf], np.nan)


def _kelly(p: pd.Series, k: pd.Series) -> pd.Series:
    k = k.astype(float)
    p = p.astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        f = (k * p - 1.0) / (k - 1.0)
    f = f.where(f > 0, 0.0)
    f = f.replace([np.inf, -np.inf], 0.0).fillna(0.0)
    return f.clip(lower=0.0)


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate risk/edge/kelly para a rodada")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--probs", default=None)
    ap.add_argument("--odds", default=None)
    ap.add_argument("--matches", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    rodada = args.rodada
    base_out = os.path.join("data", "out", rodada)
    probs_path = args.probs or os.path.join(base_out, "probabilities.csv")
    odds_path = args.odds or os.path.join(base_out, "odds.csv")
    matches_path = args.matches or os.path.join(base_out, "matches.csv")
    out_path = args.out or os.path.join(base_out, "risk_report.csv")
    os.makedirs(base_out, exist_ok=True)

    run = None
    if _HAS_WANDB:
        try:
            run = wandb.init(project="loteca", name=f"risk_{rodada}", config={"rodada": rodada})
        except Exception as e:
            _log(f"AVISO wandb: {e}")
            run = None

    # Lê entradas
    probs_raw = _safe_read_csv(probs_path)
    odds_raw = _safe_read_csv(odds_path)

    if probs_raw.empty:
        _log(f"AVISO: probabilities.csv vazio/ausente em {probs_path}")
        pd.DataFrame(columns=[
            "rodada","match_id","home","away",
            "p_home","p_draw","p_away",
            "k1","kx","k2",
            "imp_home","imp_draw","imp_away",
            "edge_home","edge_draw","edge_away",
            "kelly_home","kelly_draw","kelly_away",
            "best_bet","kelly_max","notes"
        ]).to_csv(out_path, index=False)
        _log(f"OK -> {out_path} (0 linhas)")
        if run:
            wandb.summary["risk_rows"] = 0
            wandb.finish()
        return

    # Normaliza: NÃO duplicar home/away
    teams_p = _norm_teams(probs_raw)
    probs_p = _norm_probs(probs_raw)
    probs = pd.DataFrame()
    probs["home"] = teams_p["home"]
    probs["away"] = teams_p["away"]
    probs = pd.concat([probs.reset_index(drop=True), probs_p.reset_index(drop=True)], axis=1)
    probs["rodada"] = rodada

    # Garante match_id sem duplicar colunas
    probs = _ensure_match_id(probs, matches_path)

    # Odds normalizadas (sem duplicatas)
    odds = _norm_odds(odds_raw)

    # Merge
    if not odds.empty and "match_id" in odds.columns:
        df = probs.merge(odds[["match_id", "k1", "kx", "k2"]], on="match_id", how="left")
    elif not odds.empty:
        df = probs.merge(odds[["home", "away", "k1", "kx", "k2"]], on=["home", "away"], how="left")
    else:
        df = probs.copy()

    # Garante colunas de odds
    for c in ["k1", "kx", "k2"]:
        if c not in df.columns:
            df[c] = np.nan

    # Probabilidades implícitas
    df["imp_home"] = _implied(df["k1"])
    df["imp_draw"] = _implied(df["kx"])
    df["imp_away"] = _implied(df["k2"])
    imp_sum = df[["imp_home", "imp_draw", "imp_away"]].sum(axis=1).replace(0, np.nan)
    for col in ["imp_home", "imp_draw", "imp_away"]:
        df[col] = (df[col] / imp_sum).fillna(0.0)

    # Edge
    df["edge_home"] = (df["p_home"] - df["imp_home"]).fillna(0.0)
    df["edge_draw"] = (df["p_draw"] - df["imp_draw"]).fillna(0.0)
    df["edge_away"] = (df["p_away"] - df["imp_away"]).fillna(0.0)

    # Kelly
    df["kelly_home"] = _kelly(df["p_home"], df["k1"])
    df["kelly_draw"] = _kelly(df["p_draw"], df["kx"])
    df["kelly_away"] = _kelly(df["p_away"], df["k2"])

    # Melhor aposta
    kelly_cols = ["kelly_home", "kelly_draw", "kelly_away"]
    df["kelly_max"] = df[kelly_cols].max(axis=1)
    best_idx = df[kelly_cols].idxmax(axis=1)
    df["best_bet"] = best_idx.map({"kelly_home": "home", "kelly_draw": "draw", "kelly_away": "away"}).fillna("none")

    # Notes
    if df[["k1", "kx", "k2"]].isna().all(axis=None):
        df["notes"] = "odds indisponíveis"
    else:
        df["notes"] = ""

    # Ordena colunas e salva
    out_cols = [
        "rodada", "match_id", "home", "away",
        "p_home", "p_draw", "p_away",
        "k1", "kx", "k2",
        "imp_home", "imp_draw", "imp_away",
        "edge_home", "edge_draw", "edge_away",
        "kelly_home", "kelly_draw", "kelly_away",
        "best_bet", "kelly_max", "notes"
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = "" if c in ("rodada", "match_id", "home", "away", "best_bet", "notes") else 0.0

    df_out = df[out_cols].copy()
    df_out.to_csv(out_path, index=False)
    _log(f"OK -> {out_path} ({len(df_out)} linhas)")

    if _HAS_WANDB and run:
        try:
            wandb.summary["risk_rows"] = int(len(df_out))
            wandb.summary["risk_missing_odds"] = int(df_out[["k1", "kx", "k2"]].isna().any(axis=1).sum())
        finally:
            wandb.finish()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        print(f"[risk] ERRO não fatal: {e}")
        # fallback: CSV vazio para não quebrar o workflow
        try:
            rodada = "unknown"
            for i in range(len(sys.argv) - 1):
                if sys.argv[i] == "--rodada":
                    rodada = sys.argv[i + 1]
                    break
            base_out = os.path.join("data", "out", rodada)
            os.makedirs(base_out, exist_ok=True)
            pd.DataFrame(columns=[
                "rodada","match_id","home","away",
                "p_home","p_draw","p_away",
                "k1","kx","k2",
                "imp_home","imp_draw","imp_away",
                "edge_home","edge_draw","edge_away",
                "kelly_home","kelly_draw","kelly_away",
                "best_bet","kelly_max","notes"
            ]).to_csv(os.path.join(base_out, "risk_report.csv"), index=False)
            print(f"[risk] OK -> data/out/{rodada}/risk_report.csv (0 linhas)")
        except Exception:
            pass
        sys.exit(0)
