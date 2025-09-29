#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Avaliação de risco e valor (edge/Kelly) por jogo.

Comportamento resiliente:
- Tenta unir (merge) por match_id. Se ausente, tenta por (home, away).
- Se probabilities.csv não tiver match_id, tenta buscá-lo em data/out/<RODADA>/matches.csv.
- Se odds.csv não existir ou vier vazio, ainda assim gera risk_report.csv com colunas padrão.

Entradas (padrão):
  data/out/<RODADA>/probabilities.csv
  data/out/<RODADA>/odds.csv
  data/out/<RODADA>/matches.csv   (apenas para resgatar match_id se necessário)

Saída:
  data/out/<RODADA>/risk_report.csv

Colunas principais de saída:
  rodada, match_id, home, away,
  p_home, p_draw, p_away,
  k1, kx, k2,
  imp_home, imp_draw, imp_away,
  edge_home, edge_draw, edge_away,
  kelly_home, kelly_draw, kelly_away,
  best_bet, kelly_max, notes
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

# wandb opcional (compatível com 0.22.0)
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
    if df.empty:
        return pd.DataFrame(columns=["home", "away"])
    h = _pick(df, ["home", "mandante", "time_home", "team_home"])
    a = _pick(df, ["away", "visitante", "time_away", "team_away"])
    if h is None or a is None:
        # fallback: duas primeiras colunas
        if len(df.columns) >= 2:
            h, a = df.columns[:2]
        else:
            return pd.DataFrame(columns=["home", "away"])
    out = pd.DataFrame(
        {
            "home": df[h].astype(str).fillna("").str.strip(),
            "away": df[a].astype(str).fillna("").str.strip(),
        }
    )
    out = out[(out["home"] != "") & (out["away"] != "")]
    return out


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
    # Se só vier home/away, derive draw
    if "p_draw" not in out or out["p_draw"].isna().all():
        if "p_home" in out and "p_away" in out:
            p_draw = 1.0 - out["p_home"].fillna(0) - out["p_away"].fillna(0)
            out["p_draw"] = p_draw.clip(0, 1)
    # Normaliza linha a 1
    if not out.empty:
        out = out.fillna(0.0)
        s = out.sum(axis=1).replace(0, 1.0)
        out = out.div(s, axis=0)
    return out[["p_home", "p_draw", "p_away"]]


def _ensure_match_id(df_probs: pd.DataFrame, rodada: str, matches_path: str) -> pd.DataFrame:
    """
    Garante coluna match_id. Tenta:
      1) Já existir no df_probs
      2) Buscar em matches.csv via (home, away)
      3) Criar sintético "home__away"
    """
    df_probs = df_probs.copy()
    if "match_id" in df_probs.columns:
        return df_probs

    df_matches = _safe_read_csv(matches_path)
    teams_p = _norm_teams(df_probs)
    df_probs = pd.concat([df_probs.reset_index(drop=True), teams_p.reset_index(drop=True)], axis=1)

    if not df_matches.empty:
        # Normaliza matches
        teams_m = _norm_teams(df_matches)
        df_m = pd.concat([df_matches.reset_index(drop=True), teams_m.reset_index(drop=True)], axis=1)
        id_col = _pick(df_m, ["match_id", "id", "fixture_id"])
        if id_col is None:
            # tenta reconstruir por ordem
            df_m["match_id"] = (
                df_m["home"].astype(str) + "__" + df_m["away"].astype(str)
            )
        else:
            df_m["match_id"] = df_m[id_col]
        df_probs = df_probs.merge(
            df_m[["home", "away", "match_id"]],
            on=["home", "away"],
            how="left",
        )

    if "match_id" not in df_probs.columns or df_probs["match_id"].isna().any():
        df_probs["match_id"] = df_probs.get("match_id")
        df_probs["match_id"] = df_probs["match_id"].fillna(
            df_probs["home"].astype(str) + "__" + df_probs["away"].astype(str)
        )

    return df_probs


def _norm_odds(df_odds: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza colunas de odds decimais: k1 (home), kx (draw), k2 (away).
    Aceita alternativas como: home_odds, odds_home, o_home, etc.
    """
    if df_odds.empty:
        return pd.DataFrame(columns=["match_id", "home", "away", "k1", "kx", "k2"])

    # preserva possível match_id e teams
    id_col = _pick(df_odds, ["match_id", "id", "fixture_id"])
    teams = _norm_teams(df_odds)
    out = pd.concat([df_odds.reset_index(drop=True), teams.reset_index(drop=True)], axis=1)

    c_h = _pick(df_odds, ["k1", "home_odds", "odds_home", "o_home", "home_k"])
    c_x = _pick(df_odds, ["kx", "draw_odds", "odds_draw", "o_draw", "draw_k"])
    c_a = _pick(df_odds, ["k2", "away_odds", "odds_away", "o_away", "away_k"])

    if c_h is None and c_x is None and c_a is None:
        # tenta padrões de consensus do projeto
        c_h = _pick(df_odds, ["khome", "kh"])
        c_x = _pick(df_odds, ["kdraw", "kx"])
        c_a = _pick(df_odds, ["kaway", "k2"])

    out["k1"] = pd.to_numeric(df_odds[c_h], errors="coerce") if c_h else np.nan
    out["kx"] = pd.to_numeric(df_odds[c_x], errors="coerce") if c_x else np.nan
    out["k2"] = pd.to_numeric(df_odds[c_a], errors="coerce") if c_a else np.nan

    if id_col:
        out["match_id"] = df_odds[id_col]
    else:
        out["match_id"] = out["home"].astype(str) + "__" + out["away"].astype(str)

    return out[["match_id", "home", "away", "k1", "kx", "k2"]]


def _implied(k: pd.Series) -> pd.Series:
    # k -> prob implícita (sem remover o overround)
    with np.errstate(divide="ignore", invalid="ignore"):
        p = 1.0 / k.astype(float)
    return p.replace([np.inf, -np.inf], np.nan)


def _kelly(p: pd.Series, k: pd.Series) -> pd.Series:
    # Kelly fracionário unitário: f* = (k*p - 1)/(k - 1)  (<=0 -> 0)
    k = k.astype(float)
    p = p.astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        f = (k * p - 1.0) / (k - 1.0)
    f = f.where(f > 0, 0.0)
    f = f.replace([np.inf, -np.inf], 0.0).fillna(0.0)
    return f.clip(lower=0.0)


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate risk/edge/kelly for a rodada")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--probs", default=None, help="Caminho alternativo para probabilities.csv")
    ap.add_argument("--odds", default=None, help="Caminho alternativo para odds.csv")
    ap.add_argument("--matches", default=None, help="Caminho alternativo para matches.csv")
    ap.add_argument("--out", default=None, help="Caminho de saída (risk_report.csv)")
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
    notes = []

    if probs_raw.empty:
        _log(f"AVISO: probabilities.csv vazio/ausente em {probs_path}")
        # cria esqueleto vazio e salva
        pd.DataFrame(
            columns=[
                "rodada","match_id","home","away",
                "p_home","p_draw","p_away",
                "k1","kx","k2",
                "imp_home","imp_draw","imp_away",
                "edge_home","edge_draw","edge_away",
                "kelly_home","kelly_draw","kelly_away",
                "best_bet","kelly_max","notes"
            ]
        ).to_csv(out_path, index=False)
        _log(f"OK -> {out_path} (0 linhas)")
        if run:
            wandb.summary["risk_rows"] = 0
            wandb.finish()
        return

    # Normaliza probs
    teams_p = _norm_teams(probs_raw)
    probs_p = _norm_probs(probs_raw)
    probs = pd.concat([teams_p.reset_index(drop=True), probs_p.reset_index(drop=True)], axis=1)
    probs["rodada"] = rodada

    # Garante match_id
    probs = _ensure_match_id(probs, rodada, matches_path)

    # Normaliza odds (pode estar vazio)
    odds = _norm_odds(odds_raw)

    # Estratégia de merge:
    # 1) Tenta por match_id se odds tiver
    merged = None
    if not odds.empty and "match_id" in odds.columns:
        merged = probs.merge(odds[["match_id", "k1", "kx", "k2"]], on="match_id", how="left")
    else:
        # 2) Tenta por home/away
        if not odds.empty:
            merged = probs.merge(odds[["home", "away", "k1", "kx", "k2"]], on=["home", "away"], how="left")
        else:
            merged = probs.copy()
            notes.append("odds ausentes")

    df = merged.copy()

    # Garante colunas de odds
    for c in ["k1", "kx", "k2"]:
        if c not in df.columns:
            df[c] = np.nan

    # Probabilidades implícitas
    df["imp_home"] = _implied(df["k1"])
    df["imp_draw"] = _implied(df["kx"])
    df["imp_away"] = _implied(df["k2"])
    # Normaliza implícitas por linha quando existirem
    imp_sum = df[["imp_home", "imp_draw", "imp_away"]].sum(axis=1)
    imp_sum = imp_sum.replace(0, np.nan)
    for col in ["imp_home", "imp_draw", "imp_away"]:
        df[col] = (df[col] / imp_sum).fillna(0.0)

    # Edge = p_model - p_implied
    df["edge_home"] = (df["p_home"] - df["imp_home"]).fillna(0.0)
    df["edge_draw"] = (df["p_draw"] - df["imp_draw"]).fillna(0.0)
    df["edge_away"] = (df["p_away"] - df["imp_away"]).fillna(0.0)

    # Kelly por resultado (0 se não houver odds)
    df["kelly_home"] = _kelly(df["p_home"], df["k1"])
    df["kelly_draw"] = _kelly(df["p_draw"], df["kx"])
    df["kelly_away"] = _kelly(df["p_away"], df["k2"])

    # Melhor aposta da linha
    kelly_cols = ["kelly_home", "kelly_draw", "kelly_away"]
    df["kelly_max"] = df[kelly_cols].max(axis=1)
    best_idx = df[kelly_cols].idxmax(axis=1)
    mapping = {"kelly_home": "home", "kelly_draw": "draw", "kelly_away": "away"}
    df["best_bet"] = best_idx.map(mapping).fillna("none")

    # notes
    if "odds ausentes" in notes or df[["k1", "kx", "k2"]].isna().all(axis=None):
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
    # garante todas
    for c in out_cols:
        if c not in df.columns:
            df[c] = "" if c in ("rodada", "match_id", "home", "away", "best_bet", "notes") else 0.0

    df_out = df[out_cols].copy()
    df_out.to_csv(out_path, index=False)
    _log(f"OK -> {out_path} ({len(df_out)} linhas)")

    if _HAS_WANDB and run:
        try:
            wandb.summary["risk_rows"] = int(len(df_out))
            missing_odds = int(df_out[["k1", "kx", "k2"]].isna().any(axis=1).sum())
            wandb.summary["risk_missing_odds"] = missing_odds
        finally:
            wandb.finish()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        print(f"[risk] ERRO não fatal: {e}")
        # Em último caso, ainda produz um CSV vazio para não quebrar o workflow.
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
