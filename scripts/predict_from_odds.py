#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/predict_from_odds.py — STRICT MODE

Objetivo:
  Converter odds (H2H) reais em probabilidades implícitas e "justas" (removendo overround),
  gerando picks 1X2 e uma margem de confiança simples.

Política STRICT:
  - 🚫 NÃO roda com entradas vazias/incompletas.
  - 🚫 NÃO cria dados fictícios.
  - ✅ Exige odds de consenso válidas para TODOS os jogos da whitelist.
  - ✅ Falha hard (exit 98) se qualquer pré-condição não for atendida.

Entradas obrigatórias (no mesmo OUT_DIR):
  - odds_consensus.csv  [colunas: match_id,team_home,team_away,odds_home,odds_draw,odds_away]
    (de preferência produzido pelo scripts/consensus_odds_safe.py STRICT)

Saída:
  - predictions_market.csv  [colunas: match_key,home,away,odd_home,odd_draw,odd_away,
                             p_home,p_draw,p_away,pick_1x2,conf_margin]

Uso:
  python scripts/predict_from_odds.py --rodada data/out/<RODADA_ID> [--debug]
"""

import argparse
import os
import sys
import pandas as pd
import numpy as np
import unicodedata
import re

EXIT_OK = 0
EXIT_CRITICAL = 98

# ========== Logging ==========
def log(msg): print(msg, flush=True)
def err(msg): print(f"::error::{msg}", flush=True)
def warn(msg): print(f"Warning: {msg}", flush=True)

# ========== Utils ==========
_norm_space = re.compile(r"\s+")
_norm_punct = re.compile(r"[^\w\s]+")

def normalize(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = _norm_punct.sub(" ", s.lower())
    s = _norm_space.sub(" ", s).strip()
    return s

def make_match_key(home: str, away: str) -> str:
    return f"{normalize(home)}__vs__{normalize(away)}"

def implied_from_decimal(odds):
    """Transforma odd decimal em probabilidade implícita: 1/odd (com proteção)."""
    with np.errstate(divide="ignore", invalid="ignore"):
        p = 1.0 / np.clip(odds, 1e-12, None)
    p = np.where(np.isfinite(p), p, np.nan)
    return p

def remove_overround(p_home, p_draw, p_away):
    """Normaliza para remover overround mantendo proporções."""
    s = p_home + p_draw + p_away
    # se alguma prob é nan, retorna nans (será tratado adiante)
    if np.any(~np.isfinite([p_home, p_draw, p_away])) or not np.isfinite(s) or s <= 0:
        return np.nan, np.nan, np.nan
    return p_home / s, p_draw / s, p_away / s

def pick_1x2_row(ph, pd, pa):
    arr = np.array([ph, pd, pa], dtype=float)
    if np.any(~np.isfinite(arr)):
        return "?", np.nan
    i = int(np.argmax(arr))
    label = ["1", "X", "2"][i]
    # margem de confiança: diferença entre maior e segundo maior
    top = np.sort(arr)[-1]
    second = np.sort(arr)[-2]
    return label, float(top - second)

def must_have_columns(df, cols, path_label):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        err(f"[predict] Colunas ausentes em {path_label}: {miss}")
        sys.exit(EXIT_CRITICAL)

# ========== Main ==========
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada if os.path.isdir(args.rodada) else os.path.join("data", "out", str(args.rodada))
    os.makedirs(out_dir, exist_ok=True)
    log(f"[predict] OUT_DIR = {out_dir}")

    # ---- Carrega odds_consensus STRICT ----
    oc_path = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(oc_path):
        err(f"[predict] odds_consensus.csv ausente em {oc_path}.")
        sys.exit(EXIT_CRITICAL)

    df = pd.read_csv(oc_path)
    if df.empty:
        err(f"[predict] odds_consensus.csv está vazio em {oc_path}.")
        sys.exit(EXIT_CRITICAL)

    must_have_columns(
        df,
        ["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"],
        "odds_consensus.csv"
    )

    # Checa se há qualquer odd inválida (<=1) ou NaN
    for c in ["odds_home", "odds_draw", "odds_away"]:
        if (df[c].isna().any()) or (df[c] <= 1.0).any():
            bad = df[df[c].isna() | (df[c] <= 1.0)][["match_id", "team_home", "team_away", c]]
            err("[predict] Odds inválidas detectadas (NaN ou <= 1.0). Abortando.")
            if args.debug:
                print(bad.to_string(index=False))
            sys.exit(EXIT_CRITICAL)

    # ---- Calcula probabilidades implícitas e justas ----
    df["imp_home"] = implied_from_decimal(df["odds_home"].to_numpy())
    df["imp_draw"] = implied_from_decimal(df["odds_draw"].to_numpy())
    df["imp_away"] = implied_from_decimal(df["odds_away"].to_numpy())

    # Remove overround
    fair = df[["imp_home", "imp_draw", "imp_away"]].to_numpy(dtype=float)
    fair_norm = []
    for ph, pd_, pa in fair:
        fh, fd, fa = remove_overround(ph, pd_, pa)
        fair_norm.append((fh, fd, fa))
    fair_norm = np.array(fair_norm)
    df["p_home"] = fair_norm[:, 0]
    df["p_draw"] = fair_norm[:, 1]
    df["p_away"] = fair_norm[:, 2]

    # Validação final de probabilidades
    if df[["p_home", "p_draw", "p_away"]].isna().any().any():
        err("[predict] Probabilidades finais contêm NaN. Abortando.")
        if args.debug:
            print(df[df[["p_home","p_draw","p_away"]].isna().any(axis=1)][
                ["match_id","team_home","team_away","odds_home","odds_draw","odds_away","imp_home","imp_draw","imp_away","p_home","p_draw","p_away"]
            ].to_string(index=False))
        sys.exit(EXIT_CRITICAL)

    # ---- Picks e margem ----
    picks = df.apply(lambda r: pick_1x2_row(r["p_home"], r["p_draw"], r["p_away"]), axis=1)
    df["pick_1x2"] = [p[0] for p in picks]
    df["conf_margin"] = [p[1] for p in picks]

    # ---- Chave de jogo + saída padronizada ----
    df["match_key"] = df.apply(lambda r: make_match_key(r["team_home"], r["team_away"]), axis=1)

    out_cols = [
        "match_key",
        "team_home", "team_away",
        "odds_home", "odds_draw", "odds_away",
        "p_home", "p_draw", "p_away",
        "pick_1x2", "conf_margin"
    ]
    df_out = df.rename(columns={"team_home": "home", "team_away": "away"})[[
        "match_key", "home", "away",
        "odds_home", "odds_draw", "odds_away",
        "p_home", "p_draw", "p_away",
        "pick_1x2", "conf_margin"
    ]].copy()

    out_path = os.path.join(out_dir, "predictions_market.csv")
    df_out.to_csv(out_path, index=False)

    # Sanidade: não permitir arquivo vazio
    if not os.path.isfile(out_path) or os.path.getsize(out_path) == 0:
        err("[predict] predictions_market.csv não gerado (vazio).")
        sys.exit(EXIT_CRITICAL)

    # Sanidade: toda linha deve ter pick válido (1, X ou 2)
    if (df_out["pick_1x2"].isin(["1","X","2"]) == False).any():
        err("[predict] Encontrado pick inválido ('?'). Abortando.")
        if args.debug:
            print(df_out[df_out["pick_1x2"].isin(["1","X","2"]) == False].to_string(index=False))
        sys.exit(EXIT_CRITICAL)

    # (Opcional) Log de amostra
    sample = df_out.head(10)
    log(sample.to_csv(index=False))
    sys.exit(EXIT_OK)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        err(f"[predict] Falha inesperada: {e}")
        sys.exit(EXIT_CRITICAL)