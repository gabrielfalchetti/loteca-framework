#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera previsões de mercado a partir de odds de consenso.

Entrada esperada:
  data/out/<RID>/odds_consensus.csv
  (colunas toleradas serão normalizadas para: home, away, odd_home, odd_draw, odd_away
   e, se existirem, imp_home, imp_draw, imp_away)

Saída:
  data/out/<RID>/predictions_market.csv

Uso:
  python scripts/predict_from_odds.py --rodada data/out/<RID> --debug
  # No seu workflow, --rodada recebe o caminho do OUT_DIR (não o ID).
"""

import argparse
import os
import sys
import json
import unicodedata
from typing import List
import pandas as pd
import numpy as np

DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

def log(msg: str):
    print(f"[predict] {msg}")

def die(code: int, msg: str):
    log(msg)
    sys.exit(code)

# ---------- normalização de colunas ----------

HOME_ALIASES = ["home", "team_home", "home_team", "mandante"]
AWAY_ALIASES = ["away", "team_away", "away_team", "visitante"]

OH_ALIASES   = ["odd_home", "odds_home", "home_odds", "o1", "price_home", "h2h_home"]
OD_ALIASES   = ["odd_draw", "odds_draw", "draw_odds", "ox", "price_draw", "h2h_draw"]
OA_ALIASES   = ["odd_away", "odds_away", "away_odds", "o2", "price_away", "h2h_away"]

PH_ALIASES   = ["imp_home", "p_home", "prob_home"]
PD_ALIASES   = ["imp_draw", "p_draw", "prob_draw"]
PA_ALIASES   = ["imp_away", "p_away", "prob_away"]

def first_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        lc = cand.lower()
        if lc in cols:
            return cols[lc]
    # tentativa por substring
    for c in df.columns:
        lc = c.lower()
        for cand in candidates:
            if cand.lower() in lc:
                return c
    return ""

def _strip_accents_lower(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower()

def make_key(home: str, away: str) -> str:
    return f"{_strip_accents_lower(home)}__vs__{_strip_accents_lower(away)}"

def load_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        die(7, f"arquivo de entrada não encontrado: {path}")
    try:
        df = pd.read_csv(path)
        if df.shape[0] == 0 or df.shape[1] == 0:
            die(7, f"arquivo vazio ou sem colunas: {path}")
        return df
    except Exception as e:
        die(7, f"falha ao ler {path}: {e}")
        return pd.DataFrame()  # unreachable

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    ch = first_col(df, HOME_ALIASES)
    ca = first_col(df, AWAY_ALIASES)
    if not ch or not ca:
        raise ValueError("não foi possível identificar colunas de times (home/away).")

    oh = first_col(df, OH_ALIASES)
    od = first_col(df, OD_ALIASES)
    oa = first_col(df, OA_ALIASES)

    # odds podem vir ausentes; trataremos com NaN
    out = pd.DataFrame()
    out["home"] = df[ch].astype(str)
    out["away"] = df[ca].astype(str)

    def to_float(x):
        try:
            v = float(x)
            return v if v > 1.0001 else np.nan
        except:
            return np.nan

    out["odd_home"] = df[oh].map(to_float) if oh else np.nan
    out["odd_draw"] = df[od].map(to_float) if od else np.nan
    out["odd_away"] = df[oa].map(to_float) if oa else np.nan

    # probabilidades implícitas (se já vierem do consenso)
    ph = first_col(df, PH_ALIASES)
    pd_ = first_col(df, PD_ALIASES)
    pa = first_col(df, PA_ALIASES)
    out["imp_home"] = df[ph].astype(float) if ph else np.nan
    out["imp_draw"] = df[pd_].astype(float) if pd_ else np.nan
    out["imp_away"] = df[pa].astype(float) if pa else np.nan

    out["match_key"] = out.apply(lambda r: make_key(r["home"], r["away"]), axis=1)

    # remove linhas totalmente sem odds E sem probs
    keep = out[["odd_home","odd_draw","odd_away","imp_home","imp_draw","imp_away"]].notna().any(axis=1)
    out = out[keep].reset_index(drop=True)

    if out.empty:
        raise ValueError("nenhuma linha válida após normalização.")
    return out

# ---------- probabilidades e picks ----------

def probs_from_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Se imp_* não existir, calcula de 1/odd e normaliza overround."""
    need = ["imp_home","imp_draw","imp_away"]
    if df[need].notna().any(axis=None):
        # já há pelo menos uma imp_* — preenche faltantes com odds
        mask_missing = df[need].isna()
        any_missing = mask_missing.any(axis=None)
        if any_missing:
            inv_home = 1.0/df["odd_home"]
            inv_draw = 1.0/df["odd_draw"]
            inv_away = 1.0/df["odd_away"]
            inv_sum = inv_home.add(inv_draw, fill_value=0).add(inv_away, fill_value=0)
            df.loc[mask_missing["imp_home"], "imp_home"] = (inv_home/inv_sum)[mask_missing["imp_home"]]
            df.loc[mask_missing["imp_draw"], "imp_draw"] = (inv_draw/inv_sum)[mask_missing["imp_draw"]]
            df.loc[mask_missing["imp_away"], "imp_away"] = (inv_away/inv_sum)[mask_missing["imp_away"]]
        return df

    # nenhum imp_* presente -> deriva tudo das odds
    inv_home = 1.0/df["odd_home"]
    inv_draw = 1.0/df["odd_draw"]
    inv_away = 1.0/df["odd_away"]
    inv_sum = inv_home.add(inv_draw, fill_value=0).add(inv_away, fill_value=0)

    df["imp_home"] = inv_home / inv_sum
    df["imp_draw"] = inv_draw / inv_sum
    df["imp_away"] = inv_away / inv_sum
    return df

def choose_pick(row) -> str:
    vals = {"1": row["imp_home"], "X": row["imp_draw"], "2": row["imp_away"]}
    # desempate determinístico 1 > X > 2
    return max(vals, key=lambda k: (vals[k], {"1":3,"X":2,"2":1}[k]))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída da rodada (ex: data/out/<RID>)")
    ap.add_argument("--debug", action="store_true", default=False)
    args = ap.parse_args()

    out_dir = args.rodada
    if not os.path.isdir(out_dir):
        die(7, f"OUT_DIR inexistente: {out_dir}")

    in_path = os.path.join(out_dir, "odds_consensus.csv")
    log(f"OUT_DIR = {out_dir}")
    log(f"usando odds_consensus.csv ({in_path})")

    df_raw = load_csv_safe(in_path)
    try:
        df = normalize(df_raw)
    except Exception as e:
        die(7, f"falha na normalização: {e}")

    df = probs_from_odds(df)

    # pick 1X2 e margens
    df["pick_1x2"] = df.apply(choose_pick, axis=1)
    df["p_home"] = df["imp_home"].clip(0,1)
    df["p_draw"] = df["imp_draw"].clip(0,1)
    df["p_away"] = df["imp_away"].clip(0,1)

    # margens de confiança (diferença p_top - p_segundo)
    def top_margin(r):
        arr = np.array([r["p_home"], r["p_draw"], r["p_away"]])
        sort = np.sort(arr)
        return float(sort[-1] - sort[-2]) if len(sort) >= 2 else float("nan")

    df["conf_margin"] = df.apply(top_margin, axis=1)

    # ordena por confiança desc
    df = df.sort_values(["conf_margin","p_home","p_draw","p_away"], ascending=[False,False,False,False])

    # colunas finais
    cols = [
        "match_key", "home", "away",
        "odd_home", "odd_draw", "odd_away",
        "p_home", "p_draw", "p_away",
        "pick_1x2", "conf_margin"
    ]
    # garante existência
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan

    out_path = os.path.join(out_dir, "predictions_market.csv")
    df[cols].to_csv(out_path, index=False)

    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        die(7, "predictions_market.csv não gerado")

    # meta
    meta = {
        "total_matches": int(df.shape[0]),
        "generated_from": os.path.relpath(in_path),
        "columns": cols
    }
    with open(os.path.join(out_dir, "predictions_market_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    log(f"OK -> {out_path} ({df.shape[0]} jogos)")
    if args.debug or DEBUG:
        print(df[cols].head(10).to_string(index=False))

if __name__ == "__main__":
    main()