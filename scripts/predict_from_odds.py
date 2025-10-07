#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
predict_from_odds.py
--------------------
Gera probabilidades e um palpite 1x2 a partir das odds de consenso.

Entrada:
- data/out/<rodada>/odds_consensus.csv
  colunas OBRIGATÓRIAS:
    team_home, team_away, odds_home, odds_draw, odds_away
  colunas OPCIONAIS (criadas se faltarem):
    match_key, match_id

Saída:
- data/out/<rodada>/predictions_market.csv

Colunas de saída:
  match_key,home,away,odd_home,odd_draw,odd_away,
  p_home,p_draw,p_away,pick_1x2,conf_margin
"""

import os
import sys
import csv
import argparse
import math
import pandas as pd
from typing import Tuple

REQ_COLS = ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]


def log(msg: str, debug: bool = False):
    if debug:
        print(f"[predict] {msg}", flush=True)


def resolve_out_dir(rodada_arg: str) -> str:
    """Aceita tanto um ID (ex.: '17598...') quanto o caminho 'data/out/<id>'."""
    if os.path.isdir(rodada_arg):
        return rodada_arg
    candidate = os.path.join("data", "out", str(rodada_arg))
    if os.path.isdir(candidate):
        return candidate
    # cria se ainda não existir
    os.makedirs(candidate, exist_ok=True)
    return candidate


def norm_team(s):
    if s is None:
        return ""
    return str(s).strip()


def build_match_key(home: str, away: str) -> str:
    h = (home or "").strip().lower().replace(" ", "-")
    a = (away or "").strip().lower().replace(" ", "-")
    return f"{h}__vs__{a}"


def ensure_columns(df: pd.DataFrame, out_dir: str, debug: bool = False) -> pd.DataFrame:
    """
    - Checa colunas obrigatórias.
    - Gera match_key/match_id se não existirem.
    - Converte odds para numérico.
    - Renomeia headers alternativos (odd_* -> odds_*) se necessário.
    """
    ren = {}
    if "home" in df.columns and "team_home" not in df.columns:
        ren["home"] = "team_home"
    if "away" in df.columns and "team_away" not in df.columns:
        ren["away"] = "team_away"
    if "odd_home" in df.columns and "odds_home" not in df.columns:
        ren["odd_home"] = "odds_home"
    if "odd_draw" in df.columns and "odds_draw" not in df.columns:
        ren["odd_draw"] = "odds_draw"
    if "odd_away" in df.columns and "odds_away" not in df.columns:
        ren["odd_away"] = "odds_away"

    if ren:
        log(f"renomeando colunas: {ren}", debug)
        df = df.rename(columns=ren)

    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"[predict] faltam colunas obrigatórias: {missing}")

    # normaliza nomes das equipes
    df["team_home"] = df["team_home"].map(norm_team)
    df["team_away"] = df["team_away"].map(norm_team)

    # numéricos
    for c in ["odds_home", "odds_draw", "odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # remove linhas sem odds válidas
    df = df.dropna(subset=["odds_home", "odds_draw", "odds_away"]).copy()

    # match_key
    if "match_key" not in df.columns:
        df["match_key"] = df.apply(
            lambda r: build_match_key(r["team_home"], r["team_away"]), axis=1
        )
    else:
        df["match_key"] = df["match_key"].fillna("").map(str).str.strip()
        mask_empty = df["match_key"].eq("")
        if mask_empty.any():
            df.loc[mask_empty, "match_key"] = df.loc[mask_empty].apply(
                lambda r: build_match_key(r["team_home"], r["team_away"]), axis=1
            )

    # match_id (opcional nos arquivos seguintes, mas útil)
    if "match_id" not in df.columns:
        df["match_id"] = df["team_home"].astype(str).str.strip() + "__" + df["team_away"].astype(str).str.strip()

    return df


def implied_probs(oh: float, od: float, oa: float) -> Tuple[float, float, float]:
    """Converte odds decimais em probabilidades implícitas e normaliza para somar 1."""
    inv = []
    for o in (oh, od, oa):
        try:
            inv.append(1.0 / float(o) if o and float(o) > 0 else float("nan"))
        except Exception:
            inv.append(float("nan"))
    if any(math.isnan(x) for x in inv):
        return (float("nan"), float("nan"), float("nan"))

    s = sum(inv)
    if s <= 0:
        return (float("nan"), float("nan"), float("nan"))
    return tuple(x / s for x in inv)  # p_home, p_draw, p_away


def pick_from_probs(p_home: float, p_draw: float, p_away: float) -> Tuple[str, float]:
    """Retorna pick em {1, X, 2} e a margem de confiança (top1 - top2)."""
    probs = [("1", p_home), ("X", p_draw), ("2", p_away)]
    probs_sorted = sorted(probs, key=lambda x: x[1], reverse=True)
    top = probs_sorted[0][1]
    snd = probs_sorted[1][1]
    margin = float("nan")
    try:
        margin = float(top - snd)
    except Exception:
        pass
    return probs_sorted[0][0], margin


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID da rodada OU caminho data/out/<id>")
    ap.add_argument("--debug", action="store_true", help="Imprime logs detalhados")
    args = ap.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    log(f"OUT_DIR = {out_dir}", args.debug)

    # Entrada
    fp_cons = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(fp_cons):
        raise FileNotFoundError(f"[predict] arquivo não encontrado: {fp_cons}")

    df = pd.read_csv(fp_cons)
    log(f"usando odds_consensus.csv ({fp_cons})", args.debug)

    # Garantias de colunas + saneamento
    df = ensure_columns(df, out_dir, args.debug)

    # Calcula probabilidades implícitas e pick
    out_rows = []
    for _, r in df.iterrows():
        oh = r["odds_home"]
        od = r["odds_draw"]
        oa = r["odds_away"]
        ph, pd_, pa = implied_probs(oh, od, oa)
        pick, margin = pick_from_probs(ph, pd_, pa)

        out_rows.append(
            {
                "match_key": r["match_key"],
                "home": r["team_home"],
                "away": r["team_away"],
                "odd_home": oh,
                "odd_draw": od,
                "odd_away": oa,
                "p_home": ph,
                "p_draw": pd_,
                "p_away": pa,
                "pick_1x2": pick,
                "conf_margin": margin,
            }
        )

    out_df = pd.DataFrame(out_rows)

    # Ordena por conf_margin desc (opcional)
    out_df = out_df.sort_values(by="conf_margin", ascending=False, na_position="last").reset_index(drop=True)

    # Saída
    out_path = os.path.join(out_dir, "predictions_market.csv")
    out_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # Log resumido
    if args.debug:
        print(out_df.head(20).to_string(index=False))
        print(out_df.head(20).to_csv(index=False))


if __name__ == "__main__":
    main()