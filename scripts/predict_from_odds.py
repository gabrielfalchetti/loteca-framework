#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
predict_from_odds.py

Lê odds de data/out/<RID>/odds_consensus.csv, corrige overround e produz:
  data/out/<RID>/predictions_market.csv

Colunas de saída:
  match_key,home,away,odd_home,odd_draw,odd_away,p_home,p_draw,p_away,pick_1x2,conf_margin

Robustez:
- Aceita cabeçalhos 'team_home/team_away' ou 'home/away'
- Gera match_key determinístico "<home>__vs__<away>" (minúsculo, sem acento)
- Ignora linhas com odds inválidas
"""

import argparse
import csv
import os
import sys
import unicodedata
from math import isfinite

import pandas as pd


def ffloat(x, default=0.0):
    try:
        v = float(x)
        return v if isfinite(v) else default
    except Exception:
        return default


def strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))


def norm_name(s: str) -> str:
    s = strip_accents(str(s or "")).strip().lower()
    s = s.replace("  ", " ")
    s = s.replace("_", " ").replace("-", " ")
    s = " ".join(s.split())
    # normalizações leves usadas no restante do pipeline
    s = s.replace("atletico mg", "atletico-mg")
    s = s.replace("america mg", "america-mg")
    s = s.replace("operario pr", "operario-pr")
    s = s.replace("athletico pr", "athletico-pr")
    s = s.replace("atletico go", "atletico-go")
    s = s.replace("sao bernardo", "sao bernardo")
    s = s.replace("goias", "goias")
    s = s.replace("cuiaba", "cuiaba")
    return s


def make_match_key(home: str, away: str) -> str:
    h = norm_name(home)
    a = norm_name(away)
    return f"{h}__vs__{a}"


def remove_overround(oh: float, od: float, oa: float):
    """
    Converte odds brutas (com overround) em probabilidades "justas".
    """
    if oh <= 1e-9 or od <= 1e-9 or oa <= 1e-9:
        return (1/3, 1/3, 1/3)
    imp_h = 1.0 / oh
    imp_d = 1.0 / od
    imp_a = 1.0 / oa
    over = imp_h + imp_d + imp_a
    if over <= 0:
        return (1/3, 1/3, 1/3)
    return (imp_h / over, imp_d / over, imp_a / over)


def pick_1x2(ph: float, pd: float, pa: float):
    trio = [("1", ph), ("X", pd), ("2", pa)]
    trio.sort(key=lambda t: t[1], reverse=True)
    fav, fav_p = trio[0]
    second_p = trio[1][1]
    return fav, fav_p - second_p


def ensure_columns(df: pd.DataFrame, out_dir: str, debug: bool) -> pd.DataFrame:
    # mapeia nomes
    if "team_home" in df.columns and "team_away" in df.columns:
        df["home"] = df["team_home"].astype(str)
        df["away"] = df["team_away"].astype(str)
    elif "home" in df.columns and "away" in df.columns:
        df["home"] = df["home"].astype(str)
        df["away"] = df["away"].astype(str)
    else:
        raise ValueError("Colunas de times não encontradas (esperado team_home/team_away ou home/away)")

    # odds com vários possíveis nomes
    def first_present(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    c_h = first_present("odds_home", "odd_home", "home_odds")
    c_d = first_present("odds_draw", "odd_draw", "draw_odds")
    c_a = first_present("odds_away", "odd_away", "away_odds")
    if not all([c_h, c_d, c_a]):
        raise ValueError("Colunas de odds não encontradas (odds_home/odds_draw/odds_away).")

    df = df.copy()
    df["odd_home"] = pd.to_numeric(df[c_h], errors="coerce")
    df["odd_draw"] = pd.to_numeric(df[c_d], errors="coerce")
    df["odd_away"] = pd.to_numeric(df[c_a], errors="coerce")

    # cria match_key **como string única** (o bug estava aqui)
    df["match_key"] = df.apply(lambda r: make_match_key(r["home"], r["away"]), axis=1)

    # limpa linhas inválidas
    before = len(df)
    df = df.dropna(subset=["odd_home", "odd_draw", "odd_away"])
    df = df[(df["odd_home"] > 1.01) & (df["odd_draw"] > 1.01) & (df["odd_away"] > 1.01)]
    if debug:
        print(f"[predict][DEBUG] linhas válidas: {len(df)} (antes {before})")

    return df[["match_key", "home", "away", "odd_home", "odd_draw", "odd_away"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (data/out/<RID>)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    infile = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(infile):
        print(f"::error::Arquivo não encontrado: {infile}", file=sys.stderr)
        sys.exit(7)

    df = pd.read_csv(infile)
    df = ensure_columns(df, out_dir, args.debug)

    # prob "fair"
    probs = df.apply(lambda r: remove_overround(r["odd_home"], r["odd_draw"], r["odd_away"]), axis=1)
    df["p_home"] = probs.apply(lambda t: float(t[0]))
    df["p_draw"] = probs.apply(lambda t: float(t[1]))
    df["p_away"] = probs.apply(lambda t: float(t[2]))

    # decisão 1X2 e margem de confiança
    picks = df.apply(lambda r: pick_1x2(r["p_home"], r["p_draw"], r["p_away"]), axis=1)
    df["pick_1x2"] = picks.apply(lambda t: t[0])
    df["conf_margin"] = picks.apply(lambda t: float(t[1]))

    out_cols = ["match_key", "home", "away", "odd_home", "odd_draw", "odd_away",
                "p_home", "p_draw", "p_away", "pick_1x2", "conf_margin"]
    out_path = os.path.join(out_dir, "predictions_market.csv")
    df[out_cols].to_csv(out_path, index=False)

    if args.debug:
        print(df[out_cols].head(20).to_csv(index=False))

    print(f"[predict] OK -> {out_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[predict][ERRO] {e}", file=sys.stderr)
        sys.exit(7)