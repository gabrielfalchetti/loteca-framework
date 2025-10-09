#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
predict_from_odds.py (STRICT)

Lê <OUT_DIR>/odds_consensus.csv e gera <OUT_DIR>/predictions_market.csv
a partir das odds, derivando probabilidades FAIR:
  imp = 1/odds ; fair_p = imp / sum(imp)

Regras estritas:
- Se odds_consensus.csv estiver ausente, vazio ou sem colunas esperadas,
  o script encerra com exit code 7 (erro).
- Proíbe odds <= 1.0.
- Normaliza chaves e nomes de times.
"""

import os
import sys
import argparse
import pandas as pd
import math


REQ_COLS = ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]


def die(msg: str, code: int = 7):
    print(f"##[error]{msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def read_csv_strict(path: str) -> pd.DataFrame:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        die(f"odds_consensus.csv ausente ou vazio em {path}.")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"Falha lendo {path}: {e}")
    if df.empty:
        die(f"odds_consensus.csv está vazio em {path}.")
    return df


def std_teams(df: pd.DataFrame) -> pd.DataFrame:
    # aceita 'home'/'away' e renomeia para 'team_home'/'team_away'
    if "team_home" not in df.columns and "home" in df.columns:
        df = df.rename(columns={"home": "team_home"})
    if "team_away" not in df.columns and "away" in df.columns:
        df = df.rename(columns={"away": "team_away"})
    return df


def ensure_match_id(df: pd.DataFrame) -> pd.DataFrame:
    if "match_id" not in df.columns:
        if "team_home" in df.columns and "team_away" in df.columns:
            df["match_id"] = df["team_home"].astype(str) + "__" + df["team_away"].astype(str)
        elif "match_key" in df.columns:
            df["match_id"] = df["match_key"].astype(str)
        else:
            die("Não foi possível derivar 'match_id' (faltam colunas team_home/team_away).")
    df["match_id"] = df["match_id"].astype(str)
    return df


def fair_probs_from_odds(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["odds_home", "odds_draw", "odds_away"]:
        if c not in df.columns:
            die(f"Coluna obrigatória ausente: {c}")
        if (df[c] <= 1.0).any():
            bad = df.loc[df[c] <= 1.0, ["team_home","team_away",c]].head(5)
            die(f"Odds inválidas (<=1.0) na coluna {c}. Amostra:\n{bad.to_string(index=False)}")
    imp_h = 1.0 / df["odds_home"]
    imp_d = 1.0 / df["odds_draw"]
    imp_a = 1.0 / df["odds_away"]
    over = imp_h + imp_d + imp_a
    if (over <= 0).any():
        die("Soma de probabilidades implícitas <= 0 (overround inválido).")
    df["p_home"] = imp_h / over
    df["p_draw"] = imp_d / over
    df["p_away"] = imp_a / over
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/12345)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)
    odds_path = os.path.join(out_dir, "odds_consensus.csv")
    out_path = os.path.join(out_dir, "predictions_market.csv")

    df = read_csv_strict(odds_path)
    df = std_teams(df)

    # padroniza nomes de colunas possivelmente diferentes
    if "odd_home" in df.columns and "odds_home" not in df.columns:
        df = df.rename(columns={"odd_home": "odds_home"})
    if "odd_draw" in df.columns and "odds_draw" not in df.columns:
        df = df.rename(columns={"odd_draw": "odds_draw"})
    if "odd_away" in df.columns and "odds_away" not in df.columns:
        df = df.rename(columns={"odd_away": "odds_away"})

    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        die(f"Colunas obrigatórias ausentes em odds_consensus.csv: {missing}")

    df = ensure_match_id(df)
    df = fair_probs_from_odds(df)

    # saída estrita e limpa
    out = df[["match_id","team_home","team_away",
              "odds_home","odds_draw","odds_away",
              "p_home","p_draw","p_away"]].copy()
    out.to_csv(out_path, index=False)

    if args.debug:
        print(f"[predict] OUT_DIR = {out_dir}")
        print(f"[predict] usando odds_consensus.csv ({odds_path})")
        print(out.head(10).to_csv(index=False))

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        die(f"Erro inesperado: {e}")