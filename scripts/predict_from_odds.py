#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import json
from typing import Tuple
import pandas as pd  # <- IMPORTA pandas corretamente
import numpy as np

def choose_input_file(out_dir: str, debug: bool) -> Tuple[str, str]:
    """
    Escolhe o arquivo de entrada de odds:
      1) odds_consensus.csv se existir e tiver linhas
      2) fallback: odds_theoddsapi.csv
    Retorna (in_path, source_tag)
    """
    c1 = os.path.join(out_dir, "odds_consensus.csv")
    c2 = os.path.join(out_dir, "odds_theoddsapi.csv")

    if os.path.isfile(c1):
        try:
            if pd.read_csv(c1).shape[0] > 0:
                if debug:
                    print(f"[predict] usando odds_consensus.csv ({c1})")
                return c1, "consensus"
        except Exception as e:
            if debug:
                print(f"[predict] aviso: falha ao ler {c1}: {e}")

    if os.path.isfile(c2):
        try:
            if pd.read_csv(c2).shape[0] > 0:
                if debug:
                    print(f"[predict] usando odds_theoddsapi.csv ({c2})")
                return c2, "theoddsapi"
        except Exception as e:
            if debug:
                print(f"[predict] aviso: falha ao ler {c2}: {e}")

    raise FileNotFoundError("Nenhuma fonte de odds disponível em OUT_DIR (odds_consensus.csv nem odds_theoddsapi.csv).")

def ensure_columns(df: pd.DataFrame, out_dir: str) -> pd.DataFrame:
    """
    Garante colunas-padrão: team_home, team_away, match_key (se possível),
    odds_home, odds_draw, odds_away. Tenta mapear nomes comuns.
    """
    colmap_candidates = [
        # já no padrão
        {"team_home":"team_home", "team_away":"team_away", "match_key":"match_key",
         "odds_home":"odds_home","odds_draw":"odds_draw","odds_away":"odds_away"},
        # alguns fornecedores usam "home"/"away"
        {"team_home":"home", "team_away":"away", "match_key":"match_key",
         "odds_home":"odds_home","odds_draw":"odds_draw","odds_away":"odds_away"},
        # apifootball pode vir com outros nomes (ex.: home_team, away_team)
        {"team_home":"home_team", "team_away":"away_team", "match_key":"match_key",
         "odds_home":"odds_home","odds_draw":"odds_draw","odds_away":"odds_away"},
    ]

    cols = {c.lower(): c for c in df.columns}
    df = df.rename(columns=cols)  # normaliza para minúsculas

    for m in colmap_candidates:
        if all(col in df.columns for col in m.values()):
            # renomeia para padrão
            inv = {v:k for k,v in m.items()}
            df = df.rename(columns=inv)
            break

    # tenta criar match_key se não existir
    if "match_key" not in df.columns:
        if "team_home" in df.columns and "team_away" in df.columns:
            mk = (
                df["team_home"].astype(str).str.strip().str.lower()
                + "__vs__" +
                df["team_away"].astype(str).str.strip().str.lower()
            )
            df["match_key"] = mk
        else:
            raise ValueError("[predict] impossivel criar match_key (faltam team_home/team_away).")

    required = ["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[predict] faltam colunas obrigatórias: {missing}")

    # Sanitiza odds para float e > 1.0
    for c in ["odds_home","odds_draw","odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["odds_home","odds_draw","odds_away"]).copy()
    df = df[(df["odds_home"]>1.0) & (df["odds_draw"]>1.0) & (df["odds_away"]>1.0)].copy()

    if df.empty:
        raise ValueError("[predict] nenhuma linha de odds válida (>1.0).")

    return df

def add_probs_if_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se não houver prob_home/prob_draw/prob_away, calcula por probabilidades implícitas
    com normalização simples (sem overround correction).
    """
    has_probs = all(c in df.columns for c in ["prob_home","prob_draw","prob_away"])
    if not has_probs:
        imp_home = 1.0 / df["odds_home"]
        imp_draw = 1.0 / df["odds_draw"]
        imp_away = 1.0 / df["odds_away"]
        s = imp_home + imp_draw + imp_away
        df["prob_home"] = (imp_home / s).clip(0,1)
        df["prob_draw"] = (imp_draw / s).clip(0,1)
        df["prob_away"] = (imp_away / s).clip(0,1)
    else:
        # garante limites [0,1]
        for c in ["prob_home","prob_draw","prob_away"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").clip(0,1)
    return df

def make_predictions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Predição = argmax das probabilidades (HOME/DRAW/AWAY).
    """
    probs = df[["prob_home","prob_draw","prob_away"]].to_numpy(dtype=float)
    idx = np.nanargmax(probs, axis=1)
    label_map = {0:"HOME", 1:"DRAW", 2:"AWAY"}
    df["pred"] = [label_map[i] for i in idx]
    df["pred_conf"] = probs[np.arange(len(df)), idx]
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Pasta OUT_DIR (ex.: data/out/<RUN_ID>)")
    ap.add_argument("--debug", action="store_true", help="Mostra logs detalhados")
    args = ap.parse_args()

    out_dir = args.rodada
    debug = args.debug

    if debug:
        print(f"[predict] OUT_DIR = {out_dir}")

    if not os.path.isdir(out_dir):
        print(f"[predict] ERRO: diretório inexistente: {out_dir}", file=sys.stderr)
        sys.exit(2)

    # Escolhe arquivo de entrada
    in_path, source = choose_input_file(out_dir, debug)

    # Lê e normaliza
    df = pd.read_csv(in_path)
    df = ensure_columns(df, out_dir)
    df = add_probs_if_missing(df)

    # Predição
    df = make_predictions(df)

    # Seleção de colunas de saída
    out_cols = [
        "match_key","team_home","team_away",
        "odds_home","odds_draw","odds_away",
        "prob_home","prob_draw","prob_away",
        "pred","pred_conf"
    ]
    # Preserva colunas desconhecidas também? Não — mantemos objetivo simples/estável
    out_df = df[out_cols].copy()

    # Log amostra
    if debug:
        sample = out_df.head(5).to_dict(orient="records")
        print(f"[predict] AMOSTRA (top 5): {json.dumps(sample, ensure_ascii=False)}")

    # Salva
    out_file = os.path.join(out_dir, "predictions_market.csv")
    out_df.to_csv(out_file, index=False, encoding="utf-8")
    if debug:
        print(f"[predict] OK -> {out_file} ({len(out_df)} linhas; válidas p/ predição: {len(out_df)})")

if __name__ == "__main__":
    main()