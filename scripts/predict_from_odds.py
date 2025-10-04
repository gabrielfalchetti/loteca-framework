#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import json
from typing import Optional, Tuple
import pandas as pd
import numpy as np

def log(msg: str):
    print(f"[predict] {msg}", flush=True)

def resolve_out_dir(rodada: str) -> str:
    """
    Se 'rodada' for um caminho (contiver '/' ou começar por 'data/'), usa como está.
    Caso contrário, assume que é um identificador e resolve para data/out/<rodada>.
    """
    if not rodada or str(rodada).strip() == "":
        raise ValueError("valor vazio para --rodada")
    r = rodada.strip()
    if r.startswith("data/") or (os.sep in r):
        return r
    return os.path.join("data", "out", r)

def _load_csv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        log(f"ERRO ao ler {path}: {e}")
        return None

def pick_input_file(out_dir: str, debug: bool=False) -> Tuple[str, pd.DataFrame]:
    """
    Prioridade:
      1) odds_consensus.csv
      2) odds_theoddsapi.csv
      3) odds_apifootball.csv
    Retorna (path, df). Lança erro se nenhum existir.
    """
    cand = [
        os.path.join(out_dir, "odds_consensus.csv"),
        os.path.join(out_dir, "odds_theoddsapi.csv"),
        os.path.join(out_dir, "odds_apifootball.csv"),
    ]
    for p in cand:
        df = _load_csv(p)
        if df is not None and len(df) > 0:
            if debug:
                log(f"entrada: {os.path.basename(p)} -> {len(df)} linhas")
            return p, df
        else:
            if debug:
                log(f"AVISO: {p} não existe ou está vazio.")
    raise FileNotFoundError("nenhuma fonte de odds encontrada no out_dir.")

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante colunas: match_key, team_home, team_away, odds_home, odds_draw, odds_away
    e tipifica odds como numéricas.
    """
    df = df.copy()
    # nomes aceitos
    expected = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]
    for c in ["odds_home","odds_draw","odds_away"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # tenta montar match_key se faltar
    if "match_key" not in df.columns:
        th = df.get("team_home")
        ta = df.get("team_away")
        if th is not None and ta is not None:
            df["match_key"] = th.astype(str).str.strip().str.lower() + "__vs__" + ta.astype(str).str.strip().str.lower()
        else:
            df["match_key"] = np.nan

    # garante team_home/away
    if "team_home" not in df.columns:
        df["team_home"] = np.nan
    if "team_away" not in df.columns:
        df["team_away"] = np.nan

    return df[["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]]

def implied_probs_no_overround(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte odds em probabilidades implícitas simples (sem overround).
    Normaliza por linha para que some 1.0 quando possível.
    """
    df = df.copy()
    for c in ["odds_home","odds_draw","odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    inv_home = 1.0 / df["odds_home"]
    inv_draw = 1.0 / df["odds_draw"]
    inv_away = 1.0 / df["odds_away"]

    inv_home = inv_home.replace([np.inf, -np.inf], np.nan)
    inv_draw = inv_draw.replace([np.inf, -np.inf], np.nan)
    inv_away = inv_away.replace([np.inf, -np.inf], np.nan)

    sums = inv_home.fillna(0) + inv_draw.fillna(0) + inv_away.fillna(0)
    # evita divisão por zero
    sums = sums.replace(0, np.nan)

    df["prob_home"] = inv_home / sums
    df["prob_draw"] = inv_draw / sums
    df["prob_away"] = inv_away / sums
    return df

def argmax_pred(row: pd.Series) -> Tuple[str, float]:
    vals = {
        "HOME": row.get("prob_home", np.nan),
        "DRAW": row.get("prob_draw", np.nan),
        "AWAY": row.get("prob_away", np.nan),
    }
    # escolhe maior prob válida
    best_label = max(vals, key=lambda k: (vals[k] if pd.notna(vals[k]) else -1))
    conf = float(vals[best_label]) if pd.notna(vals[best_label]) else float("nan")
    return best_label, conf

def main():
    parser = argparse.ArgumentParser(description="Gera predições de mercado a partir das odds disponíveis.")
    parser.add_argument("--rodada", required=True, help="Identificador da rodada (ex: 2025-10-04_1214) OU um caminho de saída (ex: data/out/XYZ)")
    parser.add_argument("--debug", action="store_true", help="Modo verboso")
    args = parser.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    if args.debug:
        log(f"out_dir: {out_dir}")

    in_path, df = pick_input_file(out_dir, debug=args.debug)
    df = ensure_columns(df)

    # filtra odds válidas (>1.0)
    mask_valid = (
        (df["odds_home"].astype(float) > 1.0) |
        (df["odds_draw"].astype(float) > 1.0) |
        (df["odds_away"].astype(float) > 1.0)
    )
    df = df[mask_valid].reset_index(drop=True)

    if len(df) == 0:
        log("AVISO: nenhuma linha de odds > 1.0. Nada a prever.")
        # ainda assim grava um CSV vazio com cabeçalho
        empty_out = df.copy()
        for col in ["prob_home","prob_draw","prob_away","pred","pred_conf"]:
            empty_out[col] = []
        empty_out.to_csv(os.path.join(out_dir, "predictions_market.csv"), index=False)
        sys.exit(0)

    # probs implícitas
    df = implied_probs_no_overround(df)

    # predição = argmax
    preds, confs = [], []
    for _, row in df.iterrows():
        p, c = argmax_pred(row)
        preds.append(p)
        confs.append(c)
    df["pred"] = preds
    df["pred_conf"] = confs

    # sample de debug
    if args.debug:
        sample = df.head(5)[[
            "match_key","team_home","team_away",
            "odds_home","odds_draw","odds_away",
            "prob_home","prob_draw","prob_away",
            "pred","pred_conf"
        ]]
        log("AMOSTRA (top 5): " + sample.to_json(orient="records", force_ascii=False))

    out_path = os.path.join(out_dir, "predictions_market.csv")
    df.to_csv(out_path, index=False)
    log(f"OK -> {out_path} ({len(df)} linhas; válidas p/ predição: {len(df)})")

if __name__ == "__main__":
    main()