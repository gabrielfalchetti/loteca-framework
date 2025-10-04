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
    print(f"[cartao] {msg}", flush=True)

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
        log(f"AVISO: falha ao ler {path}: {e}")
        return None

def pick_odds_file(out_dir: str, debug: bool=False) -> Tuple[str, Optional[pd.DataFrame]]:
    cand = [
        os.path.join(out_dir, "odds_consensus.csv"),
        os.path.join(out_dir, "odds_theoddsapi.csv"),
        os.path.join(out_dir, "odds_apifootball.csv"),
    ]
    for p in cand:
        df = _load_csv(p)
        if df is not None and len(df) > 0:
            if debug:
                log(f"odds: usando {os.path.basename(p)} ({len(df)} linhas)")
            return p, df
        else:
            if debug:
                log(f"odds: {p} ausente ou vazio")
    return "", None

def ensure_odds_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza colunas mínimas para odds.
    """
    df = df.copy()
    if "match_key" not in df.columns:
        th = df.get("team_home")
        ta = df.get("team_away")
        if th is not None and ta is not None:
            df["match_key"] = (
                th.astype(str).str.strip().str.lower()
                + "__vs__"
                + ta.astype(str).str.strip().str.lower()
            )
        else:
            df["match_key"] = np.nan

    if "team_home" not in df.columns:
        df["team_home"] = np.nan
    if "team_away" not in df.columns:
        df["team_away"] = np.nan

    for c in ["odds_home","odds_draw","odds_away"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df[["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]]

def implied_probs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Probabilidades implícitas simples (sem overround), normalizadas por linha.
    """
    df = df.copy()
    for c in ["odds_home","odds_draw","odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    inv = {
        "home": 1.0 / df["odds_home"],
        "draw": 1.0 / df["odds_draw"],
        "away": 1.0 / df["odds_away"],
    }
    for k in inv:
        inv[k] = inv[k].replace([np.inf, -np.inf], np.nan)
    s = inv["home"].fillna(0) + inv["draw"].fillna(0) + inv["away"].fillna(0)
    s = s.replace(0, np.nan)
    df["prob_home"] = inv["home"] / s
    df["prob_draw"] = inv["draw"] / s
    df["prob_away"] = inv["away"] / s
    return df

def pick_from_probs(row: pd.Series):
    vals = {
        "HOME": row.get("prob_home", np.nan),
        "DRAW": row.get("prob_draw", np.nan),
        "AWAY": row.get("prob_away", np.nan),
    }
    choice = max(vals, key=lambda k: (vals[k] if pd.notna(vals[k]) else -1))
    conf = float(vals[choice]) if pd.notna(vals[choice]) else float("nan")
    return choice, conf

def load_predictions(out_dir: str) -> Optional[pd.DataFrame]:
    path = os.path.join(out_dir, "predictions_market.csv")
    df = _load_csv(path)
    if df is None or len(df) == 0:
        return None
    # garante colunas
    for c in ["pred","pred_conf"]:
        if c not in df.columns:
            return None
    # tenta garantir times
    if "team_home" not in df.columns or "team_away" not in df.columns:
        return None
    return df

def build_lines(df: pd.DataFrame) -> list:
    """
    Constrói linhas do cartão no formato:
    N) TIME CASA x TIME FORA — PALPITE: <HOME/DRAW/AWAY> (conf XX.xx%)
    """
    lines = []
    for i, row in df.reset_index(drop=True).iterrows():
        home = str(row.get("team_home", "")).strip()
        away = str(row.get("team_away", "")).strip()
        pred = str(row.get("pred", "")).strip()
        conf = row.get("pred_conf", np.nan)
        conf_pct = f"{conf*100:.2f}%" if pd.notna(conf) else "NA"
        idx = i + 1
        lines.append(f"{idx}) {home} x {away} — PALPITE: {pred} ({conf_pct})")
    return lines

def ensure_nonempty_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se os nomes estiverem vazios, tenta derivar de match_key.
    """
    df = df.copy()
    if ("team_home" in df.columns) and ("team_away" in df.columns):
        need_fix = df["team_home"].isna() | df["team_away"].isna() | (df["team_home"].astype(str)=="") | (df["team_away"].astype(str)=="")
    else:
        need_fix = pd.Series([True]*len(df))
    if need_fix.any():
        def from_key(k):
            if not isinstance(k, str) or "__vs__" not in k:
                return ("", "")
            a, b = k.split("__vs__", 1)
            return (a.strip().title(), b.strip().title())
        homes, aways = [], []
        for k in df.get("match_key", pd.Series([""]*len(df))):
            h, a = from_key(k)
            homes.append(h)
            aways.append(a)
        if "team_home" not in df.columns:
            df["team_home"] = homes
        else:
            df.loc[need_fix, "team_home"] = pd.Series(homes)[need_fix.values]
        if "team_away" not in df.columns:
            df["team_away"] = aways
        else:
            df.loc[need_fix, "team_away"] = pd.Series(aways)[need_fix.values]
    return df

def main():
    parser = argparse.ArgumentParser(description="Gera o arquivo loteca_cartao.txt a partir de predições ou odds.")
    parser.add_argument("--rodada", required=True, help="Identificador ou caminho (ex: 2025-10-04_1214 ou data/out/XYZ)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    if args.debug:
        log(f"out_dir: {out_dir}")

    # 1) tenta carregar predictions_market.csv
    pred_df = load_predictions(out_dir)

    if pred_df is None:
        log("AVISO: predictions_market.csv ausente ou vazio. Caindo para odds.")
        # 2) carrega odds e produz predições simples on-the-fly
        odds_path, odds_df = pick_odds_file(out_dir, debug=args.debug)
        if not odds_df or len(odds_df) == 0:
            log("ERRO: nenhuma fonte de odds disponível.")
            sys.exit(1)
        odds_df = ensure_odds_columns(odds_df)
        # filtra odds válidas (>1.0)
        mask = (
            (odds_df["odds_home"].astype(float) > 1.0) |
            (odds_df["odds_draw"].astype(float) > 1.0) |
            (odds_df["odds_away"].astype(float) > 1.0)
        )
        odds_df = odds_df[mask].reset_index(drop=True)
        if len(odds_df) == 0:
            log("ERRO: odds presentes, porém todas <= 1.0.")
            sys.exit(1)
        odds_df = implied_probs(odds_df)
        preds, confs = [], []
        for _, row in odds_df.iterrows():
            p, c = pick_from_probs(row)
            preds.append(p)
            confs.append(c)
        odds_df["pred"] = preds
        odds_df["pred_conf"] = confs
        pred_df = odds_df

    # garante nomes de times
    pred_df = ensure_nonempty_names(pred_df)

    # monta linhas do cartão
    lines = build_lines(pred_df)

    out_path = os.path.join(out_dir, "loteca_cartao.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    if args.debug:
        log("EXEMPLO:")
        for l in lines[:5]:
            log("  " + l)

    log(f"OK -> {out_path}")

if __name__ == "__main__":
    main()