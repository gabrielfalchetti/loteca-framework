#!/usr/bin/env python3
# scripts/predict_from_odds.py
from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Tuple

import pandas as pd
import numpy as np


REQUIRED = ["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
OUT_COLS = [
    "match_id",
    "team_home",
    "team_away",
    "prob_home",
    "prob_draw",
    "prob_away",
    "pick",
    "margin",
    "notes",
]


def log(msg: str, flush: bool = True):
    print(msg, flush=flush)


def err(msg: str, code: int):
    print(f"##[error]{msg}")
    sys.exit(code)


def read_consensus(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        err(f"Arquivo não encontrado: {path}", 7)
    try:
        df = pd.read_csv(path)
    except Exception as e:
        err(f"Falha ao ler {path}: {e}", 7)
    return df


def coerce_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan


def implied_probs_3way(oh: float, od: float, oa: float) -> Tuple[float, float, float, float]:
    """
    Converte odds decimais (home/draw/away) em probabilidades implícitas,
    removendo o vigorish pela normalização simples.
    Retorna (ph, pd, pa, oi_sum) onde oi_sum = soma das probabilidades implícitas brutas (com juice).
    """
    inv = []
    for o in [oh, od, oa]:
        if o is None or np.isnan(o) or o <= 1.0:
            inv.append(np.nan)
        else:
            inv.append(1.0 / o)
    if any(np.isnan(inv)):
        return (np.nan, np.nan, np.nan, np.nan)
    s = sum(inv)
    return inv[0] / s, inv[1] / s, inv[2] / s, s


def implied_probs_2way(oh: float, oa: float) -> Tuple[float, float, float, float]:
    """
    Converte odds decimais (home/away) em probabilidades implícitas 2-way (sem empate).
    Retorna (ph, 0.0, pa, oi_sum)
    """
    inv_h = np.nan if (oh is None or np.isnan(oh) or oh <= 1.0) else 1.0 / oh
    inv_a = np.nan if (oa is None or np.isnan(oa) or oa <= 1.0) else 1.0 / oa
    if np.isnan(inv_h) or np.isnan(inv_a):
        return (np.nan, np.nan, np.nan, np.nan)
    s = inv_h + inv_a
    return inv_h / s, 0.0, inv_a / s, s


def choose_pick(ph: float, pd: float, pa: float) -> str:
    if np.isnan(ph) and np.isnan(pd) and np.isnan(pa):
        return ""
    arr = np.array([ph, pd, pa], dtype=float)
    idx = int(np.nanargmax(arr))
    return ["HOME", "DRAW", "AWAY"][idx]


def validate_and_prepare(df: pd.DataFrame, strict: bool, allow_two_way: bool, debug: bool) -> pd.DataFrame:
    # Renomeações defensivas (se vieram nomes alternativos)
    ren = {}
    lower = {c.lower(): c for c in df.columns}
    for want in REQUIRED:
        if want in df.columns:
            continue
        # mapas simples
        alt = {
            "team_home": ["home", "time_casa"],
            "team_away": ["away", "time_fora"],
            "odds_home": ["price_home", "home_odds"],
            "odds_draw": ["price_draw", "draw_odds"],
            "odds_away": ["price_away", "away_odds"],
            "match_id": ["id", "game_id"],
        }.get(want, [])
        found = None
        for a in alt:
            if a in lower:
                found = lower[a]
                break
        if found:
            ren[found] = want

    if ren:
        df = df.rename(columns=ren)

    # Garantir colunas presentes (cria vazias quando não existem)
    for c in REQUIRED:
        if c not in df.columns:
            df[c] = np.nan

    # Tipagens
    df["match_id"] = df["match_id"].astype(str)
    for c in ["team_home", "team_away"]:
        df[c] = df[c].astype(str)

    for c in ["odds_home", "odds_draw", "odds_away"]:
        df[c] = df[c].apply(coerce_float)

    # Limpeza: odds <= 1.0 => NaN
    for c in ["odds_home", "odds_draw", "odds_away"]:
        df.loc[df[c] <= 1.0, c] = np.nan

    # Marcar linhas 2-way (sem empate)
    df["two_way"] = df["odds_draw"].isna()

    if not allow_two_way and df["two_way"].any():
        bad = df[df["two_way"]][["match_id", "team_home", "team_away"]]
        if strict:
            log("##[error][predict] Mercados 2-way detectados mas --allow-two-way está desabilitado.")
            log(bad.to_string(index=False))
            sys.exit(98)
        # tolerante: descarta 2-way
        df = df[~df["two_way"]].copy()

    # Se uma linha 3-way tiver qualquer odd inválida, marcar para descarte
    df["invalid_3w"] = (~df["two_way"]) & (
        df["odds_home"].isna() | df["odds_draw"].isna() | df["odds_away"].isna()
    )

    # Em modo estrito, se existir qualquer inválida => aborta
    if strict and (df["invalid_3w"].any() or (not allow_two_way and df["two_way"].any())):
        ex = df[df["invalid_3w"] | (df["two_way"] & ~df["two_way"].isna())][
            ["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
        ]
        err("[predict] Odds inválidas detectadas (NaN ou <= 1.0). Abortando.\n" + ex.to_string(index=False), 98)

    # Tolerante: mantemos 2-way (se permitido) e removemos apenas 3-way inválidas
    kept = len(df)
    df = df[~df["invalid_3w"]].copy()
    removed = kept - len(df)

    if debug:
        log(f"[predict][DEBUG] total={kept} removidos_por_invalid_3way={removed} two_way={int(df['two_way'].sum())}")

    # Se tudo caiu fora, falha
    if len(df) == 0:
        err("[predict] Nenhuma linha válida para gerar previsões após limpeza/validação.", 7)

    return df


def compute_predictions(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        oh, od, oa = r["odds_home"], r["odds_draw"], r["odds_away"]
        if pd.isna(od):  # 2-way
            ph, pdw, pa, s = implied_probs_2way(oh, oa)
            notes = "two_way"
        else:  # 3-way
            ph, pdw, pa, s = implied_probs_3way(oh, od, oa)
            notes = "three_way"

        if any(np.isnan([ph, pdw, pa])):
            # segurança extra: pula linha
            continue

        pick = choose_pick(ph, pdw, pa)
        # margem simples = maior prob - segundo maior
        arr = np.array([ph, pdw, pa], dtype=float)
        top2 = np.sort(arr)[-2:]
        margin = float(top2[-1] - top2[-2])

        rows.append(
            {
                "match_id": r["match_id"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "prob_home": round(ph, 6),
                "prob_draw": round(pdw, 6),
                "prob_away": round(pa, 6),
                "pick": pick,
                "margin": round(margin, 6),
                "notes": notes,
            }
        )

    return pd.DataFrame(rows, columns=OUT_COLS)


def main():
    ap = argparse.ArgumentParser(description="Gera predictions_market.csv a partir de odds_consensus.csv")
    ap.add_argument("--rodada", required=True, help="Diretório (OUT_DIR) onde está o odds_consensus.csv")
    ap.add_argument("--strict", action="store_true", help="Falha se houver qualquer odd inválida/NaN/≤1.0")
    ap.add_argument("--allow-two-way", dest="allow_two_way", action="store_true", help="Permite mercados sem empate (2-way).")
    ap.add_argument("--no-allow-two-way", dest="allow_two_way", action="store_false", help="Proíbe 2-way; descarta/aborta conforme modo.")
    ap.add_argument("--debug", action="store_true", help="Logs detalhados")
    ap.set_defaults(allow_two_way=True)
    args = ap.parse_args()

    rodada_dir = args.rodada
    in_path = os.path.join(rodada_dir, "odds_consensus.csv")
    out_path = os.path.join(rodada_dir, "predictions_market.csv")

    log(f"[predict] OUT_DIR = {rodada_dir}")

    df = read_consensus(in_path)

    # Validação + limpeza (aceita 2-way por padrão)
    df = validate_and_prepare(df, strict=args.strict, allow_two_way=args.allow_two_way, debug=args.debug)

    # Cálculo das probabilidades e picks
    pred = compute_predictions(df)

    if pred.empty:
        err("[predict] Nenhuma previsão produzida (todas as linhas inválidas após cálculo).", 7)

    # Ordena por margem desc
    pred = pred.sort_values(by=["margin"], ascending=False).reset_index(drop=True)

    # Persistência
    os.makedirs(rodada_dir, exist_ok=True)
    pred.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # Preview
    head_n = min(20, len(pred))
    log(f"[predict] OK -> {out_path} (linhas={len(pred)})")
    log("===== Preview predictions_market =====")
    log(pred.head(head_n).to_string(index=False))


if __name__ == "__main__":
    main()