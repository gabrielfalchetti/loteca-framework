#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera previsões a partir das odds de mercado (consenso).
- Entrada:  data/out/{RODADA}/odds_consensus.csv
- Saída:    data/out/{RODADA}/predictions_market.csv

Lógica:
- Usa odds_home / odds_draw / odds_away (exige >= 2 odds válidas > 1.0).
- Converte para probabilidades implícitas e normaliza (corrige overround).
- Predição = argmax(prob_*), confiança = probabilidade do argmax.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd  # <<< IMPORT CORRETO


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Predict from market odds")
    p.add_argument("--rodada", required=True, help="Identificador da rodada (ex: 2025-09-27_1213)")
    p.add_argument("--debug", action="store_true", help="Debug logs")
    return p.parse_args()


def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)


def normalize_probs(odds: Dict[str, Optional[float]]) -> Dict[str, float]:
    """
    Converte odds decimais para probabilidades implícitas corrigindo overround.
    Considera apenas odds > 1.0. Se menos de duas odds válidas, retorna todas 0.
    """
    invs = {}
    for k, v in odds.items():
        try:
            fv = float(v)
        except Exception:
            fv = np.nan
        if np.isfinite(fv) and fv > 1.0:
            invs[k] = 1.0 / fv

    if len(invs) < 2:
        return {"home": 0.0, "draw": 0.0, "away": 0.0}

    s = sum(invs.values())
    if s <= 0 or not np.isfinite(s):
        return {"home": 0.0, "draw": 0.0, "away": 0.0}

    return {
        "home": invs.get("home", 0.0) / s,
        "draw": invs.get("draw", 0.0) / s,
        "away": invs.get("away", 0.0) / s,
    }


def pick_prediction(ph: float, pd_: float, pa: float) -> Tuple[str, float]:
    arr = np.array([ph, pd_, pa], dtype=float)
    labels = np.array(["HOME", "DRAW", "AWAY"])
    idx = int(np.argmax(arr))
    return labels[idx], float(arr[idx])


def main() -> None:
    args = parse_args()
    rodada = args.rodada
    debug = args.debug or (os.getenv("DEBUG", "false").lower() == "true")

    in_path = os.path.join("data", "out", rodada, "odds_consensus.csv")
    out_path = os.path.join("data", "out", rodada, "predictions_market.csv")

    if not os.path.isfile(in_path):
        print(f"[predict] ERRO: arquivo não encontrado: {in_path}", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_csv(in_path)
    except Exception as e:
        print(f"[predict] ERRO ao ler {in_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Checagem de colunas mínimas
    required_cols = ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
    for c in required_cols:
        if c not in df.columns:
            print(f"[predict] ERRO: coluna ausente em odds_consensus.csv: {c}", file=sys.stderr)
            sys.exit(2)

    # match_key se não existir
    if "match_key" not in df.columns:
        def mk(row):
            th = str(row.get("team_home", "")).strip().lower()
            ta = str(row.get("team_away", "")).strip().lower()
            return f"{th}__vs__{ta}"
        df["match_key"] = df.apply(mk, axis=1)

    probs_h, probs_d, probs_a, preds, confs = [], [], [], [], []
    valid_rows = 0

    for _, row in df.iterrows():
        odds = {
            "home": row.get("odds_home", np.nan),
            "draw": row.get("odds_draw", np.nan),
            "away": row.get("odds_away", np.nan),
        }
        pr = normalize_probs(odds)
        ph, pd_, pa = pr["home"], pr["draw"], pr["away"]

        is_valid = sum(
            1
            for k in ["home", "draw", "away"]
            if (row.get(f"odds_{k}", np.nan) is not None
                and np.isfinite(float(row.get(f"odds_{k}", np.nan)))
                and float(row.get(f"odds_{k}", np.nan)) > 1.0)
        ) >= 2

        if is_valid:
            pred, conf = pick_prediction(ph, pd_, pa)
            valid_rows += 1
        else:
            pred, conf = "NA", 0.0

        probs_h.append(ph)
        probs_d.append(pd_)
        probs_a.append(pa)
        preds.append(pred)
        confs.append(conf)

    out = pd.DataFrame({
        "match_key": df["match_key"],
        "team_home": df["team_home"],
        "team_away": df["team_away"],
        "odds_home": df["odds_home"],
        "odds_draw": df["odds_draw"],
        "odds_away": df["odds_away"],
        "prob_home": probs_h,
        "prob_draw": probs_d,
        "prob_away": probs_a,
        "pred": preds,
        "pred_conf": confs,
    })

    out = out.sort_values(["pred_conf", "match_key"], ascending=[False, True]).reset_index(drop=True)
    ensure_dir(out_path)
    out.to_csv(out_path, index=False)

    if debug:
        print("[predict] AMOSTRA (top 5):", json.dumps(out.head(5).to_dict(orient="records"), ensure_ascii=False))

    print(f"[predict] OK -> {out_path} ({len(out)} linhas; válidas p/ predição: {valid_rows})")


if __name__ == "__main__":
    main()
