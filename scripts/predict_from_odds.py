#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Deriva probabilidades "de mercado" a partir de odds_consensus.csv (sem inventar dados).
Saída: predictions_market.csv com:
  match_key,home,away,odd_home,odd_draw,odd_away,p_home,p_draw,p_away,pick_1x2,conf_margin

Erros “amigáveis”:
- Falha se odds_consensus.csv estiver vazio/ausente
- Falha se faltar colunas-chave
"""

from __future__ import annotations
import sys
import os
import math
import pandas as pd

EXIT_CODE = 7

def log(msg: str):
    print(f"[predict] {msg}")

def err(msg: str):
    print(f"::error::{msg}", file=sys.stderr)

def normalize_match_key(h: str, a: str) -> str:
    return f"{str(h).strip().lower().replace(' ','-')}__vs__{str(a).strip().lower().replace(' ','-')}"

def implied_probs(odd_home, odd_draw, odd_away):
    # Probabilidades implícitas (com overround)
    imp_h = 1.0/odd_home
    imp_d = 1.0/odd_draw
    imp_a = 1.0/odd_away
    s = imp_h + imp_d + imp_a
    return imp_h/s, imp_d/s, imp_a/s

def main():
    if len(sys.argv) < 3 or sys.argv[1] != "--rodada":
        print("Uso: python scripts/predict_from_odds.py --rodada <OUT_DIR> [--debug]")
        sys.exit(EXIT_CODE)

    out_dir = sys.argv[2]
    debug = "--debug" in sys.argv

    log(f"OUT_DIR = {out_dir}")

    odds_path = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(odds_path):
        err(f"odds_consensus.csv ausente em {odds_path}.")
        sys.exit(EXIT_CODE)

    df = pd.read_csv(odds_path)
    if df.empty:
        err(f"odds_consensus.csv está vazio em {odds_path}.")
        sys.exit(EXIT_CODE)

    needed = {"match_id","team_home","team_away","odds_home","odds_draw","odds_away"}
    if not needed.issubset(df.columns):
        err(f"Colunas ausentes em odds_consensus.csv. Esperado: {sorted(needed)}")
        sys.exit(EXIT_CODE)

    # remover linhas com odds faltantes/zeradas
    df = df.dropna(subset=["odds_home","odds_draw","odds_away"])
    df = df[(df["odds_home"]>0) & (df["odds_draw"]>0) & (df["odds_away"]>0)]
    if df.empty:
        err("Nenhuma linha válida em odds_consensus.csv após limpeza de odds.")
        sys.exit(EXIT_CODE)

    rows = []
    for _, r in df.iterrows():
        h, d, a = float(r["odds_home"]), float(r["odds_draw"]), float(r["odds_away"])
        p_h, p_d, p_a = implied_probs(h, d, a)
        # pick pelo maior p
        if p_h >= p_d and p_h >= p_a:
            pick = "1"
            margin = p_h - max(p_d, p_a)
        elif p_d >= p_h and p_d >= p_a:
            pick = "X"
            margin = p_d - max(p_h, p_a)
        else:
            pick = "2"
            margin = p_a - max(p_h, p_d)

        rows.append({
            "match_key": str(r["match_id"]),
            "home": r["team_home"],
            "away": r["team_away"],
            "odd_home": h,
            "odd_draw": d,
            "odd_away": a,
            "p_home": p_h,
            "p_draw": p_d,
            "p_away": p_a,
            "pick_1x2": pick,
            "conf_margin": margin
        })

    out = os.path.join(out_dir, "predictions_market.csv")
    pd.DataFrame(rows).to_csv(out, index=False)
    log(f"usando odds_consensus.csv ({odds_path})")
    try:
        prev = pd.DataFrame(rows)
        prev["p_home"] = prev["p_home"].round(6)
        prev["p_draw"] = prev["p_draw"].round(6)
        prev["p_away"] = prev["p_away"].round(6)
        print(prev[["match_key","home","away","odd_home","odd_draw","odd_away","p_home","p_draw","p_away","pick_1x2","conf_margin"]].head().to_string(index=False))
    except Exception:
        pass

    # pronto
    if not os.path.isfile(out) or os.path.getsize(out) == 0:
        err("predictions_market.csv não gerado.")
        sys.exit(EXIT_CODE)

if __name__ == "__main__":
    main()