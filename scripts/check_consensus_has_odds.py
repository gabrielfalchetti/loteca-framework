#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, argparse
import pandas as pd
import numpy as np

ODDS = ["odds_home","odds_draw","odds_away"]

def out_dir_for(rodada: str) -> str:
    return os.path.join("data","out",rodada)

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ren = {
        "home":"odds_home", "1":"odds_home", "home_win":"odds_home", "price_home":"odds_home",
        "draw":"odds_draw", "x":"odds_draw", "tie":"odds_draw", "price_draw":"odds_draw",
        "away":"odds_away", "2":"odds_away", "away_win":"odds_away", "price_away":"odds_away",
    }
    rn = {}
    for c in list(out.columns):
        lc = str(c).strip().lower()
        if lc in ren:
            rn[c] = ren[lc]
    if rn:
        out = out.rename(columns=rn)
    for c in ODDS:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

def count_valid_rows(df: pd.DataFrame) -> int:
    def valid(r):
        vals = []
        for c in ODDS:
            v = r.get(c)
            if pd.notna(v) and np.isfinite(v) and v > 1.0:
                vals.append(v)
        # Exigimos ao menos DUAS odds > 1.0 (p.ex. casa e fora)
        return len(vals) >= 2
    return int(df.apply(valid, axis=1).sum())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--require", action="store_true",
                    help="Se não houver odds válidas, sai com código 10 (fail-fast).")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    path = os.path.join(out_dir_for(args.rodada), "odds_consensus.csv")
    if not os.path.exists(path):
        msg = f"[guard] odds_consensus.csv não encontrado em {path}"
        if args.require:
            print(f"{msg} — falhando.", file=sys.stderr)
            sys.exit(10)
        else:
            print(f"{msg} — ok (no-op).")
            sys.exit(0)

    try:
        df = pd.read_csv(path)
    except Exception as e:
        if args.require:
            print(f"[guard] falha ao ler odds_consensus.csv: {e}", file=sys.stderr)
            sys.exit(10)
        else:
            print(f"[guard] aviso: não consegui ler odds_consensus.csv: {e}")
            sys.exit(0)

    if args.debug:
        print(f"[guard] consensus lido: {len(df)} linhas")

    df = normalize_cols(df)
    valid = count_valid_rows(df)

    if args.debug:
        print(f"[guard] linhas com >=2 odds > 1.0: {valid}")

    if valid <= 0:
        msg = "[guard] ERRO: nenhuma linha de odds válida (>= 2 colunas odds_* > 1.0)."
        if args.require:
            print(msg, file=sys.stderr)
            sys.exit(10)
        else:
            print(msg.replace("ERRO","AVISO"))
            sys.exit(0)

    print(f"[guard] OK: {valid} linhas com odds reais.")
    sys.exit(0)

if __name__ == "__main__":
    main()