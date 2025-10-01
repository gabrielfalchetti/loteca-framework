#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Valida que:
- Existe odds_consensus.csv com pelo menos 1 linha;
- Opcionalmente, que **todas** as partidas de matches_source.csv tenham odds.

Sai com código != 0 se falhar (fail fast no CI).
"""

import os
import sys
import argparse
import pandas as pd

def norm(s: str) -> str:
    return str(s).strip().lower()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--require_all", action="store_true", help="Exige odds para todos os matches_source")
    args = ap.parse_args()

    out_dir = os.path.join("data","out", args.rodada)
    in_dir  = os.path.join("data","in",  args.rodada)

    p_cons   = os.path.join(out_dir, "odds_consensus.csv")
    p_source = os.path.join(in_dir,  "matches_source.csv")

    if not os.path.exists(p_cons):
        print(f"[validate-odds] ERRO: arquivo não encontrado: {p_cons}")
        sys.exit(30)

    df = pd.read_csv(p_cons)
    if df.empty:
        print("[validate-odds] ERRO: odds_consensus.csv está vazio (0 linhas).")
        sys.exit(31)

    if args.require_all:
        if not os.path.exists(p_source):
            print(f"[validate-odds] ERRO: matches_source.csv não encontrado em {p_source}")
            sys.exit(32)
        ms = pd.read_csv(p_source)
        # chaves de jogos
        ms["__join_key"] = (ms["team_home"].astype(str).str.lower().str.strip()
                            + "__vs__" +
                            ms["team_away"].astype(str).str.lower().str.strip())
        df["__join_key"] = (df["team_home"].astype(str).str.lower().str.strip()
                            + "__vs__" +
                            df["team_away"].astype(str).str.lower().str.strip())
        have = set(df["__join_key"].dropna().tolist())
        need = list(ms["__join_key"].dropna().tolist())
        missing = [k for k in need if k not in have]
        if missing:
            print("[validate-odds] ERRO: faltam odds para os seguintes jogos:")
            for k in missing:
                th, ta = k.split("__vs__")
                print(f" - {th} x {ta}")
            sys.exit(33)

    print("[validate-odds] OK: odds presentes e válidas.")

if __name__ == "__main__":
    main()
