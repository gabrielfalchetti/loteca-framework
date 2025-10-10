#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera o Cartão Loteca final com base nos stakes de Kelly.

Entrada obrigatória:
  {OUT_DIR}/kelly_stakes.csv   (com colunas: match_id, team_home, team_away, pick, stake)

Saída:
  {OUT_DIR}/cartao_loteca.csv  (ordenado por stake desc, limitado por KELLY_TOP_N se existir)
"""

import os
import sys
import argparse
import pandas as pd

EXIT_CODE = 31

def eprint(*a, **k):
    print(*a, file=sys.stderr, **k)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="Diretório da rodada (OUT_DIR)")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    out_dir = args.rodada
    path = os.path.join(out_dir, "kelly_stakes.csv")
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        eprint("::error::kelly_stakes.csv ausente/vazio.")
        sys.exit(EXIT_CODE)

    try:
        df = pd.read_csv(path)
    except Exception as ex:
        eprint(f"::error::Falha ao ler kelly_stakes.csv: {ex}")
        sys.exit(EXIT_CODE)

    need = {"match_id", "team_home", "team_away", "pick", "stake"}
    if not need.issubset(set(df.columns)):
        missing = sorted(list(need - set(df.columns)))
        eprint(f"::error::kelly_stakes.csv sem colunas mínimas {missing}")
        sys.exit(EXIT_CODE)

    # Ordena por stake desc e aplica TOP_N se existir
    top_n = None
    try:
        top_n = int(float(os.environ.get("KELLY_TOP_N", "0")))
    except Exception:
        top_n = 0

    out = df.sort_values(["stake"], ascending=False).reset_index(drop=True)
    if top_n and top_n > 0:
        out = out.head(top_n)

    # Normaliza pick textual para Loteca: H/D/A
    # Se vier 1x2 numérico, traduz. Mantém texto se já for 'H','D','A'.
    def norm_pick(v):
        s = str(v).strip().upper()
        if s in {"H","D","A"}:
            return s
        # números comuns: 1=H, 0=D, 2=A ou 3=A
        if s in {"1","HOME"}:
            return "H"
        if s in {"0","DRAW","X"}:
            return "D"
        if s in {"2","3","AWAY"}:
            return "A"
        return "H"  # fallback conservador

    out["pick_loteca"] = out["pick"].apply(norm_pick)

    out_cols = ["match_id", "team_home", "team_away", "pick_loteca", "stake"]
    out = out[out_cols].rename(columns={"pick_loteca": "pick"})

    out_csv = os.path.join(out_dir, "cartao_loteca.csv")
    out.to_csv(out_csv, index=False)

    if os.path.getsize(out_csv) == 0:
        eprint("::error::cartao_loteca.csv não gerado")
        sys.exit(EXIT_CODE)

    if args.debug:
        eprint(f"[loteca] OK gerado: {out_csv}  linhas={len(out)}")

if __name__ == "__main__":
    main()