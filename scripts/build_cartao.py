#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera o cartão Loteca após TODAS as etapas (consenso, predição, Kelly).
Usa:
  - {OUT_DIR}/predictions_market.csv   (para 1/X/2)
  - {OUT_DIR}/kelly_stakes.csv         (para marcar picks mais fortes) [opcional]

Saída:
  - {OUT_DIR}/loteca_cartao.txt
"""

import os
import sys
import argparse
import pandas as pd

def die(msg: str):
    print(f"[cartao] ERRO: {msg}", file=sys.stderr)
    sys.exit(2)

def to_1x2(pred: str) -> str:
    return {"HOME":"1","DRAW":"X","AWAY":"2"}.get(str(pred).upper(), "?")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="OUT_DIR")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    pred_p = os.path.join(out_dir, "predictions_market.csv")
    if not os.path.isfile(pred_p):
        die("predictions_market.csv ausente ou vazio.")

    df = pd.read_csv(pred_p)
    if df.empty:
        die("predictions_market.csv vazio.")
    # ordena alfabeticamente pelo match_key para estabilidade
    df = df.sort_values(by="match_key").reset_index(drop=True)

    # tenta ler Kelly para marcar picks com stake > 0
    picks = {}
    ks_path = os.path.join(out_dir, "kelly_stakes.csv")
    if os.path.isfile(ks_path):
        dfk = pd.read_csv(ks_path)
        for _, r in dfk.iterrows():
            if float(r.get("stake",0)) > 0:
                picks[str(r["match_key"]).strip()] = True

    lines = []
    lines.append("CARTÃO LOTECA (gerado pelo framework)")
    lines.append("-"*40)
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        mk = str(r["match_key"]).strip()
        left = f"{r['team_home']} x {r['team_away']}"
        tip = to_1x2(r["pred"])
        star = " *" if picks.get(mk) else ""
        lines.append(f"{i:02d}) {left:<40}  {tip}{star}")

    outp = os.path.join(out_dir, "loteca_cartao.txt")
    with open(outp, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"[cartao] OK -> {outp}")

if __name__ == "__main__":
    main()