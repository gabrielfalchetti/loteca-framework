# -*- coding: utf-8 -*-
"""
Gera o cartão da Loteca a partir de predictions_market.csv
Saída: data/out/<rodada>/loteca_cartao.txt
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ex: 2025-09-27_1213")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = Path(f"data/out/{args.rodada}")
    pred_path = out_dir/"predictions_market.csv"
    if not pred_path.exists() or pred_path.stat().st_size == 0:
        raise SystemExit("[cartao] ERRO: predictions_market.csv ausente ou vazio.")

    df = pd.read_csv(pred_path)
    # ordena por match_key para manter previsibilidade de numeração
    df = df.sort_values("match_key").reset_index(drop=True)

    lines = []
    lines.append(f"LOTECA – CARTÃO ({args.rodada})")
    lines.append("="*40)
    for i, r in df.iterrows():
        jogo = i+1
        duelo = f"{r['team_home']} x {r['team_away']}"
        palpite = r["pred"]
        conf = f"{r['pred_conf']:.1%}"
        lines.append(f"{jogo:02d}. {duelo} -> {palpite} ({conf})")

    out = "\n".join(lines) + "\n"
    (out_dir/"loteca_cartao.txt").write_text(out, encoding="utf-8")
    print(f"[cartao] OK -> {out_dir/'loteca_cartao.txt'}")

if __name__ == "__main__":
    main()