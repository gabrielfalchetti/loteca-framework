#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera recomendação de seco/duplo/triplo para Loteca a partir das probabilidades finais,
priorizando duplos/triplos nos jogos mais incertos (maior entropia).

Entrada (em data/out/<rodada>/), usa na ordem de preferência:
- probabilities_calibrated.csv
- probabilities_blended.csv
- probabilities.csv

Saídas:
- loteca_card.csv       (match_id, home, away, pick, p_sucesso_jogo, entropy, detalhes)
- loteca_summary.json   (contagem por tipo, probabilidade de acertar o volante)

Exemplo:
  python scripts/loteca_picker.py --rodada 2025-09-27_1213 --duplos 4 --triplos 2
"""

from __future__ import annotations
import argparse
import os
import sys
import json
import math
import pandas as pd
import numpy as np

PREFS = ["probabilities_calibrated.csv","probabilities_blended.csv","probabilities.csv"]

def _load_probs(out_dir: str) -> pd.DataFrame:
    path = None
    for p in PREFS:
        test = os.path.join(out_dir, p)
        if os.path.exists(test):
            path = test
            break
    if path is None:
        raise FileNotFoundError("Nenhum probabilities_* encontrado.")
    df = pd.read_csv(path)
    lower = {c: c.lower() for c in df.columns}
    df.rename(columns=lower, inplace=True)
    need = {"match_id","p1","px","p2"}
    if not need.issubset(df.columns):
        raise ValueError(f"{path} sem colunas necessárias: {need - set(df.columns)}")
    # tentar nomes dos times, se existirem
    if "home" not in df.columns:
        df["home"] = None
    if "away" not in df.columns:
        df["away"] = None
    return df, os.path.basename(path)

def _entropy_row(p1, px, p2):
    eps = 1e-12
    arr = np.array([p1,px,p2], dtype=float)
    arr = np.clip(arr, eps, 1.0)
    arr = arr / arr.sum()
    return -float(np.sum(arr * np.log(arr)))

def main():
    ap = argparse.ArgumentParser(description="Decisor Loteca: seco/duplo/triplo + probabilidade do volante.")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--duplos", type=int, default=4)
    ap.add_argument("--triplos", type=int, default=2)
    args = ap.parse_args()

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    df, used = _load_probs(out_dir)

    # calcula entropia e ranking de incerteza
    df["entropy"] = df.apply(lambda r: _entropy_row(r["p1"], r["px"], r["p2"]), axis=1)
    df = df.sort_values("entropy", ascending=False).reset_index(drop=True)

    # alocação: top 'triplos' recebem 1X2; próximos 'duplos' recebem os dois maiores; resto seco.
    picks = []
    for i, r in df.iterrows():
        p = [("1", float(r["p1"])), ("X", float(r["px"])), ("2", float(r["p2"]))]
        p_sorted = sorted(p, key=lambda t: t[1], reverse=True)

        if i < args.triplos:
            choice = "1X2"
            p_sucesso = sum(v for _,v in p)
            detalhe = "triplo (maior incerteza)"
        elif i < args.triplos + args.duplos:
            choice = "".join([p_sorted[0][0], p_sorted[1][0]])
            p_sucesso = p_sorted[0][1] + p_sorted[1][1]
            detalhe = "duplo (incerteza intermediária)"
        else:
            choice = p_sorted[0][0]
            p_sucesso = p_sorted[0][1]
            detalhe = "seco (mais previsível)"

        picks.append({
            "match_id": r["match_id"],
            "home": r.get("home", None),
            "away": r.get("away", None),
            "pick": choice,
            "p_sucesso_jogo": p_sucesso,
            "entropy": r["entropy"],
            "detalhes": detalhe
        })

    card = pd.DataFrame(picks)
    card_path = os.path.join(out_dir, "loteca_card.csv")
    card.to_csv(card_path, index=False, encoding="utf-8")

    # probabilidade total do volante = produto das p_sucesso_jogo
    p_total = float(np.prod(card["p_sucesso_jogo"].to_numpy()))
    summary = {
        "rodada": args.rodada,
        "usou_arquivo_probs": used,
        "jogos": int(len(card)),
        "triplos": int(min(args.triplos, len(card))),
        "duplos": int(min(args.duplos, max(0, len(card)-args.triplos))),
        "secos": int(max(0, len(card) - (args.triplos + args.duplos))),
        "prob_sucesso_bilhete": p_total,  # chance de acertar todos
    }
    with open(os.path.join(out_dir, "loteca_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[loteca] OK -> {card_path} ({len(card)} jogos). Prob.(acertar todos) = {p_total:.6f}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[loteca] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
