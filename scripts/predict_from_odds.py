#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, math
import pandas as pd

def implied_probs_no_overround(row):
    # Odds válidas (>1.0); pode faltar alguma (NaN)
    o_home = row.get("odds_home", float("nan"))
    o_draw = row.get("odds_draw", float("nan"))
    o_away = row.get("odds_away", float("nan"))

    invs = []
    for o in (o_home, o_draw, o_away):
        invs.append((1.0 / o) if (isinstance(o, (int, float)) and o and o > 1.0) else 0.0)

    s = sum(invs)
    if s <= 0:
        return 0.0, 0.0, 0.0  # linha inválida

    # normaliza para remover o overround
    p_home = invs[0] / s
    p_draw = invs[1] / s
    p_away = invs[2] / s
    return p_home, p_draw, p_away

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    in_path = os.path.join(out_dir, "odds_consensus.csv")
    out_path = os.path.join(out_dir, "predictions_market.csv")

    if not os.path.exists(in_path):
        raise SystemExit(f"[predict] ERRO: arquivo não encontrado: {in_path}")

    df = pd.read_csv(in_path)
    required = ["match_key", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"[predict] ERRO: colunas ausentes em odds_consensus.csv: {missing}")

    probs_home, probs_draw, probs_away, picks, confs = [], [], [], [], []

    for _, row in df.iterrows():
        ph, pdw, pa = implied_probs_no_overround(row)
        probs_home.append(ph)
        probs_draw.append(pdw)
        probs_away.append(pa)
        triple = [("HOME", ph), ("DRAW", pdw), ("AWAY", pa)]
        pick, conf = max(triple, key=lambda x: x[1])
        picks.append(pick)
        confs.append(conf)

    df_out = df.copy()
    df_out["prob_home"] = probs_home
    df_out["prob_draw"] = probs_draw
    df_out["prob_away"] = probs_away
    df_out["pick"] = picks
    df_out["confidence"] = confs

    # ordena por confiança desc
    df_out = df_out.sort_values(by="confidence", ascending=False)

    os.makedirs(out_dir, exist_ok=True)
    df_out.to_csv(out_path, index=False, float_format="%.6f")

    if args.debug:
        amostra = df_out.head(5).to_dict(orient="records")
        print(f"[predict] AMOSTRA (top 5): {amostra}")

    print(f"[predict] OK -> {out_path} ({len(df_out)} linhas)")

if __name__ == "__main__":
    main()
