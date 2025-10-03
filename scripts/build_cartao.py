#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, argparse
import pandas as pd
import numpy as np

"""
Gera o cartão da Loteca (14 jogos) a partir da ordem de data/in/<RODADA>/matches_source.csv.
Fonte de palpites (em ordem de preferência):
1) data/out/<RODADA>/predictions_market.csv
2) fallback: decide pelo menor odd em data/out/<RODADA>/odds_consensus.csv
Saída: data/out/<RODADA>/loteca_cartao.txt
Formato:
  Jogo 01 - Vitoria x Ceara -> 1 (HOME) [conf=0.62]
  ...
E também uma linha compacta 14 colunas: ex: 1 X 2 1 1 X ...
"""

def pick_to_1x2(p):
    if p == "HOME": return "1"
    if p == "DRAW": return "X"
    if p == "AWAY": return "2"
    return "?"

def best_from_odds(row):
    # menor odd = favorito
    trio = []
    for tag,col in [("HOME","odds_home"),("DRAW","odds_draw"),("AWAY","odds_away")]:
        o = row.get(col, np.nan)
        if isinstance(o,(int,float,np.floating)) and o>1.0 and np.isfinite(o):
            trio.append((tag,o))
    if not trio:
        return "?", np.nan
    pick, odd = min(trio, key=lambda x: x[1])
    return pick, float(1.0/odd) if odd>0 else np.nan  # confiança ~ prob implícita aprox.

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    in_matches = os.path.join("data","in",args.rodada,"matches_source.csv")
    out_dir    = os.path.join("data","out",args.rodada)
    pred_path  = os.path.join(out_dir,"predictions_market.csv")
    cons_path  = os.path.join(out_dir,"odds_consensus.csv")
    out_path   = os.path.join(out_dir,"loteca_cartao.txt")

    if not os.path.exists(in_matches):
        raise SystemExit(f"[cartao] ERRO: arquivo não encontrado: {in_matches}")
    df_m = pd.read_csv(in_matches)
    for c in ["match_key","team_home","team_away"]:
        if c not in df_m.columns:
            raise SystemExit(f"[cartao] ERRO: coluna ausente em matches_source.csv: {c}")

    df_pred = pd.read_csv(pred_path) if os.path.exists(pred_path) else None
    df_cons = pd.read_csv(cons_path) if os.path.exists(cons_path) else None

    # index por match_key para acesso rápido
    pred_map = {}
    if df_pred is not None and not df_pred.empty:
        pred_map = {r["match_key"]: r for _,r in df_pred.iterrows()}

    cons_map = {}
    if df_cons is not None and not df_cons.empty:
        cons_map = {r["match_key"]: r for _,r in df_cons.iterrows()}

    lines = []
    picks_compact = []
    jogo_idx = 1

    for _, m in df_m.iterrows():
        mk = m["match_key"]
        th = m["team_home"]
        ta = m["team_away"]

        pick, conf = None, None
        origem = None

        if mk in pred_map:
            r = pred_map[mk]
            pick = r.get("pick", None)
            conf = r.get("confidence", None)
            origem = "predictions_market"
        elif mk in cons_map:
            r = cons_map[mk]
            pick, conf = best_from_odds(r)
            origem = "odds_consensus"
        else:
            pick = "?"
            conf = np.nan
            origem = "indisponível"

        simb = pick_to_1x2(pick)
        picks_compact.append(simb)

        lines.append(f"Jogo {jogo_idx:02d} - {th} x {ta} -> {simb} ({pick}) [conf={conf if conf==conf else 'NA'} | src={origem}]")
        jogo_idx += 1

    os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for ln in lines:
            f.write(ln+"\n")
        f.write("\nLinha 14 palpites (1/X/2):\n")
        f.write(" ".join(picks_compact)+"\n")

    print(f"[cartao] OK -> {out_path}")

    if args.debug:
        print("[cartao] AMOSTRA:")
        for ln in lines[:5]:
            print("  "+ln)

if __name__ == "__main__":
    main()
