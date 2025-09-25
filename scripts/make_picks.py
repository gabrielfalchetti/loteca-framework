#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_picks.py — gera palpites 1/X/2 (base) e versão com duplos/triplos a partir das probabilidades do pipeline.
Saídas:
  - reports/picks_<rodada>_base.csv
  - reports/picks_<rodada>_duplos.csv
  - (opcional) reports/picks_concurso_<id>.csv se você fornecer data/raw/loteca_concurso_<id>.csv
Uso:
  python pipeline/scripts/make_picks.py --rodada "2025-09-20_21" --duplos 4 --triplos 0 --concurso 1213
"""

import argparse, os, sys
from pathlib import Path
import pandas as pd
import numpy as np
import yaml
import re

def load_cfg():
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def norm(s):
    if pd.isna(s): return ""
    s = str(s).lower()
    s = re.sub(r"[^a-z0-9áàâãéêíóôõúüç\s-]", "", s)
    s = s.replace(" fc","").replace(" afc","").replace(" ac","").replace(" ec","")
    s = re.sub(r"\s+"," ", s).strip()
    return s

def label_from_argmax(i):
    return {0:"1", 1:"X", 2:"2"}[int(i)]

def duplo_label_from_top2(idx0, idx1):
    pair = tuple(sorted([idx0, idx1]))
    # (0=home, 1=draw, 2=away) -> rotular par
    if pair==(0,1): return "1X"
    if pair==(1,2): return "X2"
    if pair==(0,2): return "12"
    return "1X"  # fallback

def compute_base_and_uncertainty(df):
    """Retorna picks base e incerteza por jogo"""
    P = df[["p_home","p_draw","p_away"]].astype(float).fillna(0.0).to_numpy()
    argmax = P.argmax(axis=1)
    pick = [label_from_argmax(i) for i in argmax]
    # margem = melhor - segundo melhor (quanto menor, mais incerto)
    top = np.sort(P, axis=1)[:, ::-1]
    margin = top[:,0] - top[:,1]
    # reforço opcional de empate: se p_draw >= 0.33, força "X"
    force_x = (P[:,1] >= 0.33)
    pick = [("X" if force_x[i] else pick[i]) for i in range(len(pick))]
    return pick, margin, argmax, P

def select_duplos_triplos(df, margin, argmax, P, n_duplos=4, n_triplos=0):
    idx_sorted = np.argsort(margin)  # incertos primeiro
    duplos_idx = list(idx_sorted[:n_duplos]) if n_duplos>0 else []
    triplos_idx = list(idx_sorted[n_duplos:n_duplos+n_triplos]) if n_triplos>0 else []

    duplo_col = [""]*len(df)
    triplo_col = [""]*len(df)

    for i in duplos_idx:
        row = P[i,:]
        top2 = np.argsort(row)[-2:]  # duas maiores
        duplo_col[i] = duplo_label_from_top2(top2[0], top2[1])

    for i in triplos_idx:
        triplo_col[i] = "1X2"

    return duplo_col, triplo_col

def align_to_concurso(df_picks, concurso_id):
    """Se existir data/raw/loteca_concurso_<id>.csv com slot,home,away, alinha as picks nessa ordem."""
    path = Path(f"data/raw/loteca_concurso_{concurso_id}.csv")
    if not path.exists():
        print(f"[WARN] Arquivo de alinhamento não encontrado: {path} (pulando).")
        return None
    lot = pd.read_csv(path)
    need_cols = {"slot","home","away"}
    if not need_cols.issubset(lot.columns):
        print(f"[WARN] {path} precisa das colunas: slot,home,away")
        return None

    df = df_picks.copy()
    df["home_n"] = df["home"].map(norm); df["away_n"] = df["away"].map(norm)

    out_rows=[]
    for _,r in lot.sort_values("slot").iterrows():
        h,a = norm(r["home"]), norm(r["away"])
        # tenta match direto (home/away na mesma ordem)
        cand = df[(df["home_n"]==h) & (df["away_n"]==a)]
        if cand.empty:
            # tenta invertido (quando o mandante não bate)
            cand = df[(df["home_n"]==a) & (df["away_n"]==h)]
        if cand.empty:
            out_rows.append({"slot":int(r["slot"]),"home":r["home"],"away":r["away"],"pick":"(NÃO ENCONTRADO)"})
        else:
            row = cand.iloc[0]
            out_rows.append({"slot":int(r["slot"]), "match_id":row["match_id"], "home":row["home"], "away":row["away"],
                             "p_home":row["p_home"], "p_draw":row["p_draw"], "p_away":row["p_away"],
                             "pick":row["pick"], "duplo":row.get("duplo",""), "triplo":row.get("triplo","")})
    return pd.DataFrame(out_rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--duplos", type=int, default=4, help="quantidade de duplos a alocar")
    ap.add_argument("--triplos", type=int, default=0, help="quantidade de triplos a alocar")
    ap.add_argument("--concurso", type=str, default="", help="ex.: 1213 (gera arquivo alinhado se CSV existir)")
    args = ap.parse_args()

    C = load_cfg()
    paths = C["paths"]

    matches_path = paths["matches_csv"].replace("${rodada}", args.rodada)
    scores_path  = paths["context_score_out"].replace("${rodada}", args.rodada)

    if not Path(matches_path).exists():
        print(f"[ERRO] matches não encontrado: {matches_path}"); sys.exit(2)
    if not Path(scores_path).exists():
        print(f"[ERRO] context_scores não encontrado: {scores_path}"); sys.exit(2)

    matches = pd.read_csv(matches_path)
    scores = pd.read_csv(scores_path)

    # garantir colunas necessárias
    for c in ["match_id","home","away"]: 
        if c not in matches.columns: 
            print(f"[ERRO] coluna {c} ausente em matches.csv"); sys.exit(2)
    for c in ["match_id","p_home","p_draw","p_away"]:
        if c not in scores.columns:
            print(f"[ERRO] coluna {c} ausente em context_scores.csv"); sys.exit(2)

    df = matches.merge(scores[["match_id","p_home","p_draw","p_away"]], on="match_id", how="left")
    df[["p_home","p_draw","p_away"]] = df[["p_home","p_draw","p_away"]].astype(float).fillna(0.0)

    # picks base + incerteza
    picks_base, margin, argmax, P = compute_base_and_uncertainty(df)
    df["pick"] = picks_base
    df["uncertainty_margin"] = margin

    # duplos/triplos
    duplo_col, triplo_col = select_duplos_triplos(df, margin, argmax, P, args.duplos, args.triplos)
    df["duplo"] = duplo_col
    df["triplo"] = triplo_col

    # salvar
    out_base = f"reports/picks_{args.rodada}_base.csv"
    out_duplos = f"reports/picks_{args.rodada}_duplos.csv"
    Path("reports").mkdir(parents=True, exist_ok=True)

    base_cols = ["match_id","home","away","p_home","p_draw","p_away","pick"]
    df[base_cols].to_csv(out_base, index=False)
    df[base_cols+["duplo","triplo","uncertainty_margin"]].to_csv(out_duplos, index=False)
    print(f"[OK] picks base → {out_base}")
    print(f"[OK] picks com duplos/triplos → {out_duplos}")

    if args.concurso:
        aligned = align_to_concurso(df[base_cols+["duplo","triplo"]], args.concurso)
        if aligned is not None:
            out_aligned = f"reports/picks_concurso_{args.concurso}.csv"
            aligned.to_csv(out_aligned, index=False)
            print(f"[OK] picks alinhado ao concurso {args.concurso} → {out_aligned}")
        else:
            print("[WARN] Não foi possível alinhar ao concurso (CSV ausente ou inválido).")

if __name__ == "__main__":
    main()
