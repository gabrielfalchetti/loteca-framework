#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_volante.py — Gera a saída FINAL em formato "volante da Loteca" (Markdown), sem CSV.
- Lê matches.csv e reports/context_scores_<rodada>.csv do pipeline.
- Gera palpites "secos" por maior probabilidade, força X quando p_draw>=0.33 (ajustável).
- Aloca duplos nos jogos mais incertos (menor margem entre 1º e 2º), e triplos se solicitado.
- Opcional: alinha a ordem dos 14 jogos com data/raw/loteca_concurso_<id>.csv (slot,home,away).
- Salva:
    reports/volante_<rodada>.md
    reports/volante_concurso_<id>.md  (se alinhamento existir)
Uso:
  python pipeline/scripts/make_volante.py --rodada "2025-09-20_21" --duplos 4 --triplos 0 --concurso 1213
"""

import argparse, os, sys, re
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

FORCE_DRAW_THRESHOLD = 0.33  # força "X" quando p_draw >= 0.33 (clássicos/derbies)

def load_cfg():
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def norm(s: str) -> str:
    if pd.isna(s): return ""
    s = str(s).lower()
    s = re.sub(r"[^a-z0-9áàâãéêíóôõúüç\s-]", "", s)
    s = s.replace(" fc","").replace(" afc","").replace(" ac","").replace(" ec","")
    s = re.sub(r"\s+"," ", s).strip()
    return s

def pick_from_probs(ph, pd, pa):
    # base: maior probabilidade
    arr = np.array([ph, pd, pa], dtype=float)
    idx = int(np.nanargmax(arr))
    base = ["1","X","2"][idx]
    # força X se muito alto
    if pd >= FORCE_DRAW_THRESHOLD:
        base = "X"
    return base, idx

def duplo_from_top2(arr):
    top2 = np.argsort(arr)[-2:]  # índices das duas maiores
    pair = tuple(sorted(list(top2)))
    if pair==(0,1): return "1X"
    if pair==(1,2): return "X2"
    if pair==(0,2): return "12"
    return "1X"

def render_volante_md(rows, titulo: str) -> str:
    """rows: lista de dicts com keys: slot, home, away, ph, pd, pa, pick, duplo, triplo"""
    # Monta cabeçalho e legenda
    md = []
    md.append(f"# {titulo}")
    md.append("")
    md.append("> Formato tipo volante da Loteca: cada linha traz as caixas [1] [X] [2] com marcação do palpite; duplos/triplos vêm ao lado.")
    md.append("")
    # tabela simples para leitura rápida
    md.append("| Nº | Jogo | 1 | X | 2 | Palpite | Duplo | Triplo | p_home | p_draw | p_away |")
    md.append("|:-:|:-----|:-:|:-:|:-:|:------:|:----:|:-----:|-----:|------:|------:|")
    for r in rows:
        mark1 = "X" if r["pick"] in ("1","1X","12","1X2") else ("X" if r.get("duplo","") in ("1X","12") else ("X" if r.get("triplo","")== "1X2" else ""))
        markx = "X" if r["pick"] in ("X","1X","X2","1X2") else ("X" if r.get("duplo","") in ("1X","X2") else ("X" if r.get("triplo","")== "1X2" else ""))
        mark2 = "X" if r["pick"] in ("2","12","X2","1X2") else ("X" if r.get("duplo","") in ("12","X2") else ("X" if r.get("triplo","")== "1X2" else ""))

        # Ajuste melhor das marcações: derive das strings duplo/triplo
        # Simples: se duplo contém '1' -> marca 1; se contém 'X' -> marca X; se contém '2' -> marca 2
        m1 = "X" if ("1" in r.get("duplo","") or r["pick"]=="1" or r.get("triplo","")=="1X2") else ""
        mx = "X" if ("X" in r.get("duplo","") or r["pick"]=="X" or r.get("triplo","")=="1X2") else ""
        m2 = "X" if ("2" in r.get("duplo","") or r["pick"]=="2" or r.get("triplo","")=="1X2") else ""

        md.append(f"| {r['slot']:>2} | {r['home']} x {r['away']} | {m1:^1} | {mx:^1} | {m2:^1} | **{r['pick']}** | {r.get('duplo','')} | {r.get('triplo','')} | {r['ph']:.2f} | {r['pd']:.2f} | {r['pa']:.2f} |")

    md.append("")
    md.append("_Observação:_ probabilidades são derivadas do seu pipeline (odds de-vig + contexto).")
    return "\n".join(md)

def build_rows(matches, scores, duplos=4, triplos=0, concurso_id=""):
    df = matches.merge(scores[["match_id","p_home","p_draw","p_away"]], on="match_id", how="left")
    df[["p_home","p_draw","p_away"]] = df[["p_home","p_draw","p_away"]].astype(float).fillna(0.0)

    # cálculo do palpite base e margem
    picks = []
    margins = []
    topidx = []
    arrP = df[["p_home","p_draw","p_away"]].to_numpy(dtype=float)
    for i,row in df.iterrows():
        ph, pd, pa = float(row["p_home"]), float(row["p_draw"]), float(row["p_away"])
        pick, idx = pick_from_probs(ph, pd, pa)
        picks.append(pick)
        topidx.append(idx)
        sorted_desc = np.sort([ph,pd,pa])[::-1]
        margin = sorted_desc[0] - sorted_desc[1]
        margins.append(margin)

    df["pick"] = picks
    df["margin"] = margins

    # alocar duplos/triplos
    order_uncertain = np.argsort(df["margin"].to_numpy())  # mais incertos primeiro
    dupl_idx = list(order_uncertain[:duplos]) if duplos>0 else []
    trip_idx = list(order_uncertain[duplos:duplos+triplos]) if triplos>0 else []

    df["duplo"] = ""
    for i in dupl_idx:
        arr = arrP[i,:]
        df.loc[df.index[i], "duplo"] = duplo_from_top2(arr)

    df["triplo"] = ""
    for i in trip_idx:
        df.loc[df.index[i], "triplo"] = "1X2"

    # alinhar à ordem do concurso, se existir CSV
    rows=[]
    if concurso_id:
        align_path = Path(f"data/raw/loteca_concurso_{concurso_id}.csv")
    else:
        align_path = Path("__no__")

    if align_path.exists():
        lot = pd.read_csv(align_path)
        need = {"slot","home","away"}
        if not need.issuperset(set()):
            pass
        out=[]
        df["home_n"] = df["home"].map(norm); df["away_n"] = df["away"].map(norm)
        for _,r in lot.sort_values("slot").iterrows():
            h,a = norm(r["home"]), norm(r["away"])
            cand = df[(df["home_n"]==h) & (df["away_n"]==a)]
            if cand.empty:
                cand = df[(df["home_n"]==a) & (df["away_n"]==h)]  # fallback invertido
            if cand.empty:
                out.append({"slot":int(r["slot"]), "home":r["home"], "away":r["away"],
                            "ph":0.0,"pd":0.0,"pa":0.0,"pick":"-", "duplo":"","triplo":""})
            else:
                row=cand.iloc[0]
                out.append({"slot":int(r["slot"]), "home":row["home"], "away":row["away"],
                            "ph":float(row["p_home"]), "pd":float(row["p_draw"]), "pa":float(row["p_away"]),
                            "pick":row["pick"], "duplo":row["duplo"], "triplo":row["triplo"]})
        rows = out
    else:
        # usa ordem do matches.csv (1..N)
        for i,row in enumerate(df.itertuples(), start=1):
            rows.append({"slot":i, "home":row.home, "away":row.away,
                         "ph":float(row.p_home), "pd":float(row.p_draw), "pa":float(row.p_away),
                         "pick":row.pick, "duplo":row.duplo, "triplo":row.triplo})
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--duplos", type=int, default=4)
    ap.add_argument("--triplos", type=int, default=0)
    ap.add_argument("--concurso", type=str, default="")
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
    scores  = pd.read_csv(scores_path)

    rows = build_rows(matches, scores, args.duplos, args.triplos, args.concurso)

    Path("reports").mkdir(parents=True, exist_ok=True)
    # salva MD por rodada
    md_rodada = render_volante_md(rows, f"Volante — Rodada {args.rodada}")
    out_rodada = f"reports/volante_{args.rodada}.md"
    with open(out_rodada, "w", encoding="utf-8") as f:
        f.write(md_rodada)
    print(f"[OK] Volante (rodada) → {out_rodada}")

    # se concurso foi informado, gera arquivo com nome do concurso também
    if args.concurso:
        md_conc = render_volante_md(rows, f"Volante — Concurso {args.concurso}")
        out_conc = f"reports/volante_concurso_{args.concurso}.md"
        with open(out_conc, "w", encoding="utf-8") as f:
            f.write(md_conc)
        print(f"[OK] Volante (concurso) → {out_conc}")

if __name__ == "__main__":
    main()
