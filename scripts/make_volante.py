#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_volante.py — Gera "volante" da Loteca em Markdown, com caixinhas [1][X][2] marcadas corretamente.
- Lê matches.csv e reports/context_scores_<rodada>.csv
- Palpite base: maior probabilidade; força X se p_draw>=0.33
- Duplos nos jogos mais incertos (menor margem), triplos se solicitado
- Alinha com data/raw/loteca_concurso_<id>.csv (slot,home,away) se existir
Saídas:
  reports/volante_<rodada>.md
  reports/volante_concurso_<id>.md
"""

import argparse, re
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

FORCE_DRAW_THRESHOLD = 0.33

def load_cfg():
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def norm(s):
    if pd.isna(s): return ""
    s = str(s).lower()
    s = re.sub(r"[^a-z0-9áàâãéêíóôõúüç\s-]", "", s)
    s = s.replace(" fc","").replace(" afc","").replace(" ac","").replace(" ec","")
    s = re.sub(r"\s+"," ", s).strip()
    return s

def base_pick(ph, pd, pa):
    arr = np.array([ph,pd,pa], float)
    idx = int(np.nanargmax(arr))
    pick = ["1","X","2"][idx]
    if pd >= FORCE_DRAW_THRESHOLD:
        pick = "X"
        idx = 1
    return pick, idx

def duplo_from_probs(arr):
    top2 = np.argsort(arr)[-2:]
    pair = tuple(sorted(list(top2)))
    return { (0,1):"1X", (1,2):"X2", (0,2):"12" }.get(pair, "1X")

def render_md(rows, title):
    md=[]
    md.append(f"# {title}\n")
    md.append("> Formato tipo volante da Loteca: cada linha traz as caixas [1] [X] [2] marcadas; duplos/triplos ao lado.\n")
    md.append("| Nº | Jogo | 1 | X | 2 | Palpite | Duplo | Triplo | p_home | p_draw | p_away |")
    md.append("|:-:|:-----|:-:|:-:|:-:|:------:|:----:|:-----:|-----:|------:|------:|")
    for r in rows:
        # marcação fiel: começa vazio e marca conforme pick/duplo/triplo
        m1=mX=m2=""
        if r["pick"]=="1": m1="X"
        if r["pick"]=="X": mX="X"
        if r["pick"]=="2": m2="X"
        d = r.get("duplo","")
        if "1" in d: m1="X"
        if "X" in d: mX="X"
        if "2" in d: m2="X"
        if r.get("triplo","")=="1X2":
            m1=mX=m2="X"
        md.append(f"| {r['slot']:>2} | {r['home']} x {r['away']} | {m1:^1} | {mX:^1} | {m2:^1} | **{r['pick']}** | {d} | {r.get('triplo','')} | {r['ph']:.2f} | {r['pd']:.2f} | {r['pa']:.2f} |")
    md.append("\n_Obs.: probabilidades vêm do pipeline (odds de-vig + contexto)._")
    return "\n".join(md)

def build_rows(matches, scores, duplos=4, triplos=0, concurso_id=""):
    df = matches.merge(scores[["match_id","p_home","p_draw","p_away"]], on="match_id", how="left")
    df[["p_home","p_draw","p_away"]] = df[["p_home","p_draw","p_away"]].astype(float).fillna(0.0)

    # palpite base + margem
    picks=[]; margins=[]; probs=df[["p_home","p_draw","p_away"]].to_numpy(float)
    for i,row in df.iterrows():
        ph,pd,pa = float(row["p_home"]),float(row["p_draw"]),float(row["p_away"])
        pk,_ = base_pick(ph,pd,pa)
        picks.append(pk)
        s = sorted([ph,pd,pa], reverse=True)
        margins.append(s[0]-s[1])
    df["pick"]=picks; df["margin"]=margins

    # duplos e triplos
    order = np.argsort(df["margin"].to_numpy())  # menor margem = mais incerto
    d_idx = list(order[:duplos]) if duplos>0 else []
    t_idx = list(order[duplos:duplos+triplos]) if triplos>0 else []
    df["duplo"]=""; df["triplo"]=""
    for i in d_idx:
        df.loc[df.index[i],"duplo"] = duplo_from_probs(probs[i,:])
    for i in t_idx:
        df.loc[df.index[i],"triplo"] = "1X2"

    # alinhamento com concurso
    rows=[]
    align = Path(f"data/raw/loteca_concurso_{concurso_id}.csv") if concurso_id else None
    if align and align.exists():
        lot = pd.read_csv(align)
        lot = lot.sort_values("slot")
        df["home_n"]=df["home"].map(norm); df["away_n"]=df["away"].map(norm)
        for _,r in lot.iterrows():
            h,a = norm(r["home"]), norm(r["away"])
            cand = df[(df["home_n"]==h)&(df["away_n"]==a)]
            if cand.empty: cand = df[(df["home_n"]==a)&(df["away_n"]==h)]
            if cand.empty:
                rows.append({"slot":int(r["slot"]), "home":r["home"], "away":r["away"],
                             "ph":0.0,"pd":0.0,"pa":0.0,"pick":"-","duplo":"","triplo":""})
            else:
                x=cand.iloc[0]
                rows.append({"slot":int(r["slot"]), "home":x["home"], "away":x["away"],
                             "ph":float(x["p_home"]),"pd":float(x["p_draw"]),"pa":float(x["p_away"]),
                             "pick":x["pick"],"duplo":x["duplo"],"triplo":x["triplo"]})
    else:
        # sem arquivo do concurso: usa a ordem do matches
        for i,row in enumerate(df.itertuples(), start=1):
            rows.append({"slot":i, "home":row.home, "away":row.away,
                         "ph":float(row.p_home),"pd":float(row.p_draw),"pa":float(row.p_away),
                         "pick":row.pick,"duplo":row.duplo,"triplo":row.triplo})
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--duplos", type=int, default=4)
    ap.add_argument("--triplos", type=int, default=0)
    ap.add_argument("--concurso", type=str, default="")
    args = ap.parse_args()

    C = load_cfg(); paths=C["paths"]
    mpath = paths["matches_csv"].replace("${rodada}", args.rodada)
    spath = paths["context_score_out"].replace("${rodada}", args.rodada)
    if not Path(mpath).exists(): print(f"[ERRO] matches não encontrado: {mpath}"); return
    if not Path(spath).exists(): print(f"[ERRO] context_scores não encontrado: {spath}"); return

    matches = pd.read_csv(mpath)
    scores  = pd.read_csv(spath)

    rows = build_rows(matches, scores, args.duplos, args.triplos, args.concurso)

    Path("reports").mkdir(parents=True, exist_ok=True)
    outR = f"reports/volante_{args.rodada}.md"
    with open(outR,"w",encoding="utf-8") as f:
        f.write(render_md(rows, f"Volante — Rodada {args.rodada}"))
    print(f"[OK] Volante (rodada) → {outR}")

    if args.concurso:
        outC = f"reports/volante_concurso_{args.concurso}.md"
        with open(outC,"w",encoding="utf-8") as f:
            f.write(render_md(rows, f"Volante — Concurso {args.concurso}"))
        print(f"[OK] Volante (concurso) → {outC}")

if __name__ == "__main__":
    main()
