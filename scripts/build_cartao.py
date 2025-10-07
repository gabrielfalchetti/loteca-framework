#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_cartao.py
---------------
Gera o cartão final da Loteca com picks e stakes.

Entrada: kelly_stakes.csv
Saída: loteca_cartao.txt
"""

import argparse, os, pandas as pd

def resolve_out_dir(r): 
    if os.path.isdir(r): return r
    p=os.path.join("data","out",str(r)); os.makedirs(p,exist_ok=True); return p

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--rodada",required=True)
    args=ap.parse_args()
    out_dir=resolve_out_dir(args.rodada)

    fp=os.path.join(out_dir,"kelly_stakes.csv")
    if not os.path.isfile(fp): raise FileNotFoundError(fp)
    df=pd.read_csv(fp)

    lines=["==== CARTÃO LOTECA ===="]
    for i,(idx,r) in enumerate(df.iterrows(),1):
        lines.append(f"Jogo {i:02d} - {r.team_home} x {r.team_away}: {r.pick[0]} (stake={r.stake}) [{round(r.prob*100,2) if pd.notna(r.prob) else 'nan'}%]")
    lines.append("=======================")

    txt="\n".join(lines)
    out=os.path.join(out_dir,"loteca_cartao.txt")
    with open(out,"w",encoding="utf-8") as f: f.write(txt)
    print(txt)

if __name__=="__main__": main()