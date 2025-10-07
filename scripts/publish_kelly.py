#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
publish_kelly.py
----------------
Calcula frações de aposta segundo critério de Kelly fracionado.

Entrada:
  odds_consensus.csv e calibrated_probs.csv (ou predictions_market.csv)
Saída:
  kelly_stakes.csv
"""

import argparse, os, csv, pandas as pd

def resolve_out_dir(r): 
    if os.path.isdir(r): return r
    p=os.path.join("data","out",str(r)); os.makedirs(p,exist_ok=True); return p

def log(msg,dbg=False):
    if dbg: print(f"[kelly] {msg}",flush=True)

def kelly_fraction(prob,odds,cap):
    b=odds-1
    f=((b*prob-(1-prob))/b)
    return max(min(f,cap),0)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--rodada",required=True)
    ap.add_argument("--debug",action="store_true")
    args=ap.parse_args()
    out_dir=resolve_out_dir(args.rodada)

    cfg={"bankroll":1000.0,"kelly_fraction":0.5,"kelly_cap":0.1,"round_to":1.0,"top_n":14}
    log(f"config: {cfg}",args.debug)

    odds=pd.read_csv(os.path.join(out_dir,"odds_consensus.csv"))
    odds['match_key']=odds['team_home'].astype(str)+"__vs__"+odds['team_away'].astype(str)

    prob_file=os.path.join(out_dir,"calibrated_probs.csv")
    if os.path.isfile(prob_file):
        probs=pd.read_csv(prob_file)
        probs.rename(columns={"calib_home":"p_home","calib_draw":"p_draw","calib_away":"p_away"},inplace=True)
    else:
        p_market=os.path.join(out_dir,"predictions_market.csv")
        probs=pd.read_csv(p_market) if os.path.isfile(p_market) else pd.DataFrame()

    if not {'p_home','p_draw','p_away'}.issubset(probs.columns):
        log("predictions_market.csv sem colunas prob_* — ignorando.",True)

    merged=odds.merge(probs,on='match_id',how='left')

    rows=[]
    for _,r in merged.iterrows():
        best_pick,prob,odds_val=max(
            [('HOME',r.get('p_home',0),r.odds_home),
             ('DRAW',r.get('p_draw',0),r.odds_draw),
             ('AWAY',r.get('p_away',0),r.odds_away)],
            key=lambda x:x[1]*x[2]
        )
        edge=prob*odds_val-1
        k_raw=kelly_fraction(prob,odds_val,cfg['kelly_cap'])
        stake=round(cfg['bankroll']*cfg['kelly_fraction']*k_raw,cfg['round_to'])
        rows.append({
            "match_key":r.match_key,
            "team_home":r.team_home,
            "team_away":r.team_away,
            "pick":best_pick,
            "prob":prob,
            "odds":odds_val,
            "edge":edge,
            "kelly_frac_raw":k_raw,
            "kelly_frac_applied":k_raw*cfg['kelly_fraction'],
            "stake":stake
        })

    df=pd.DataFrame(rows)
    out=os.path.join(out_dir,"kelly_stakes.csv")
    df.to_csv(out,index=False,quoting=csv.QUOTE_MINIMAL)
    log(f"OK -> {out} ({len(df)} linhas)",True)
    print(df.head(10).to_string(index=False))

if __name__=="__main__": main()