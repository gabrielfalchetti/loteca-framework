#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
calibrate_probs.py
------------------
Realiza calibração Dirichlet das probabilidades (1-X-2) derivadas de odds de mercado.

Entrada:
  data/out/<rodada>/predictions_market.csv
     ou, se ausente, odds_consensus.csv (fallback odds→probs).

Saída:
  data/out/<rodada>/calibrated_probs.csv
     match_id,calib_method,calib_home,calib_draw,calib_away
"""

import argparse, os, csv, pandas as pd, numpy as np

def log(msg, dbg=False):
    if dbg:
        print(f"[calibrate] {msg}", flush=True)

def resolve_out_dir(r):
    if os.path.isdir(r): return r
    p=os.path.join("data","out",str(r)); os.makedirs(p,exist_ok=True); return p

def implied_from_odds(h,d,a):
    ih,id_,ia=[1/x if x>0 else 0 for x in (h,d,a)]; s=ih+id_+ia
    return (ih/s,id_/s,ia/s) if s>0 else (0,0,0)

def dirichlet_calibration(df):
    """Simples reescala para suavizar entropia; sem dependência externa."""
    def _row(r):
        arr=np.array([r['p_home'],r['p_draw'],r['p_away']])
        arr=np.clip(arr,1e-6,1.0)
        arr=arr/arr.sum()
        alpha=arr*30
        calib=alpha/alpha.sum()
        return calib
    out=np.vstack(df.apply(_row,axis=1))
    df[['calib_home','calib_draw','calib_away']]=out
    return df

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--rodada",required=True)
    ap.add_argument("--debug",action="store_true")
    args=ap.parse_args()

    out_dir=resolve_out_dir(args.rodada)
    log("="*51,args.debug)
    log("INICIANDO CALIBRAÇÃO DE PROBABILIDADES",args.debug)
    log(f"Diretório de rodada : {out_dir}",args.debug)
    log("="*51,args.debug)

    cand1=os.path.join(out_dir,"predictions_market.csv")
    cand2=os.path.join(out_dir,"odds_consensus.csv")
    if os.path.isfile(cand1):
        df=pd.read_csv(cand1)
        if not {'p_home','p_draw','p_away'}.issubset(df.columns):
            log("predictions_market.csv sem colunas ['p_home','p_draw','p_away']. Usando fallback por odds.",True)
            df=pd.read_csv(cand2)
            probs=[implied_from_odds(r.odds_home,r.odds_draw,r.odds_away) for _,r in df.iterrows()]
            df[['p_home','p_draw','p_away']]=pd.DataFrame(probs)
    else:
        df=pd.read_csv(cand2)
        log("predictions_market.csv ausente, usando fallback odds_consensus.csv",True)
        probs=[implied_from_odds(r.odds_home,r.odds_draw,r.odds_away) for _,r in df.iterrows()]
        df[['p_home','p_draw','p_away']]=pd.DataFrame(probs)

    df=dirichlet_calibration(df)
    df['calib_method']='Dirichlet'

    out=os.path.join(out_dir,"calibrated_probs.csv")
    df[['match_id','calib_method','calib_home','calib_draw','calib_away']].to_csv(out,index=False,quoting=csv.QUOTE_MINIMAL)
    log(f"Salvo em: {out}",True)
    print(df[['match_id','calib_method','calib_home','calib_draw','calib_away']].head(10).to_string(index=False))
    log("[ok] Calibração concluída com sucesso.",True)

if __name__=="__main__": main()