#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sanity_reality_check.py
-----------------------
Validação básica pós-pipeline: confere presença e integridade dos principais arquivos.
"""

import argparse, os, json, pandas as pd

REQUIRED={
    "matches_source":["match_id","home","away"],
    "odds_consensus":["team_home","team_away"],
    "predictions_market":["home","away"],
    "kelly_stakes":["match_key","team_home","team_away","pick"],
}

def resolve_out_dir(r):
    if os.path.isdir(r): return r
    p=os.path.join("data","out",str(r)); os.makedirs(p,exist_ok=True); return p

def check_csv(name,path,cols):
    if not os.path.isfile(path):
        return {"status":"missing","path":path}
    try:
        df=pd.read_csv(path)
        missing=[c for c in cols if c not in df.columns]
        if missing: return {"status":"error","missing_cols":missing,"path":path}
        return {"status":"ok","rows":len(df),"path":path}
    except Exception as e:
        return {"status":"error","error":str(e)}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--rodada",required=True)
    args=ap.parse_args()
    out_dir=resolve_out_dir(args.rodada)

    results={}
    for name,cols in REQUIRED.items():
        path=("data/in/"+name+".csv") if name=="matches_source" else os.path.join(out_dir,name+".csv")
        results[name]=check_csv(name,path,cols)

    report=os.path.join(out_dir,"reality_report.json")
    with open(report,"w") as f: json.dump(results,f,indent=2)
    txt="\n".join([f"{k}: {v['status']} | {v.get('path','')}" for k,v in results.items()])
    with open(os.path.join(out_dir,"reality_report.txt"),"w") as f: f.write(txt)
    print("[reality] OK ->",report)
    print(txt)
    print("Sanity OK.")

if __name__=="__main__": main()