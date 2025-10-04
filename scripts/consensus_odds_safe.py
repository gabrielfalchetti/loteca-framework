#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import math
import argparse
import pandas as pd

def valid_mask(df: pd.DataFrame) -> pd.Series:
    def row_ok(r):
        vals = [r.get("odds_home"), r.get("odds_draw"), r.get("odds_away")]
        cnt = sum(1 for x in vals if isinstance(x,(int,float)) and x and x>1.0 and not (isinstance(x,float) and math.isnan(x)))
        return cnt >= 2
    return df.apply(row_ok, axis=1)

def safe_read(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {path}")
        return pd.DataFrame(columns=["match_key","team_home","team_away","odds_home","odds_draw","odds_away"])
    df = pd.read_csv(path)
    for col in ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]:
        if col not in df.columns:
            df[col] = None
    return df[["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    p_theodds = os.path.join(out_dir,"odds_theoddsapi.csv")
    p_apifoot = os.path.join(out_dir,"odds_apifootball.csv")
    out_path  = os.path.join(out_dir,"odds_consensus.csv")

    df1 = safe_read(p_theodds)
    df2 = safe_read(p_apifoot)

    # filtra válidas por provedor
    v1 = df1[valid_mask(df1)] if not df1.empty else df1
    v2 = df2[valid_mask(df2)] if not df2.empty else df2

    total_valid = (0 if v1 is None else len(v1)) + (0 if v2 is None else len(v2))
    print(f"[consensus-safe] consenso bruto: {total_valid} (soma linhas válidas dos provedores)")

    # se só existe um provedor válido, usa ele
    if not v1.empty and v2.empty:
        v1.to_csv(out_path, index=False)
        print(f"[consensus-safe] OK -> {out_path} ({len(v1)} linhas) | mapping theoddsapi: team_home='team_home', team_away='team_away', match_key='match_key', odds_home='odds_home', odds_draw='odds_draw', odds_away='odds_away'")
        sys.exit(0)
    if v1.empty and not v2.empty:
        v2.to_csv(out_path, index=False)
        print(f"[consensus-safe] OK -> {out_path} ({len(v2)} linhas) | mapping apifootball: team_home='team_home', team_away='team_away', match_key='match_key', odds_home='odds_home', odds_draw='odds_draw', odds_away='odds_away'")
        sys.exit(0)

    # se os dois existem, prioriza o que tiver mais odds (ou faz média onde ambos existem)
    if v1.empty and v2.empty:
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(1)

    # merge por match_key
    m = pd.merge(v1, v2, on=["match_key","team_home","team_away"], how="outer", suffixes=("_1","_2"))
    def pick(o1, o2):
        # se uma faltar, pega a outra; se ambas existirem, pode escolher a maior (ou média)
        if pd.isna(o1) and pd.isna(o2):
            return None
        if pd.isna(o1): return o2
        if pd.isna(o2): return o1
        return max(o1, o2)  # escolhe o melhor preço

    out = pd.DataFrame({
        "match_key": m["match_key"],
        "team_home": m["team_home"],
        "team_away": m["team_away"],
        "odds_home": [pick(a,b) for a,b in zip(m.get("odds_home_1"), m.get("odds_home_2"))],
        "odds_draw": [pick(a,b) for a,b in zip(m.get("odds_draw_1"), m.get("odds_draw_2"))],
        "odds_away": [pick(a,b) for a,b in zip(m.get("odds_away_1"), m.get("odds_away_2"))],
    })
    out = out[valid_mask(out)]
    out.to_csv(out_path, index=False)
    print(f"[consensus-safe] OK -> {out_path} ({len(out)} linhas)")

if __name__ == "__main__":
    main()