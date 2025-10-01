#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera consenso (TheOddsAPI > API-Football).
Se nenhum provedor tiver odds:
- normal: escreve CSV vazio com cabeçalho;
- **REQUIRE_ODDS=true**: falha com exit code.
"""

import os
import sys
import argparse
import pandas as pd

COLS = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]

def read_if_exists(path: str):
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame(columns=COLS)
    return pd.DataFrame(columns=COLS)

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ren = {}
    if "home_team" in out.columns: ren["home_team"] = "team_home"
    if "away_team" in out.columns: ren["away_team"] = "team_away"
    out = out.rename(columns=ren)
    for c in COLS:
        if c not in out.columns:
            out[c] = None
    out["__join_key"] = (
        out["team_home"].astype(str).str.lower().str.strip()
        + "__vs__"
        + out["team_away"].astype(str).str.lower().str.strip()
    )
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    strict = os.environ.get("REQUIRE_ODDS", "").strip().lower() in ("1","true","yes")

    out_dir = os.path.join("data","out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    p1 = os.path.join(out_dir, "odds_theoddsapi.csv")
    p2 = os.path.join(out_dir, "odds_apifootball.csv")

    a = normalize(read_if_exists(p1))
    b = normalize(read_if_exists(p2))

    if a.empty and b.empty:
        if strict:
            print("[consensus-safe] ERRO: modo estrito ativo e nenhum provedor retornou odds.")
            sys.exit(21)
        else:
            print("[consensus-safe] AVISO: nenhum provedor retornou odds. CSV vazio gerado.")
            pd.DataFrame(columns=COLS).to_csv(os.path.join(out_dir, "odds_consensus.csv"), index=False)
            print(f"[consensus-safe] OK -> {os.path.join(out_dir,'odds_consensus.csv')} (0 linhas)")
            return

    merged = a.set_index("__join_key")
    if not b.empty:
        b = b.set_index("__join_key")
        merged = merged.combine_first(b)

    final = merged.reset_index(drop=True)[COLS].drop_duplicates()
    final.to_csv(os.path.join(out_dir, "odds_consensus.csv"), index=False)
    print(f"[consensus-safe] OK -> {os.path.join(out_dir,'odds_consensus.csv')} ({len(final)} linhas)")

if __name__ == "__main__":
    main()
