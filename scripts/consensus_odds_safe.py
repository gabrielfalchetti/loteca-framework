# -*- coding: utf-8 -*-
"""
Gera odds_consensus.csv a partir de odds_theoddsapi.csv e/ou odds_apifootball.csv,
FILTRANDO estritamente pelos jogos definidos em data/in/matches_whitelist.csv.

Saída: data/out/<RODADA_ID>/odds_consensus.csv com:
match_id,team_home,team_away,odds_home,odds_draw,odds_away,source

Coloque este arquivo em: scripts/consensus_odds_safe.py
"""

import csv
import os
import sys
from pathlib import Path
import pandas as pd

from _common_norm import match_key_from_teams

def fail(msg, code=6):
    print(f"::error::{msg}")
    sys.exit(code)

def must_have_columns(df, cols, name):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        fail(f"[consensus] {name} sem colunas {miss}")

def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)

def main():
    rodada_id = os.environ.get("RODADA_ID", "").strip()
    if not rodada_id:
        fail("[consensus] RODADA_ID ausente no ambiente")

    out_dir = Path(f"data/out/{rodada_id}")
    out_dir.mkdir(parents=True, exist_ok=True)

    wl_path = Path("data/in/matches_whitelist.csv")
    if not wl_path.exists():
        fail("[consensus] matches_whitelist.csv ausente. Rode scripts/match_whitelist.py antes.")

    wl = pd.read_csv(wl_path)
    must_have_columns(wl, ["match_id", "home", "away", "match_key"], "matches_whitelist.csv")

    # Carrega odds de fontes
    p_theodds = out_dir / "odds_theoddsapi.csv"
    p_apifoot = out_dir / "odds_apifootball.csv"

    df_t = read_csv_safe(p_theodds)
    df_a = read_csv_safe(p_apifoot)

    # Normaliza nomes + gera match_key nas fontes (se presentes)
    def add_key(df):
        if df.empty:
            return df
        # tenta colunas padronizadas (team_home/team_away) ou (home/away)
        th = "team_home" if "team_home" in df.columns else ("home" if "home" in df.columns else None)
        ta = "team_away" if "team_away" in df.columns else ("away" if "away" in df.columns else None)
        if th and ta:
            df = df.copy()
            df["__mk"] = [match_key_from_teams(h, a) for h, a in zip(df[th], df[ta])]
        else:
            df["__mk"] = None
        return df

    df_t = add_key(df_t)
    df_a = add_key(df_a)

    # Combina e filtra SÓ whitelist
    df_all = pd.concat([df_t, df_a], ignore_index=True)
    if df_all.empty:
        fail("[consensus] Não há odds de entrada (theoddsapi/apifootball)")

    # Espera-se colunas de odds (nomes mais comuns)
    odds_cols = [("odds_home", "odds_draw", "odds_away"),
                 ("odd_home", "odd_draw", "odd_away")]  # fallback de alguns scripts

    have = None
    for trio in odds_cols:
        if all(c in df_all.columns for c in trio):
            have = trio
            break
    if have is None:
        fail("[consensus] Não encontrei colunas de odds (odds_home/odds_draw/odds_away ou odd_*) nas fontes")

    oh, od, oa = have
    # filtra pelas chaves da whitelist
    keep_keys = set(wl["match_key"].astype(str))
    df_all = df_all[df_all["__mk"].isin(keep_keys)].copy()
    if df_all.empty:
        fail("[consensus] Após filtro pela whitelist, não sobraram partidas. Confira os nomes/capitalização em matches_source.csv")

    # Agrupa por match_key e faz média das odds
    grp = df_all.groupby("__mk", as_index=False)[[oh, od, oa]].mean()

    # Monta df final com nomes certos e match_id
    wl_small = wl[["match_id", "home", "away", "match_key"]].rename(columns={
        "home": "team_home",
        "away": "team_away"
    })

    final = grp.merge(wl_small, left_on="__mk", right_on="match_key", how="left")
    final = final[["match_id", "team_home", "team_away", oh, od, oa]].rename(columns={
        oh: "odds_home",
        od: "odds_draw",
        oa: "odds_away"
    })
    final["source"] = "consensus"

    out_file = out_dir / "odds_consensus.csv"
    final.to_csv(out_file, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[consensus] OK -> {out_file}")

if __name__ == "__main__":
    main()