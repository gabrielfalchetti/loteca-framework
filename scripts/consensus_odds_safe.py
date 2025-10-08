#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera odds_consensus.csv com normalização robusta e checagem contra whitelist.
"""

from __future__ import annotations
import argparse
import pandas as pd
import unicodedata as ud
import re
from pathlib import Path

UNICODE_SPACES = r"[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000]"

ALIAS = {
    "atletico mineiro": "atletico-mg", "atletico mg": "atletico-mg", "atletico-mineiro": "atletico-mg",
    "américa mineiro": "america-mg", "america mineiro": "america-mg", "america-mineiro": "america-mg", "america mg": "america-mg",
    "operario": "operario-pr", "operario pr": "operario-pr", "operario-pr": "operario-pr",
    "gremio novorizontino": "novorizontino", "grêmio novorizontino": "novorizontino",
    "cuiaba": "cuiaba", "cuiabá": "cuiaba",
    "avai": "avai", "avaí": "avai",
    "mirassol": "mirassol",
    "fluminense": "fluminense",
    "volta redonda": "volta redonda",
    "vila nova": "vila nova",
    "sport recife": "sport", "sport": "sport",
    "athletic club": "athletic club", "athletic club (mg)": "athletic club",
    # já normalizados
    "atletico-mg": "atletico-mg", "america-mg": "america-mg",
}

def strip_accents(s: str) -> str:
    if pd.isna(s): return s
    return "".join(ch for ch in ud.normalize("NFKD", str(s)) if not ud.combining(ch))

def canon(s: str) -> str:
    if pd.isna(s): return s
    s0 = strip_accents(s)
    s0 = re.sub(UNICODE_SPACES, " ", s0)    # normaliza espaços invisíveis
    s0 = s0.lower()
    s0 = re.sub(r"[^a-z0-9\s\-()]", " ", s0)
    s0 = re.sub(r"\s+", " ", s0).strip()
    return ALIAS.get(s0, s0)

def title_id(x: str) -> str:
    return re.sub(r"\s+", " ", x).strip().title()

def load_whitelist(rodada_dir: Path) -> pd.DataFrame:
    wl = pd.read_csv(rodada_dir / "matches_whitelist.csv")
    wl["team_home_n"] = wl["team_home"].apply(canon)
    wl["team_away_n"] = wl["team_away"].apply(canon)
    wl["match_key_n"] = wl["team_home_n"] + "__vs__" + wl["team_away_n"]
    return wl

def read_theodds(rodada_dir: Path) -> pd.DataFrame:
    p = rodada_dir / "odds_theoddsapi.csv"
    if not p.exists(): return pd.DataFrame()
    df = pd.read_csv(p)
    if df.empty: return df
    df["team_home"] = df["home"].apply(canon)
    df["team_away"] = df["away"].apply(canon)
    if "sport" in df.columns:
        df = df[df["sport"].astype(str).str.contains("soccer", case=False, na=False)]
    for c in ("odds_home","odds_draw","odds_away"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["team_home","team_away","odds_home","odds_draw","odds_away"])
    g = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    g["source"] = "theoddsapi"
    return g

def read_apifoot(rodada_dir: Path) -> pd.DataFrame:
    p = rodada_dir / "odds_apifootball.csv"
    if not p.exists(): return pd.DataFrame()
    df = pd.read_csv(p)
    if df.empty: return df
    df["team_home"] = df["team_home"].apply(canon)
    df["team_away"] = df["team_away"].apply(canon)
    for c in ("odds_home","odds_draw","odds_away"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["team_home","team_away","odds_home","odds_draw","odds_away"])
    g = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    g["source"] = "apifootball"
    return g

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Ex.: data/out/1759959606")
    args = ap.parse_args()
    out_dir = Path(args.rodada)

    print("[consensus] ===================================================")
    print("[consensus] GERANDO ODDS CONSENSUS")
    print(f"[consensus] RODADA_ID: {out_dir.name}")
    print(f"[consensus] OUT_DIR  : {out_dir}")
    print("[consensus] ===================================================")

    wl = load_whitelist(out_dir)
    print(f"[consensus][DEBUG] Aliases carregados: {len(ALIAS)}")

    d1 = read_theodds(out_dir);  print(f"[consensus][DEBUG] Carregado odds from theoddsapi: {len(d1)} linhas")
    d2 = read_apifoot(out_dir);  print(f"[consensus][DEBUG] Carregado odds from apifootball: {len(d2)} linhas")

    df = pd.concat([d1, d2], ignore_index=True)
    if df.empty:
        # ainda assim gera arquivo vazio com header correto
        (out_dir / "odds_consensus.csv").write_text("match_id,team_home,team_away,odds_home,odds_draw,odds_away,source\n", encoding="utf-8")
        print("[consensus] Arquivo vazio -> odds_consensus.csv")
        return

    base = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    base["match_id"] = base["team_home"].apply(title_id) + "__" + base["team_away"].apply(title_id)
    base["source"] = "consensus"

    # checa cobertura vs whitelist
    base["match_key_n"] = base["team_home"] + "__vs__" + base["team_away"]
    merged = wl.merge(base, how="left", on="match_key_n", suffixes=("_wl",""))

    not_matched = merged[merged["odds_home"].isna()][["match_id_wl","team_home_wl","team_away_wl","match_key_n"]]
    if len(not_matched) > 0:
        print("Warning: [consensus] Alguns jogos da whitelist não casaram com odds. Mostrando até 10:")
        print(not_matched.rename(columns={
            "match_id_wl":"match_id",
            "team_home_wl":"team_home",
            "team_away_wl":"team_away",
            "match_key_n":"match_key"
        }).head(10).to_string(index=False))

    # salva somente os confrontos que têm odds
    out = merged.dropna(subset=["odds_home"])[["match_id","team_home","team_away","odds_home","odds_draw","odds_away","source"]]
    out = out.sort_values(by="match_id")
    out.to_csv(out_dir / "odds_consensus.csv", index=False)
    print(f"[consensus] OK -> {out_dir/'odds_consensus.csv'}")

    # debug preview
    prev = out.head(10)
    if not prev.empty:
        print("[consensus][DEBUG] Preview odds_consensus (até 10):")
        print(prev.to_string(index=False))

if __name__ == "__main__":
    main()
