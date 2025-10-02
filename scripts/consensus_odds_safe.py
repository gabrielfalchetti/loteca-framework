#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera odds de consenso a partir dos CSVs em data/out/<RODADA>/.

- Aceita 1 ou 2 provedores:
    data/out/<RODADA>/odds_theoddsapi.csv
    data/out/<RODADA>/odds_apifootball.csv
- Se existir só um com linhas válidas, é pass-through.
- Se existirem dois, usa a MAIOR odd por mercado (home/draw/away) linha a linha.

Critério de validade da linha: pelo menos 2 odds > 1.0.

Saída: data/out/<RODADA>/odds_consensus.csv com:
  team_home, team_away, match_key, odds_home, odds_draw, odds_away
"""

import argparse
import os
import sys
from typing import Tuple, Dict, List
import pandas as pd
import re

# --------------------------
# Helpers
# --------------------------

def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normaliza cabeçalhos: lower, strip, troca separadores por "_"
    def norm(c: str) -> str:
        c = str(c).strip().lower()
        c = re.sub(r"[ \t\.\-/]+", "_", c)
        c = re.sub(r"[\(\)\[\]\{\}]+", "", c)
        return c
    df = df.copy()
    df.columns = [norm(c) for c in df.columns]
    return df

def _to_num_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype("object").astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    )

def _pick_col_contains(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    # fallback de substring
    for want in candidates:
        for col in df.columns:
            if want in col:
                return col
    return ""

def _pick_numeric_fallback(df: pd.DataFrame, keywords: List[str]) -> str:
    # procura qualquer coluna numérica cujo nome contenha um dos keywords
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(_to_num_series(df[c]))]
    for kw in keywords:
        for c in numeric_cols:
            if kw in c:
                return c
    return ""

# Aliases bem amplos
HOME_CANDS = [
    "odds_home","home_odds","h2h_home","price_home","home","home_price",
    "price1","book_home","h","team_home_odds","home_decimal","home_odds_decimal",
    "h2h_0","m1","outcome_home","market_home","selection_home"
]
DRAW_CANDS = [
    "odds_draw","draw_odds","h2h_draw","price_draw","draw","draw_price",
    "pricex","book_draw","d","empate_odds","x","tie","h2h_1","mx",
    "outcome_draw","market_draw","selection_draw"
]
AWAY_CANDS = [
    "odds_away","away_odds","h2h_away","price_away","away","away_price",
    "price2","book_away","a","team_away_odds","away_decimal","away_odds_decimal",
    "h2h_2","m2","outcome_away","market_away","selection_away"
]

THOME_CANDS = ["team_home","home_team","mandante","time_casa","equipa_casa","time_home"]
TAWAY_CANDS = ["team_away","away_team","visitante","time_fora","equipa_fora","time_away"]
KEY_CANDS   = ["match_key","game_key","fixture_key","key","match","partida"]

def _normalize(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    dbg = {}  # mapeamentos escolhidos
    if df is None or df.empty:
        cols = ["team_home","team_away","match_key","odds_home","odds_draw","odds_away","__valid"]
        return pd.DataFrame(columns=cols), dbg

    df = _clean_columns(df)

    th = _pick_col_contains(df, THOME_CANDS) or "team_home"
    ta = _pick_col_contains(df, TAWAY_CANDS) or "team_away"
    mk = _pick_col_contains(df, KEY_CANDS)

    out = pd.DataFrame({
        "team_home": df.get(th, df.get("team_home")),
        "team_away": df.get(ta, df.get("team_away")),
    })

    if mk and mk in df.columns:
        out["match_key"] = df[mk].astype(str)
        dbg["match_key"] = mk
    else:
        out["match_key"] = (
            out["team_home"].astype(str).str.strip().str.lower()
            + "__vs__" +
            out["team_away"].astype(str).str.strip().str.lower()
        )
        dbg["match_key"] = "<auto>"

    ch = _pick_col_contains(df, HOME_CANDS)
    cd = _pick_col_contains(df, DRAW_CANDS)
    ca = _pick_col_contains(df, AWAY_CANDS)

    # fallbacks por palavras-chave se não achou pelos aliases
    if not ch:
        ch = _pick_numeric_fallback(df, ["home", "cas", "_1", "price1"])
    if not cd:
        cd = _pick_numeric_fallback(df, ["draw", "empate", "_x", "pricex", "tie"])
    if not ca:
        ca = _pick_numeric_fallback(df, ["away", "fora", "_2", "price2"])

    dbg["odds_home"] = ch or "<na>"
    dbg["odds_draw"] = cd or "<na>"
    dbg["odds_away"] = ca or "<na>"

    out["odds_home"] = _to_num_series(df[ch]) if ch else pd.NA
    out["odds_draw"] = _to_num_series(df[cd]) if cd else pd.NA
    out["odds_away"] = _to_num_series(df[ca]) if ca else pd.NA

    def _valid_row(r) -> bool:
        vals = [r["odds_home"], r["odds_draw"], r["odds_away"]]
        vals = [float(x) for x in vals if pd.notna(x)]
        return sum(v > 1.0 for v in vals) >= 2

    out["__valid"] = out.apply(_valid_row, axis=1)
    return out, dbg

def _read_provider(path: str, tag: str) -> Tuple[pd.DataFrame, Dict[str,int]]:
    reasons = {"menos_de_duas_odds": 0}
    if not os.path.isfile(path):
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {path}")
        return pd.DataFrame(), reasons

    try:
        raw = pd.read_csv(path)
    except Exception as e:
        print(f"[consensus-safe] ERRO ao ler {path}: {e}", file=sys.stderr)
        return pd.DataFrame(), reasons

    norm, dbg = _normalize(raw)
    total = len(norm)
    valid = int(norm["__valid"].sum())
    reasons["menos_de_duas_odds"] = total - valid

    print(f"[consensus-safe] lido {os.path.basename(path)} -> {total} linhas; válidas: {valid}")
    print(f"[consensus-safe] mapping {tag}: team_home='{dbg.get('team_home','team_home')}' "
          f"team_away='{dbg.get('team_away','team_away')}' match_key='{dbg.get('match_key')}', "
          f"odds_home='{dbg.get('odds_home')}', odds_draw='{dbg.get('odds_draw')}', odds_away='{dbg.get('odds_away')}'")

    norm = norm[norm["__valid"]].drop(columns=["__valid"]).copy()
    norm["__provider"] = tag
    return norm, reasons

def _merge_best(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Une por match_key e pega a MAIOR odd por mercado."""
    if a.empty and b.empty:
        return pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"])
    if b.empty:
        return a.drop(columns=["__provider"], errors="ignore")
    if a.empty:
        return b.drop(columns=["__provider"], errors="ignore")

    cols = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]
    a_ = a[cols].copy()
    b_ = b[cols].copy()

    m = pd.merge(a_, b_, on=["match_key","team_home","team_away"], how="outer", suffixes=("_a","_b"))

    def pick_max(row, col):
        va = row.get(col + "_a", pd.NA)
        vb = row.get(col + "_b", pd.NA)
        vals = [v for v in [va, vb] if pd.notna(v)]
        return max(vals) if vals else pd.NA

    out = pd.DataFrame({
        "team_home": m["team_home"],
        "team_away": m["team_away"],
        "match_key": m["match_key"],
        "odds_home": m.apply(lambda r: pick_max(r, "odds_home"), axis=1),
        "odds_draw": m.apply(lambda r: pick_max(r, "odds_draw"), axis=1),
        "odds_away": m.apply(lambda r: pick_max(r, "odds_away"), axis=1),
    })

    def valid_row(r):
        vals = [r["odds_home"], r["odds_draw"], r["odds_away"]]
        vals = [float(x) for x in vals if pd.notna(x)]
        return sum(v > 1.0 for v in vals) >= 2

    out["__valid"] = out.apply(valid_row, axis=1)
    out = out[out["__valid"]].drop(columns=["__valid"])
    return out

# --------------------------
# Main
# --------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-09-27_1213")
    args = ap.parse_args()

    out_dir = os.path.join("data","out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    path_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    path_apifoot = os.path.join(out_dir, "odds_apifootball.csv")
    path_cons    = os.path.join(out_dir, "odds_consensus.csv")

    df_theo, r_theo = _read_provider(path_theodds, "theoddsapi")
    df_api , r_api  = _read_provider(path_apifoot, "apifootball")

    df_cons = _merge_best(df_theo, df_api)

    total_valid = len(df_cons)
    total_raw = len(df_theo) + len(df_api)
    print(f"[consensus-safe] consenso bruto: {total_raw} (soma linhas válidas dos provedores); finais (>=2 odds > 1.0): {total_valid}")

    if total_valid == 0:
        # escreve arquivo vazio com cabeçalho para facilitar debug downstream
        empty = pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"])
        empty.to_csv(path_cons, index=False)
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    # Amostra para debug
    sample = df_cons.head(5).to_dict(orient="records")
    print(f"[consensus-safe] AMOSTRA (top 5): {sample}")

    df_cons.to_csv(path_cons, index=False)
    print(f"[consensus-safe] OK -> {path_cons} ({total_valid} linhas)")

if __name__ == "__main__":
    main()