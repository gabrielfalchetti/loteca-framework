#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera odds de consenso a partir dos CSVs presentes em data/out/<RODADA>/.

✅ Aceita 1 ou 2 provedores:
   - data/out/<RODADA>/odds_theoddsapi.csv
   - data/out/<RODADA>/odds_apifootball.csv
Se existir só um arquivo com linhas válidas, ele é usado como consenso (pass-through).
Se existirem os dois, o consenso pega a **melhor odd disponível (máxima)** por mercado
(home/draw/away) linha a linha.

Critério de validade por linha: pelo menos 2 odds > 1.0 (home/draw/away).

Saída: data/out/<RODADA>/odds_consensus.csv com colunas:
  team_home, team_away, match_key, odds_home, odds_draw, odds_away

Uso:
  python -m scripts.consensus_odds_safe --rodada 2025-09-27_1213
"""

import argparse
import os
import sys
import math
from typing import Tuple, Dict, List
import pandas as pd

# --------------------------
# Normalização de colunas
# --------------------------
HOME_CANDIDATES = [
    "odds_home","home_odds","h2h_home","price_home","home","home_price",
    "price1","book_home","h","team_home_odds"
]
DRAW_CANDIDATES = [
    "odds_draw","draw_odds","h2h_draw","price_draw","draw","draw_price",
    "pricex","book_draw","d","empate_odds","x"
]
AWAY_CANDIDATES = [
    "odds_away","away_odds","h2h_away","price_away","away","away_price",
    "price2","book_away","a","team_away_odds"
]
THOME_CANDIDATES = ["team_home","home_team","mandante","time_casa"]
TAWAY_CANDIDATES = ["team_away","away_team","visitante","time_fora"]
KEY_CANDIDATES   = ["match_key","game_key","fixture_key","key","match","partida"]

def _to_num_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype("object").astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    )

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    # tentativa por "contém" (ex.: outcomes.home, markets_h2h_home etc.)
    lcols = [c.lower() for c in df.columns]
    for name in candidates:
        for i, lc in enumerate(lcols):
            if name in lc:
                return df.columns[i]
    return ""

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "team_home","team_away","match_key",
            "odds_home","odds_draw","odds_away","__valid"
        ])

    th = _pick_col(df, THOME_CANDIDATES) or "team_home"
    ta = _pick_col(df, TAWAY_CANDIDATES) or "team_away"
    mk = _pick_col(df, KEY_CANDIDATES)

    out = pd.DataFrame({
        "team_home": df[th] if th in df.columns else df.get("team_home"),
        "team_away": df[ta] if ta in df.columns else df.get("team_away"),
    })

    if mk and mk in df.columns:
        out["match_key"] = df[mk].astype(str)
    else:
        # gera se não existir
        out["match_key"] = (
            out["team_home"].astype(str).str.strip().str.lower()
            + "__vs__" +
            out["team_away"].astype(str).str.strip().str.lower()
        )

    ch = _pick_col(df, HOME_CANDIDATES)
    cd = _pick_col(df, DRAW_CANDIDATES)
    ca = _pick_col(df, AWAY_CANDIDATES)

    out["odds_home"] = _to_num_series(df[ch]) if ch else pd.NA
    out["odds_draw"] = _to_num_series(df[cd]) if cd else pd.NA
    out["odds_away"] = _to_num_series(df[ca]) if ca else pd.NA

    def _valid_row(r) -> bool:
        vals = [r["odds_home"], r["odds_draw"], r["odds_away"]]
        vals = [float(x) for x in vals if pd.notna(x)]
        return sum(v > 1.0 for v in vals) >= 2

    out["__valid"] = out.apply(_valid_row, axis=1)
    return out

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

    norm = _normalize(raw)
    total = len(norm)
    valid = int(norm["__valid"].sum())
    reasons["menos_de_duas_odds"] = total - valid

    print(f"[consensus-safe] lido {os.path.basename(path)} -> {total} linhas; válidas: {valid}")
    if total and reasons["menos_de_duas_odds"] > 0:
        print(f"[consensus-safe] motivos inválidos {tag}: {reasons}")

    norm = norm[norm["__valid"]].drop(columns=["__valid"]).copy()
    norm["__provider"] = tag
    return norm, reasons

# --------------------------
# Consenso
# --------------------------
def _merge_best(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Une por match_key e pega a MELHOR odd disponível (máxima) por mercado."""
    if a.empty and b.empty:
        return pd.DataFrame(columns=["team_home","team_away","match_key",
                                     "odds_home","odds_draw","odds_away"])
    if b.empty:
        return a.drop(columns=["__provider"], errors="ignore")
    if a.empty:
        return b.drop(columns=["__provider"], errors="ignore")

    cols = ["match_key","team_home","team_away",
            "odds_home","odds_draw","odds_away"]
    # evita colunas duplicadas de nomes iguais no merge
    a_ = a[cols].copy()
    b_ = b[cols].copy()

    m = pd.merge(
        a_, b_,
        on=["match_key","team_home","team_away"],
        how="outer",
        suffixes=("_a","_b")
    )

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

    # reforça o critério de validade
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

    # Regra principal: aceita 1 provedor só; se tiver 2, faz "melhor odd".
    df_cons = _merge_best(df_theo, df_api)

    total_valid = len(df_cons)
    total_raw = len(df_theo) + len(df_api)
    print(f"[consensus-safe] consenso bruto: {total_raw} (soma linhas válidas dos provedores); finais (>=2 odds > 1.0): {total_valid}")

    # Amostra para debug
    if total_valid:
        sample = df_cons.head(5).to_dict(orient="records")
        print(f"[consensus-safe] AMOSTRA (top 5): {sample}")

    if total_valid == 0:
        # escreve arquivo vazio com cabeçalho para facilitar debug downstream
        empty = pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"])
        empty.to_csv(path_cons, index=False)
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    df_cons.to_csv(path_cons, index=False)
    print(f"[consensus-safe] OK -> {path_cons} ({total_valid} linhas)")

if __name__ == "__main__":
    main()