#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Normaliza um CSV de odds para o formato esperado:
  obrigatórias: team_home, team_away, match_key, odds_home, odds_draw, odds_away
- Varre o CSV procurando nomes alternativos para home/draw/away (h2h, price, etc)
- Converte números com vírgula/strings para float
- Mantém só linhas com pelo menos 2 odds válidas (>1.0)
Uso:
  python scripts/normalize_odds_csv.py \
    --in data/out/RODADA/odds_theoddsapi.csv \
    --out data/out/RODADA/odds_theoddsapi.csv
"""
import argparse
import math
import sys
import pandas as pd

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

KEY_CANDIDATES = ["match_key","game_key","fixture_key","key","match","partida"]
THOME_CANDIDATES = ["team_home","home_team","mandante","time_casa"]
TAWAY_CANDIDATES = ["team_away","away_team","visitante","time_fora"]

NUMERIC_COLS = set(HOME_CANDIDATES + DRAW_CANDIDATES + AWAY_CANDIDATES)

def to_num(s):
    return pd.to_numeric(
        pd.Series(s, dtype="object").astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    )

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    # tenta por “contém” para casos tipo book.home, outcomes.home etc
    lc = [c.lower() for c in df.columns]
    for name in candidates:
        for i, c in enumerate(lc):
            if name in c:
                return df.columns[i]
    return None

def ensure_columns(df):
    # mapeia básicos
    th = pick_col(df, THOME_CANDIDATES) or "team_home"
    ta = pick_col(df, TAWAY_CANDIDATES) or "team_away"
    mk = pick_col(df, KEY_CANDIDATES) or "__mk_tmp__"
    if mk not in df.columns:
        df[mk] = (df[th].astype(str).str.strip().str.lower() + "__vs__" +
                  df[ta].astype(str).str.strip().str.lower())
    # odds
    ch = pick_col(df, HOME_CANDIDATES)
    cd = pick_col(df, DRAW_CANDIDATES)
    ca = pick_col(df, AWAY_CANDIDATES)

    out = pd.DataFrame({
        "team_home": df[th],
        "team_away": df[ta],
        "match_key": df[mk]
    })

    if ch:
        out["odds_home"] = to_num(df[ch])
    else:
        out["odds_home"] = pd.NA

    if cd:
        out["odds_draw"] = to_num(df[cd])
    else:
        out["odds_draw"] = pd.NA

    if ca:
        out["odds_away"] = to_num(df[ca])
    else:
        out["odds_away"] = pd.NA

    # remove linhas com menos de 2 odds válidas > 1.0
    def valid_row(r):
        vals = [r["odds_home"], r["odds_draw"], r["odds_away"]]
        vals = [float(x) for x in vals if pd.notna(x)]
        return sum(v > 1.0 for v in vals) >= 2

    out["__valid"] = out.apply(valid_row, axis=1)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.in_path)
    norm = ensure_columns(df)

    total = len(norm)
    valid = int(norm["__valid"].sum())
    print(f"[normalize] linhas totais: {total}; válidas (>=2 odds > 1.0): {valid}")

    norm = norm[norm["__valid"]].drop(columns=["__valid"])
    if valid == 0:
        print("[normalize] ERRO: nenhuma linha válida encontrada. Verifique os nomes das colunas no arquivo de entrada.", file=sys.stderr)
        # ainda sobrescreve para refletir limpeza, mas retorna erro !=0
        norm.to_csv(args.out_path, index=False)
        sys.exit(10)

    norm.to_csv(args.out_path, index=False)
    print(f"[normalize] OK -> {args.out_path} ({len(norm)} linhas)")

if __name__ == "__main__":
    main()