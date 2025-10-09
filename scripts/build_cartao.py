#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_cartao.py (STRICT)

Monta <OUT_DIR>/loteca_cartao.txt a partir de:
- matches_whitelist.csv (ordem e rótulos dos jogos)
- predictions_final.csv (preferência) OU predictions_blend.csv OU predictions_market.csv
- kelly_stakes.csv (para stake e pick)

Regra: se QUALQUER jogo da whitelist não tiver prob+stake calculados → falha (exit 26).
"""

import os
import sys
import argparse
import pandas as pd


def die(msg: str, code: int = 26):
    print(f"##[error]{msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def read_ok(path: str) -> pd.DataFrame:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def ensure_match_key(df: pd.DataFrame) -> pd.DataFrame:
    if "match_key" in df.columns:
        df["match_key"] = df["match_key"].astype(str)
        return df
    # tenta derivar a partir de match_id
    if "match_id" in df.columns:
        df["match_key"] = df["match_id"].astype(str)
        return df
    if "team_home" in df.columns and "team_away" in df.columns:
        df["match_key"] = df["team_home"].astype(str) + "__" + df["team_away"].astype(str)
        return df
    raise ValueError("Não foi possível garantir 'match_key'.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)
    wl = read_ok(os.path.join(out_dir, "matches_whitelist.csv"))
    if wl.empty:
        die("matches_whitelist.csv ausente/vazio.")
    wl = ensure_match_key(wl)

    # probabilidades
    preds = None
    for fname in ["predictions_final.csv","predictions_blend.csv","predictions_market.csv"]:
        df = read_ok(os.path.join(out_dir, fname))
        if not df.empty and all(c in df.columns for c in ["p_home","p_draw","p_away"]):
            preds = df.copy()
            preds = ensure_match_key(preds)
            break
    if preds is None:
        die("Nenhum arquivo de probabilidades encontrado (final/blend/market).")

    kelly = read_ok(os.path.join(out_dir, "kelly_stakes.csv"))
    if kelly.empty:
        die("kelly_stakes.csv ausente/vazio.")
    kelly = ensure_match_key(kelly)

    # join estrito
    df = wl.merge(preds[["match_key","team_home","team_away","p_home","p_draw","p_away"]],
                  on="match_key", how="left")
    df = df.merge(kelly[["match_key","pick","stake"]],
                  on="match_key", how="left")

    if df[["team_home","team_away","p_home","p_draw","p_away","pick","stake"]].isna().any().any():
        missing = df[df[["team_home","team_away","p_home","p_draw","p_away","pick","stake"]].isna().any(axis=1)][["match_key"]]
        die(f"Cartão incompleto: faltam dados para {len(missing)} jogo(s):\n{missing.to_string(index=False)}")

    # montar texto
    lines = ["==== CARTÃO LOTECA ===="]
    for i, r in df.reset_index(drop=True).iterrows():
        jnum = f"Jogo {i+1:02d}"
        fav = max(("1", r["p_home"]), ("X", r["p_draw"]), ("2", r["p_away"]), key=lambda x: x[1])
        fav_txt = {"1":"1","X":"X","2":"2"}[fav[0]]
        conf = round(float(fav[1])*100, 1)
        lines.append(f"{jnum} - {r['team_home']} x {r['team_away']}: {fav_txt} (stake={r['stake']}) [{conf}%]")
    lines.append("=======================")

    out_path = os.path.join(out_dir, "loteca_cartao.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))
    print(f"[cartao] OK -> {out_path}")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        die(f"Erro inesperado: {e}")