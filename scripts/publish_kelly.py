#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
publish_kelly.py (STRICT)

Gera <OUT_DIR>/kelly_stakes.csv com base em probabilidades REAIS e odds REAIS.
Ordem de probabilidade:
  1) predictions_final.csv
  2) predictions_blend.csv
  3) predictions_market.csv

Odds:
  - REQUER odds_consensus.csv (sem fallback).
  - Se ausente/inválido → falha (exit 25).

Sem dados sintéticos. Sem pular etapas.
"""

import os
import sys
import argparse
import math
import pandas as pd


def die(msg: str, code: int = 25):
    print(f"##[error]{msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except Exception:
        return default


def read_ok(path: str, need_cols=None) -> pd.DataFrame:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if need_cols and not all(c in df.columns for c in need_cols):
        return pd.DataFrame()
    return df


def ensure_match_id(df: pd.DataFrame) -> pd.DataFrame:
    if "match_id" not in df.columns:
        if "team_home" in df.columns and "team_away" in df.columns:
            df["match_id"] = df["team_home"].astype(str) + "__" + df["team_away"].astype(str)
        elif "home" in df.columns and "away" in df.columns:
            df["match_id"] = df["home"].astype(str) + "__" + df["away"].astype(str)
        elif "match_key" in df.columns:
            df["match_id"] = df["match_key"].astype(str)
        else:
            die("Não foi possível derivar 'match_id' em algum dataset para Kelly.")
    df["match_id"] = df["match_id"].astype(str)
    return df


def kelly_fraction_decimal(p: float, d: float) -> float:
    # f* = (p*d - 1) / (d - 1), para odds decimais d
    if d <= 1.0 or p < 0 or p > 1:
        return 0.0
    b = d - 1.0
    return max(0.0, (p * d - 1.0) / b)


def expected_edge(p: float, d: float) -> float:
    return p * d - 1.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "kelly_stakes.csv")

    bankroll = env_float("BANKROLL", 1000.0)
    kelly_fraction = env_float("KELLY_FRACTION", 0.5)
    kelly_cap = env_float("KELLY_CAP", 0.1)
    top_n = env_int("KELLY_TOP_N", 14)
    round_to = env_float("ROUND_TO", 1.0)

    # Probabilidades (ordem de preferência)
    order = [
        ("predictions_final.csv", ["match_id","team_home","team_away","p_home","p_draw","p_away"]),
        ("predictions_blend.csv", ["match_id","team_home","team_away","p_home","p_draw","p_away"]),
        ("predictions_market.csv", ["match_id","team_home","team_away","p_home","p_draw","p_away"]),
    ]
    preds = pd.DataFrame()
    used = None
    for fname, cols in order:
        df = read_ok(os.path.join(out_dir, fname), need_cols=cols)
        if not df.empty:
            preds, used = df.copy(), fname
            break
    if preds.empty:
        die("Nenhum arquivo de probabilidades disponível (final/blend/market).")

    preds = ensure_match_id(preds)

    # Odds REAIS obrigatórias
    odds = read_ok(os.path.join(out_dir, "odds_consensus.csv"))
    if odds.empty:
        die("odds_consensus.csv ausente/vazio — sem odds reais não calculamos Kelly.")
    # padroniza colunas de odds
    if "team_home" not in odds.columns and "home" in odds.columns:
        odds = odds.rename(columns={"home":"team_home"})
    if "team_away" not in odds.columns and "away" in odds.columns:
        odds = odds.rename(columns={"away":"team_away"})
    if "odd_home" in odds.columns and "odds_home" not in odds.columns:
        odds = odds.rename(columns={"odd_home":"odds_home","odd_draw":"odds_draw","odd_away":"odds_away"})
    # checa colunas
    for c in ["odds_home","odds_draw","odds_away"]:
        if c not in odds.columns:
            die(f"Coluna obrigatória ausente em odds_consensus.csv: {c}")

    odds = ensure_match_id(odds)

    # Merge estrito por match_id; se falhar, tenta por nomes
    df = preds.merge(odds[["match_id","odds_home","odds_draw","odds_away"]],
                     on="match_id", how="left")
    if df["odds_home"].isna().any():
        # fallback por nomes (ainda real, sem inventar)
        df = df.drop(columns=["odds_home","odds_draw","odds_away"], errors="ignore")
        df = df.merge(odds[["team_home","team_away","odds_home","odds_draw","odds_away"]],
                      on=["team_home","team_away"], how="left")

    if df[["odds_home","odds_draw","odds_away"]].isna().any().any():
        faltantes = df.loc[
            df[["odds_home","odds_draw","odds_away"]].isna().any(axis=1),
            ["match_id","team_home","team_away"]
        ]
        die(f"Odds reais ausentes para partidas:\n{faltantes.to_string(index=False)}")

    # Calcula melhores apostas e stakes
    rows = []
    for _, r in df.iterrows():
        cand = []
        for label, pcol, ocol in [("HOME","p_home","odds_home"),
                                  ("DRAW","p_draw","odds_draw"),
                                  ("AWAY","p_away","odds_away")]:
            p = float(r[pcol])
            o = float(r[ocol])
            e = expected_edge(p, o)
            f = kelly_fraction_decimal(p, o) * kelly_fraction
            f = min(max(f, 0.0), kelly_cap)  # cap
            stake = bankroll * f
            if round_to > 0:
                stake = (stake // round_to) * round_to
            cand.append((label, p, o, e, f, stake))
        # escolhe maior edge
        pick = max(cand, key=lambda t: (t[3], t[1]))
        rows.append({
            "match_key": r["match_id"],
            "team_home": r["team_home"],
            "team_away": r["team_away"],
            "pick": pick[0],
            "prob": pick[1],
            "odds": pick[2],
            "edge": pick[3],
            "kelly_frac_raw": pick[4]/max(kelly_fraction,1e-9),  # fracao antes de multiplicar pela fração global
            "kelly_frac_applied": pick[4],
            "stake": pick[5]
        })

    out = pd.DataFrame(rows).sort_values(["stake","edge","prob"], ascending=[False, False, False])
    if top_n > 0 and len(out) > top_n:
        out = out.head(top_n).copy()
    out.to_csv(out_path, index=False)

    if args.debug:
        print(f"[kelly] config: {{'bankroll': {bankroll}, 'kelly_fraction': {kelly_fraction}, 'kelly_cap': {kelly_cap}, 'round_to': {round_to}, 'top_n': {top_n}}}")
        print(f"[kelly] OK -> {out_path} (linhas={len(out)}) | probs={used}, odds=odds_consensus.csv")
        print(out.head(20).to_csv(index=False))


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        die(f"Falha em publish_kelly: {e}")