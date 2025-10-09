#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera apostas pelo critério de Kelly usando apenas dados reais (sem placeholders).

Prioridade de fontes dentro de <RODADA>:
1) calibrated_probs.csv            -> p_home,p_draw,p_away (preferida)
2) predictions_final.csv           -> p_* OU odds_*
3) predictions_blend.csv           -> p_* OU odds_*   (opcional)
4) predictions_market.csv          -> odds_*
5) odds_consensus.csv              -> odds_*

Se uma fonte não trouxer team_home/team_away, tentamos:
- Normalizar aliases (home, away, home_team, away_team, etc.)
- Enriquecer via <RODADA>/matches_whitelist.csv (merge por match_id)

Saída:
- <RODADA>/kelly_stakes.csv

Flags (ou via env):
--bankroll (BANKROLL=1000)
--kelly_fraction (KELLY_FRACTION=0.5)
--kelly_cap (KELLY_CAP=0.1)
--top_n (KELLY_TOP_N=14)
--round_to (ROUND_TO=1)

Erros "duros" e claros se:
- Nenhuma fonte válida
- Odds inválidas (NaN ou <=1.0) quando necessárias
- Probabilidades fora de [0,1] ou soma zerada
"""

import argparse
import os
import sys
import math
import pandas as pd
from typing import Optional

REQ_TEAM = ["match_id", "team_home", "team_away"]
P_COLS = ["p_home", "p_draw", "p_away"]
O_COLS = ["odds_home", "odds_draw", "odds_away"]

TEAM_ALIASES = [
    ("home", "team_home"), ("away", "team_away"),
    ("home_team", "team_home"), ("away_team", "team_away"),
    ("homeName", "team_home"), ("awayName", "team_away"),
    ("home_name", "team_home"), ("away_name", "team_away"),
    ("TeamHome", "team_home"), ("TeamAway", "team_away"),
    ("teamHome", "team_home"), ("teamAway", "team_away"),
    ("localteam_name", "team_home"), ("visitorteam_name", "team_away"),
    ("Home", "team_home"), ("Away", "team_away"),
]

ID_ALIASES = ["match_id", "fixture_id", "game_id", "id", "matchId", "fixtureId"]

P_ALIASES = [
    ("prob_home", "p_home"), ("prob_draw", "p_draw"), ("prob_away", "p_away"),
    ("ph", "p_home"), ("pd", "p_draw"), ("pa", "p_away"),
    ("home_prob", "p_home"), ("draw_prob", "p_draw"), ("away_prob", "p_away"),
]

O_ALIASES = [
    ("home_odds", "odds_home"), ("draw_odds", "odds_draw"), ("away_odds", "odds_away"),
    ("odd_home", "odds_home"), ("odd_draw", "odds_draw"), ("odd_away", "odds_away"),
    ("o_home", "odds_home"), ("o_draw", "odds_draw"), ("o_away", "odds_away"),
]

def die(msg: str, code: int = 25):
    print(f"[kelly][ERRO] {msg}", file=sys.stderr)
    sys.exit(code)

def read_csv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    try:
        df = pd.read_csv(path)
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        print(f"[kelly][WARN] Falha lendo {path}: {e}", file=sys.stderr)
        return None

def rename_by_aliases(df: pd.DataFrame, aliases: list[tuple[str, str]]) -> pd.DataFrame:
    to_rename = {}
    cols_lower = {c.lower(): c for c in df.columns}
    for src, dst in aliases:
        # match case-insensitive
        if src in df.columns:
            to_rename[src] = dst
        elif src.lower() in cols_lower:
            to_rename[cols_lower[src.lower()]] = dst
    if to_rename:
        df = df.rename(columns=to_rename)
    return df

def ensure_match_id(df: pd.DataFrame) -> pd.DataFrame:
    # já existe match_id?
    if "match_id" in df.columns:
        return df
    # tenta aliases
    for c in ID_ALIASES:
        if c in df.columns:
            df = df.rename(columns={c: "match_id"})
            return df
        # case-insensitive
        lower_map = {x.lower(): x for x in df.columns}
        if c.lower() in lower_map:
            df = df.rename(columns={lower_map[c.lower()]: "match_id"})
            return df
    die("Fonte sem coluna de identificação do jogo (match_id/fixture_id/game_id/id).")

def normalize_teams(df: pd.DataFrame) -> pd.DataFrame:
    # amplia normalização de times
    df = rename_by_aliases(df, TEAM_ALIASES)
    # alguns dumps trazem "homeTeam"/"awayTeam"
    df = rename_by_aliases(df, [("homeTeam", "team_home"), ("awayTeam", "team_away")])

    return df

def normalize_probs_and_odds(df: pd.DataFrame) -> pd.DataFrame:
    df = rename_by_aliases(df, P_ALIASES)
    df = rename_by_aliases(df, O_ALIASES)
    # também tenta mapear qualquer coisa que comece com 'prob_' para p_*
    for c in list(df.columns):
        lc = c.lower().strip()
        if lc.startswith("prob_"):
            if "home" in lc and "p_home" not in df.columns:
                df = df.rename(columns={c: "p_home"})
            elif "draw" in lc and "p_draw" not in df.columns:
                df = df.rename(columns={c: "p_draw"})
            elif "away" in lc and "p_away" not in df.columns:
                df = df.rename(columns={c: "p_away"})
    return df

def enrich_teams_from_whitelist(rodada: str, df: pd.DataFrame) -> pd.DataFrame:
    """Se faltar team_home/team_away, tenta enriquecer via matches_whitelist.csv."""
    if all(c in df.columns for c in ["team_home", "team_away"]):
        return df
    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    wl = read_csv(wl_path)
    if wl is None:
        # tenta em data/in como fallback (tudo real também)
        wl = read_csv(os.path.join("data", "in", "matches_whitelist.csv"))
    if wl is None or wl.empty:
        return df
    # normaliza whitelist (home/away -> team_home/team_away)
    if "team_home" not in wl.columns and "home" in wl.columns:
        wl = wl.rename(columns={"home": "team_home"})
    if "team_away" not in wl.columns and "away" in wl.columns:
        wl = wl.rename(columns={"away": "team_away"})
    if not all(c in wl.columns for c in REQ_TEAM):
        return df
    base = df.copy()
    base = ensure_match_id(base)
    m = base.merge(wl[REQ_TEAM], on="match_id", how="left", suffixes=("", "_wl"))
    # preenche apenas quando faltar
    for tgt in ["team_home", "team_away"]:
        if tgt not in base.columns or base[tgt].isna().any():
            src = tgt
            if src not in m.columns:
                src = f"{tgt}_wl"
            if src in m.columns:
                m[tgt] = m[tgt].fillna(m[src])
    return m

def validate_team_cols(df: pd.DataFrame, src: str, rodada: str) -> pd.DataFrame:
    df = ensure_match_id(df)
    df = normalize_teams(df)
    missing = [c for c in ["team_home", "team_away"] if c not in df.columns]
    if missing:
        df = enrich_teams_from_whitelist(rodada, df)
    missing2 = [c for c in REQ_TEAM if c not in df.columns]
    if missing2:
        die(f"{src} sem colunas mínimas {REQ_TEAM}. Faltando: {sorted(missing2)}")
    return df

def odds_to_probs(df: pd.DataFrame, src: str) -> pd.DataFrame:
    if not all(c in df.columns for c in O_COLS):
        die(f"{src} não possui odds_* completas para inferir probabilidades.")
    bad = (
        df["odds_home"].isna() | df["odds_draw"].isna() | df["odds_away"].isna() |
        (df["odds_home"] <= 1.0) | (df["odds_draw"] <= 1.0) | (df["odds_away"] <= 1.0)
    )
    if bad.any():
        sample = df.loc[bad, ["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]].head(10)
        die(f"Odds inválidas em {src} (NaN ou <=1.0). Amostra:\n{sample.to_string(index=False)}", code=98)
    ph = 1.0 / df["odds_home"].astype(float)
    pd_ = 1.0 / df["odds_draw"].astype(float)
    pa = 1.0 / df["odds_away"].astype(float)
    s = ph + pd_ + pa
    df["p_home"] = ph / s
    df["p_draw"] = pd_ / s
    df["p_away"] = pa / s
    return df

def ensure_probs(df: pd.DataFrame, src: str) -> pd.DataFrame:
    df = normalize_probs_and_odds(df)
    if all(c in df.columns for c in P_COLS):
        for c in P_COLS:
            if df[c].isna().any():
                die(f"{src} contém NaN em {c}.", code=98)
            if (df[c] < 0).any() or (df[c] > 1).any():
                die(f"{src} contém {c} fora de [0,1].", code=98)
        s = (df["p_home"] + df["p_draw"] + df["p_away"])
        if (s <= 0).any():
            die(f"{src} com soma de probabilidades <= 0.", code=98)
        # normaliza por linha
        df["p_home"] = df["p_home"] / s
        df["p_draw"] = df["p_draw"] / s
        df["p_away"] = df["p_away"] / s
        return df
    # caso contrário, exige odds_*
    return odds_to_probs(df, src)

def load_best_source(rodada: str) -> tuple[pd.DataFrame, str]:
    candidates = [
        ("calibrated_probs.csv", True),
        ("predictions_final.csv", False),
        ("predictions_blend.csv", False),
        ("predictions_market.csv", False),
        ("odds_consensus.csv", False),
    ]
    for fname, expect_probs in candidates:
        path = os.path.join(rodada, fname)
        df = read_csv(path)
        if df is None:
            continue
        # valida/normaliza IDs e times
        df = validate_team_cols(df, fname, rodada)
        # normaliza prob/odds
        df = ensure_probs(df, fname) if (not expect_probs or ("p_home" in df.columns)) else ensure_probs(df, fname)
        return df, fname
    die("Nenhuma fonte de probabilidades disponível (calibrated/final/blend/market/consensus).")

def best_kelly_for_row(row: pd.Series, bankroll: float, frac: float, cap: float) -> Optional[dict]:
    options = [
        ("home", row["p_home"], row.get("odds_home", float("nan"))),
        ("draw", row["p_draw"], row.get("odds_draw", float("nan"))),
        ("away", row["p_away"], row.get("odds_away", float("nan"))),
    ]
    have_market = all((c in row and isinstance(row[c], (int, float))) for c in O_COLS)
    results = []
    for sel, p, o in options:
        if p is None or not (0 <= p <= 1) or (isinstance(p, float) and math.isnan(p)):
            continue
        if have_market and (o is not None) and (not (isinstance(o, float) and math.isnan(o))) and o > 1.0:
            b = o - 1.0
            k = (p * o - 1.0) / b
            k_eff = max(0.0, min(k, cap))
            stake = bankroll * frac * k_eff if k_eff > 0 else 0.0
            edge = p * o - 1.0
            results.append({"selection": sel, "prob": p, "odds": o, "kelly_fraction": k_eff, "stake": stake, "edge": edge})
        else:
            results.append({"selection": sel, "prob": p, "odds": float("nan"), "kelly_fraction": 0.0, "stake": 0.0, "edge": float("nan")})
    if not results:
        return None
    results.sort(key=lambda d: (d["kelly_fraction"], (d["edge"] if not math.isnan(d["edge"]) else -1e9)), reverse=True)
    return results[0]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--bankroll", type=float, default=float(os.getenv("BANKROLL", "1000")))
    ap.add_argument("--kelly_fraction", type=float, default=float(os.getenv("KELLY_FRACTION", "0.5")))
    ap.add_argument("--kelly_cap", type=float, default=float(os.getenv("KELLY_CAP", "0.1")))
    ap.add_argument("--top_n", type=int, default=int(os.getenv("KELLY_TOP_N", "14")))
    ap.add_argument("--round_to", type=float, default=float(os.getenv("ROUND_TO", "1")))
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    df, src = load_best_source(args.rodada)
    if args.debug:
        print(f"[kelly] Fonte usada: {src} ({len(df)} linhas)")

    picks = []
    for _, row in df.iterrows():
        best = best_kelly_for_row(row, args.bankroll, args.kelly_fraction, args.kelly_cap)
        if best is None:
            continue
        picks.append({
            "match_id": row["match_id"],
            "team_home": row["team_home"],
            "team_away": row["team_away"],
            "selection": best["selection"],
            "prob": best["prob"],
            "odds": best["odds"],
            "kelly_fraction": best["kelly_fraction"],
            "stake": best["stake"],
            "edge": best["edge"],
            "source": src,
        })

    if not picks:
        die("Nenhuma aposta elegível (provável ausência de odds_* reais ou probs inválidas).")

    out = pd.DataFrame(picks)
    if (out["stake"] <= 0).all():
        print("[kelly][AVISO] Nenhuma stake positiva calculada (pode faltar odds_* reais).", file=sys.stderr)

    # ordena
    if (out["stake"] > 0).any():
        out = out.sort_values(["stake", "edge"], ascending=[False, False])
    else:
        out = out.sort_values(["prob"], ascending=False)

    # top N
    if args.top_n and args.top_n > 0:
        out = out.head(args.top_n).copy()

    # arredonda stake
    if args.round_to and args.round_to > 0:
        def _round(x):
            return (math.floor(x / args.round_to + 0.5) * args.round_to) if x > 0 else 0
        out["stake"] = out["stake"].apply(_round)

    out_path = os.path.join(args.rodada, "kelly_stakes.csv")
    out.to_csv(out_path, index=False)

    if args.debug:
        print(f"[kelly] config: bankroll={args.bankroll}, frac={args.kelly_fraction}, cap={args.kelly_cap}, top_n={args.top_n}, round_to={args.round_to}")
        print(f"[kelly] gravado: {out_path} ({len(out)} linhas)")
        print(out.head(20).to_string(index=False))

if __name__ == "__main__":
    main()