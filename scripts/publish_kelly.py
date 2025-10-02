#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Publica stakes pelo critério de Kelly.
Aceita consenso gerado com apenas um provedor de odds.

Entrada:
  data/out/<RODADA>/odds_consensus.csv

Saída:
  data/out/<RODADA>/kelly_stakes.csv
"""

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from typing import Optional

import pandas as pd


ODDS_COLS = ["odds_home", "odds_draw", "odds_away"]


def _log(msg: str) -> None:
    print(f"[kelly] {msg}")


@dataclass
class KellyCfg:
    bankroll: float
    kelly_fraction: float
    kelly_cap: float
    min_stake: float
    max_stake: float
    round_to: float
    top_n: int


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def read_consensus(out_dir: str) -> pd.DataFrame:
    p = os.path.join(out_dir, "odds_consensus.csv")
    if not (os.path.isfile(p) and os.path.getsize(p) > 0):
        raise FileNotFoundError(f"arquivo não encontrado: {p}")

    df = pd.read_csv(p)
    df.columns = [c.strip().lower() for c in df.columns]

    required = ["match_key", "team_home", "team_away", *ODDS_COLS]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"coluna obrigatória ausente no consenso: {c}")

    # odds numéricas > 1
    for c in ODDS_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # filtra válidas (pelo menos 2 odds > 1)
    valid = (
        ((df["odds_home"] > 1).astype("Int64").fillna(0))
        + ((df["odds_draw"] > 1).astype("Int64").fillna(0))
        + ((df["odds_away"] > 1).astype("Int64").fillna(0))
    ) >= 2
    df = df[valid].reset_index(drop=True)

    _log(f"consensus lido: {len(df)} linhas")
    if len(df) == 0:
        _log("ERRO: nenhuma linha de odds válida (odds_* > 1.0).")
        sys.exit(10)

    return df


def kelly_fraction(p: float, o: float) -> float:
    """Kelly completo para odds decimais; retorna fração da banca."""
    if not (p and o and p > 0 and o > 1):
        return 0.0
    b = o - 1.0
    q = 1.0 - p
    f = (b * p - q) / b
    return max(f, 0.0)


def stake_from_kelly(p: float, o: float, cfg: KellyCfg) -> tuple[float, float, float]:
    kfull = kelly_fraction(p, o)  # fração "cheia"
    edge = p * o - 1.0
    f = min(kfull * cfg.kelly_fraction, cfg.kelly_cap)
    stake = f * cfg.bankroll
    if cfg.round_to and cfg.round_to > 0:
        stake = math.floor(stake / cfg.round_to + 1e-9) * cfg.round_to
    if cfg.min_stake and stake < cfg.min_stake:
        stake = 0.0
    if cfg.max_stake and cfg.max_stake > 0:
        stake = min(stake, cfg.max_stake)
    return stake, kfull, edge


def compute_uniform_probs(df: pd.DataFrame) -> pd.DataFrame:
    """Sem modelos, usamos probabilidade 'justa' derivada das odds (sem vigorish)."""
    out = df.copy()
    # transforma odds em p_bruta = 1/odds
    for c in ODDS_COLS:
        out[c + "_praw"] = 1.0 / out[c]

    # normaliza removendo vigorish
    s = out[[c + "_praw" for c in ODDS_COLS]].sum(axis=1)
    for c in ODDS_COLS:
        out["prob_" + c.split("_")[1]] = out[c + "_praw"] / s

    return out


def compute_kelly_rows(df: pd.DataFrame, cfg: KellyCfg, debug: bool = False) -> pd.DataFrame:
    work = compute_uniform_probs(df)

    rows = []
    for _, r in work.iterrows():
        # calcula Kelly em todos os mercados disponíveis; escolhe o maior stake
        candidates = []
        for side, odd_col, p_col in [
            ("home", "odds_home", "prob_home"),
            ("draw", "odds_draw", "prob_draw"),
            ("away", "odds_away", "prob_away"),
        ]:
            o = float(r.get(odd_col, float("nan")))
            p = float(r.get(p_col, 0.0))
            if not (o > 1 and p > 0):
                continue
            stake, kfull, edge = stake_from_kelly(p, o, cfg)
            candidates.append((stake, side, o, p, kfull, edge))

        if not candidates:
            # nenhuma odd >1 com prob — pula
            continue

        stake, side, o, p, kfull, edge = max(candidates, key=lambda t: t[0])
        rows.append(
            {
                "match_key": r["match_key"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "best_side": side,
                "odds": round(o, 3),
                "prob": round(p, 5),
                "kelly_full": round(kfull, 6),
                "edge": round(edge, 6),
                "stake": float(stake),
            }
        )

    out = pd.DataFrame(rows)
    if len(out) == 0:
        _log("ERRO: após calcular Kelly nenhuma aposta elegível restou.")
        sys.exit(10)

    # ordena por stake desc e aplica top_n
    out = out.sort_values(["stake", "edge"], ascending=[False, False]).reset_index(drop=True)
    if cfg.top_n and cfg.top_n > 0 and len(out) > cfg.top_n:
        out = out.head(cfg.top_n).reset_index(drop=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    cfg = KellyCfg(
        bankroll=_env_float("BANKROLL", 1000.0),
        kelly_fraction=_env_float("KELLY_FRACTION", 0.5),
        kelly_cap=_env_float("KELLY_CAP", 0.1),
        min_stake=_env_float("MIN_STAKE", 0.0),
        max_stake=_env_float("MAX_STAKE", 0.0),
        round_to=_env_float("ROUND_TO", 1.0),
        top_n=_env_int("KELLY_TOP_N", 14),
    )

    _log("config: " + json.dumps(cfg.__dict__))
    out_dir = os.path.join("data", "out", args.rodada)
    _log(f"out_dir: {out_dir}")

    df_cons = read_consensus(out_dir)
    picks = compute_kelly_rows(df_cons, cfg, debug=args.debug)

    out_path = os.path.join(out_dir, "kelly_stakes.csv")
    picks.to_csv(out_path, index=False)
    _log(f"OK -> {out_path} ({len(picks)} linhas)")


if __name__ == "__main__":
    main()