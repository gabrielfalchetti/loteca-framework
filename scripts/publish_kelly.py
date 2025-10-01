#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
import sys
import math
import json
import argparse
from dataclasses import dataclass
from typing import Optional, List, Tuple

import pandas as pd
import numpy as np


###############################################################################
# Config
###############################################################################

@dataclass
class KellyConfig:
    bankroll: float = 1000.0
    kelly_fraction: float = 0.5
    kelly_cap: float = 0.10         # fração máxima do bankroll em um único pick
    min_stake: float = 0.0
    max_stake: float = 0.0          # 0 => sem teto absoluto por valor
    round_to: float = 1.0           # arredondamento do stake
    top_n: int = 14

def env_bool(name: str, default: bool=False) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if v in ("1","true","yes","y","on"): return True
    if v in ("0","false","no","n","off"): return False
    return default

def load_cfg() -> KellyConfig:
    def f(name, default): 
        try: return float(os.environ.get(name, default))
        except: return float(default)
    def i(name, default):
        try: return int(os.environ.get(name, default))
        except: return int(default)
    cfg = KellyConfig(
        bankroll       = f("BANKROLL", 1000.0),
        kelly_fraction = f("KELLY_FRACTION", 0.5),
        kelly_cap      = f("KELLY_CAP", 0.10),
        min_stake      = f("MIN_STAKE", 0.0),
        max_stake      = f("MAX_STAKE", 0.0),
        round_to       = f("ROUND_TO", 1.0),
        top_n          = i("KELLY_TOP_N", 14),
    )
    print(f"[kelly] config: {json.dumps(cfg.__dict__)}")
    return cfg


###############################################################################
# IO helpers
###############################################################################

def must_file(path: str, what: str):
    if not os.path.exists(path):
        print(f"[kelly] ERRO: arquivo não encontrado: {path} ({what})")
        sys.exit(2)

def read_csv_safe(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[kelly] ERRO: falha ao ler {path}: {e}")
        sys.exit(3)

def out_dir_for(rodada: str) -> str:
    d = os.path.join("data","out",rodada)
    os.makedirs(d, exist_ok=True)
    return d

def in_dir_for(rodada: str) -> str:
    d = os.path.join("data","in",rodada)
    os.makedirs(d, exist_ok=True)
    return d


###############################################################################
# Normalizações
###############################################################################

BASIC_COLS = ["match_key","team_home","team_away"]
ODDS_COLS = ["odds_home","odds_draw","odds_away"]
PROB_COLS = ["prob_home","prob_draw","prob_away"]

def mk_join_key(th: str, ta: str) -> str:
    return f"{str(th).strip().lower()}__vs__{str(ta).strip().lower()}"

def normalize_basic_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # renomes comuns
    ren = {}
    if "home_team" in out.columns: ren["home_team"] = "team_home"
    if "away_team" in out.columns: ren["away_team"] = "team_away"
    out = out.rename(columns=ren)
    for c in ["team_home","team_away"]:
        if c not in out.columns:
            out[c] = np.nan
    # match_key
    if "match_key" not in out.columns or out["match_key"].isna().any():
        out["match_key"] = out.apply(
            lambda r: mk_join_key(r.get("team_home",""), r.get("team_away","")),
            axis=1
        )
    # __join_key
    out["__join_key"] = out.apply(
        lambda r: mk_join_key(r.get("team_home",""), r.get("team_away","")),
        axis=1
    )
    return out

def normalize_odds_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # garantir colunas
    for c in ODDS_COLS:
        if c not in out.columns:
            out[c] = np.nan
    # validar numéricas > 1.0
    for c in ODDS_COLS:
        out[c] = pd.to_numeric(out[c], errors="coerce")
        out.loc[(out[c] <= 1.0) | (~np.isfinite(out[c])), c] = np.nan
    return out

def normalize_prob_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    have_any = False
    for c in PROB_COLS:
        if c in out.columns:
            have_any = True
            out[c] = pd.to_numeric(out[c], errors="coerce")
    if not have_any:
        # cria colunas vazias; podemos preencher com fallback
        for c in PROB_COLS:
            out[c] = np.nan
    # clamp p in [0,1]
    for c in PROB_COLS:
        out.loc[(out[c] < 0) | (~np.isfinite(out[c])), c] = np.nan
        out.loc[(out[c] > 1), c] = 1.0
    return out

def probs_from_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Retira a margem por 1/odds e renormaliza."""
    out = df.copy()
    inv = pd.DataFrame({
        "i_home": 1.0 / out["odds_home"],
        "i_draw": 1.0 / out["odds_draw"],
        "i_away": 1.0 / out["odds_away"],
    })
    inv = inv.replace([np.inf, -np.inf], np.nan)
    s = inv.sum(axis=1)
    # quando faltar alguma odd, evita divisão por zero
    out["prob_home"] = inv["i_home"] / s
    out["prob_draw"] = inv["i_draw"] / s
    out["prob_away"] = inv["i_away"] / s
    # limpa não finitos
    for c in PROB_COLS:
        out.loc[~np.isfinite(out[c]), c] = np.nan
    return out


###############################################################################
# Kelly
###############################################################################

def kelly_fraction(p: float, o: float) -> float:
    """
    Kelly teórica: f* = (p*(o-1) - (1-p)) / (o-1).
    Retorna 0 se o <= 1, p inválido ou resultado <= 0 (edge negativo).
    """
    if not np.isfinite(p) or not np.isfinite(o) or o <= 1.0:
        return 0.0
    b = o - 1.0
    f = (p * b - (1.0 - p)) / b
    if not np.isfinite(f) or f <= 0:
        return 0.0
    return float(f)

def stake_from_kelly(p: float, o: float, cfg: KellyConfig) -> Tuple[float, float, float]:
    """
    Retorna (stake, kelly_full, edge).
    - kelly_full = f* sem fração do cfg (0..1)
    - stake já com fração (kelly_fraction), cap e arredondamento aplicados.
    """
    f_full = kelly_fraction(p, o)  # 0..1
    if f_full <= 0:
        return 0.0, 0.0, (p*o - 1.0) if np.isfinite(p) and np.isfinite(o) else float("nan")
    # aplica fração e cap
    f = min(f_full * cfg.kelly_fraction, cfg.kelly_cap)
    if f <= 0 or not np.isfinite(f):
        return 0.0, f_full, (p*o - 1.0)
    stake = cfg.bankroll * f
    # teto absoluto opcional
    if cfg.max_stake and cfg.max_stake > 0:
        stake = min(stake, cfg.max_stake)
    # piso
    if stake < cfg.min_stake:
        return 0.0, f_full, (p*o - 1.0)
    # arredondamento seguro
    if cfg.round_to and cfg.round_to > 0 and np.isfinite(stake):
        stake = math.floor(stake / cfg.round_to + 1e-9) * cfg.round_to
    # evitar -0.0
    if not np.isfinite(stake) or stake <= 0:
        return 0.0, f_full, (p*o - 1.0)
    edge = p * o - 1.0
    return float(stake), float(f_full), float(edge)


###############################################################################
# Merge & Cálculo
###############################################################################

def load_consensus(out_dir: str, debug: bool=False) -> pd.DataFrame:
    p = os.path.join(out_dir, "odds_consensus.csv")
    must_file(p, "odds_consensus")
    df = read_csv_safe(p)
    df = normalize_basic_cols(df)
    df = normalize_odds_cols(df)
    if debug:
        print(f"[kelly] consensus lido: {len(df)} linhas")
    return df

def try_load_predictions(out_dir: str, debug: bool=False) -> Optional[pd.DataFrame]:
    # ordem de preferência
    candidates = [
        "predictions_calibrated.csv",
        "predictions_stacked.csv",
        "predictions_xg_bi.csv",
        "predictions_xg_uni.csv",
    ]
    for name in candidates:
        p = os.path.join(out_dir, name)
        if os.path.exists(p):
            df = read_csv_safe(p)
            df = normalize_basic_cols(df)
            df = normalize_prob_cols(df)
            if debug:
                print(f"[kelly] predictions de {name}: {len(df)} linhas")
            return df
    if debug:
        print("[kelly] AVISO: nenhum arquivo de probabilidades encontrado.")
    return None

def build_work_df(cons: pd.DataFrame, preds: Optional[pd.DataFrame], allow_implied: bool, debug: bool=False) -> pd.DataFrame:
    base = cons.copy()
    # garantias
    base = normalize_basic_cols(base)
    base = normalize_odds_cols(base)

    if preds is not None:
        m = pd.merge(
            base,
            preds[["__join_key","team_home","team_away"] + PROB_COLS],
            on="__join_key", how="left", suffixes=("", "_p")
        )
        # usa colunas de prob (sem _p) preenchendo pelas _p caso vazio
        for c in PROB_COLS:
            if c not in m.columns:
                m[c] = np.nan
            if f"{c}_p" in m.columns:
                m[c] = m[c].combine_first(m[f"{c}_p"])
                m.drop(columns=[f"{c}_p"], inplace=True)
        work = m
    else:
        work = base

    # se não há probs e allow_implied, gera a partir das odds
    if allow_implied and work[PROB_COLS].isna().all(axis=None):
        if debug:
            print("[kelly] fallback: gerando probabilidades implícitas a partir das odds.")
        work = probs_from_odds(work)

    # valida odds e probs linha a linha
    def valid_row(r) -> bool:
        ok_odds = all(np.isfinite(r[c]) and r[c] > 1.0 for c in ODDS_COLS)
        ok_probs = all(np.isfinite(r[c]) and r[c] >= 0 for c in PROB_COLS)
        if not ok_odds or not ok_probs:
            return False
        # normaliza somatório de probs se precisar
        s = r["prob_home"] + r["prob_draw"] + r["prob_away"]
        return np.isfinite(s) and s > 0.0

    before = len(work)
    work = work[[*BASIC_COLS, *ODDS_COLS, *PROB_COLS, "__join_key"]]
    work = work[work.apply(valid_row, axis=1)].copy()
    # renormaliza probs para somarem 1
    s = work["prob_home"] + work["prob_draw"] + work["prob_away"]
    for c in PROB_COLS:
        work[c] = work[c] / s
    if debug:
        print(f"[kelly] linhas válidas após filtros: {len(work)}/{before}")
    return work

def compute_kelly_rows(df: pd.DataFrame, cfg: KellyConfig, debug: bool=False) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        # HOME
        stake_h, kfull_h, edge_h = stake_from_kelly(r["prob_home"], r["odds_home"], cfg)
        if stake_h > 0:
            rows.append({
                "match_key": r["match_key"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "pick": "HOME",
                "prob": r["prob_home"],
                "odds": r["odds_home"],
                "edge": edge_h,
                "kelly_full": kfull_h,
                "stake": stake_h,
            })
        # DRAW
        stake_d, kfull_d, edge_d = stake_from_kelly(r["prob_draw"], r["odds_draw"], cfg)
        if stake_d > 0:
            rows.append({
                "match_key": r["match_key"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "pick": "DRAW",
                "prob": r["prob_draw"],
                "odds": r["odds_draw"],
                "edge": edge_d,
                "kelly_full": kfull_d,
                "stake": stake_d,
            })
        # AWAY
        stake_a, kfull_a, edge_a = stake_from_kelly(r["prob_away"], r["odds_away"], cfg)
        if stake_a > 0:
            rows.append({
                "match_key": r["match_key"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "pick": "AWAY",
                "prob": r["prob_away"],
                "odds": r["odds_away"],
                "edge": edge_a,
                "kelly_full": kfull_a,
                "stake": stake_a,
            })

    if not rows:
        return pd.DataFrame(columns=[
            "match_key","team_home","team_away","pick","prob","odds","edge","kelly_full","stake"
        ])

    out = pd.DataFrame(rows)
    # ordena por edge desc e stake desc
    out = out.sort_values(by=["edge","stake"], ascending=[False, False]).reset_index(drop=True)
    return out


###############################################################################
# Main
###############################################################################

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    cfg = load_cfg()
    out_dir = out_dir_for(args.rodada)
    print(f"[kelly] out_dir: {out_dir}")

    require_odds  = env_bool("REQUIRE_ODDS", True)   # por padrão exigimos odds reais
    require_probs = env_bool("REQUIRE_PROBS", False) # por padrão aceitamos fallback
    debug = args.debug or env_bool("DEBUG", False)

    # 1) Lê consenso de odds
    cons = load_consensus(out_dir, debug=debug)
    cons = cons.dropna(subset=["odds_home","odds_draw","odds_away"], how="any")
    if cons.empty:
        msg = "[kelly] ERRO: nenhuma linha de odds válida (odds_* > 1.0)."
        if require_odds:
            print(msg)
            sys.exit(10)
        else:
            print("[kelly] AVISO: sem odds válidas; gerando kelly_stakes.csv vazio.")
            p_out = os.path.join(out_dir, "kelly_stakes.csv")
            pd.DataFrame(columns=["match_key","team_home","team_away","pick","prob","odds","edge","kelly_full","stake"]).to_csv(p_out, index=False)
            return

    # 2) Lê probabilidades (modelo) se houver
    preds = try_load_predictions(out_dir, debug=debug)

    if require_probs and (preds is None or preds[PROB_COLS].isna().all(axis=None)):
        print("[kelly] ERRO: REQUIRE_PROBS=true e nenhuma probabilidade de modelo foi encontrada.")
        sys.exit(11)

    # 3) Constrói DF de trabalho (com fallback para probs implícitas se permitido)
    df_work = build_work_df(cons, preds, allow_implied=(not require_probs), debug=debug)

    # Segurança extra: garantir que odds/probs estejam válidas
    if df_work.empty:
        msg = "[kelly] ERRO: após normalização, não há linhas com odds/prob válidas."
        if require_odds:
            print(msg)
            sys.exit(12)
        else:
            print("[kelly] AVISO: sem linhas válidas; gerando CSV vazio.")
            p_out = os.path.join(out_dir, "kelly_stakes.csv")
            pd.DataFrame(columns=["match_key","team_home","team_away","pick","prob","odds","edge","kelly_full","stake"]).to_csv(p_out, index=False)
            return

    # 4) Calcula Kelly
    picks = compute_kelly_rows(df_work, cfg, debug=debug)

    # 5) Top N e salva
    if picks.empty:
        print("[kelly] AVISO: nenhuma aposta com edge/kelly > 0. Gerando CSV vazio.")
        p_out = os.path.join(out_dir, "kelly_stakes.csv")
        picks.to_csv(p_out, index=False)
        return

    if cfg.top_n and cfg.top_n > 0:
        picks = picks.head(cfg.top_n).copy()

    p_out = os.path.join(out_dir, "kelly_stakes.csv")
    picks.to_csv(p_out, index=False)
    if debug:
        print(picks.to_string(index=False))
    print(f"[kelly] OK -> {p_out} ({len(picks)} linhas)")

if __name__ == "__main__":
    main()
