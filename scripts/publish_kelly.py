#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import math
import glob
import json
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd
import numpy as np

# -----------------------------
# Config
# -----------------------------
@dataclass
class KellyConfig:
    bankroll: float = 1000.0
    kelly_fraction: float = 0.5
    kelly_cap: float = 0.10
    min_stake: float = 0.0
    max_stake: float = 0.0
    round_to: float = 1.0
    top_n: int = 14

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "")
    try:
        return float(v) if v != "" else default
    except Exception:
        return default

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "")
    try:
        return int(v) if v != "" else default
    except Exception:
        return default

def load_cfg() -> KellyConfig:
    return KellyConfig(
        bankroll=env_float("BANKROLL", 1000.0),
        kelly_fraction=env_float("KELLY_FRACTION", 0.5),
        kelly_cap=env_float("KELLY_CAP", 0.10),
        min_stake=env_float("MIN_STAKE", 0.0),
        max_stake=env_float("MAX_STAKE", 0.0),
        round_to=env_float("ROUND_TO", 1.0),
        top_n=env_int("KELLY_TOP_N", 14),
    )

# -----------------------------
# Util
# -----------------------------
def make_match_key(h: str, a: str) -> str:
    return f"{str(h).strip().lower()}__vs__{str(a).strip().lower()}"

def implied_probs_from_odds(oh: Optional[float], od: Optional[float], oa: Optional[float]) -> Tuple[float,float,float]:
    """Sem overround. Se só 1 odd existir, atribui prob 1 para ela."""
    vals = []
    labels = []
    if pd.notna(oh) and oh > 1.0:
        vals.append(1.0/oh); labels.append("H")
    if pd.notna(od) and od > 1.0:
        vals.append(1.0/od); labels.append("D")
    if pd.notna(oa) and oa > 1.0:
        vals.append(1.0/oa); labels.append("A")
    if not vals:
        return 0.0, 0.0, 0.0
    s = sum(vals)
    # se só uma odd válida, vira 1.0
    if len(vals) == 1:
        h=d=a=0.0
        if labels[0] == "H": h=1.0
        if labels[0] == "D": d=1.0
        if labels[0] == "A": a=1.0
        return h,d,a
    # normaliza
    probs = [v/s for v in vals]
    h=d=a=0.0
    idx=0
    for lab in labels:
        if lab == "H": h = probs[idx]
        elif lab == "D": d = probs[idx]
        elif lab == "A": a = probs[idx]
        idx += 1
    return h,d,a

def kelly_fraction(prob: float, odds: float) -> float:
    # Kelly clássico p - (1-p)/(o-1). Se odds <= 1, retorna 0.
    if not (odds and odds > 1.0 and prob and prob > 0.0):
        return 0.0
    b = odds - 1.0
    return prob - (1.0 - prob) / b

def stake_from_kelly(p: float, o: float, cfg: KellyConfig) -> Tuple[float,float,float]:
    k_full = kelly_fraction(p, o)
    if k_full <= 0.0:
        return 0.0, k_full, (p*o - 1.0) if (o and o>0) else -1.0
    k_adj = k_full * cfg.kelly_fraction
    k_adj = min(k_adj, cfg.kelly_cap)
    stake = cfg.bankroll * k_adj
    if cfg.min_stake > 0:
        stake = max(stake, cfg.min_stake)
    if cfg.max_stake > 0:
        stake = min(stake, cfg.max_stake)
    if cfg.round_to and cfg.round_to > 0:
        stake = math.floor(stake / cfg.round_to + 1e-9) * cfg.round_to
    return stake, k_full, (p*o - 1.0)

# -----------------------------
# Main
# -----------------------------
def main():
    debug = os.getenv("DEBUG", "false").lower() == "true"
    rodada = os.getenv("RODADA")
    if not rodada:
        print("[kelly] ERRO: RODADA não definida")
        raise SystemExit(2)

    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)

    cfg = load_cfg()
    print(f"[kelly] config: {json.dumps(cfg.__dict__, ensure_ascii=False)}")
    print(f"[kelly] out_dir: {out_dir}")

    odds_csv = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(odds_csv):
        print(f"[kelly] ERRO: arquivo de odds não encontrado: {odds_csv}")
        raise SystemExit(10)

    # Le odds consensus
    odds = pd.read_csv(odds_csv)
    # Normaliza colunas esperadas
    needed = ["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]
    missing = [c for c in needed if c not in odds.columns]
    if missing:
        print(f"[kelly] ERRO: odds_consensus.csv sem colunas {missing}")
        raise SystemExit(10)

    if debug:
        print(f"[kelly] odds carregadas: {len(odds)}")

    # Tenta achar previsões
    pred_glob = [
        os.path.join(out_dir, "predictions_stacked.csv"),
        os.path.join(out_dir, "predictions_calibrated.csv"),
        os.path.join(out_dir, "predictions_xg_bi.csv"),
        os.path.join(out_dir, "predictions_xg_uni.csv"),
    ]
    pred_csv = None
    for p in pred_glob:
        if os.path.isfile(p):
            pred_csv = p
            break

    if pred_csv is None:
        print("[kelly] AVISO: nenhum arquivo de previsões encontrado.")
        print("[kelly]        Caindo para probabilidades implícitas de mercado (sem overround).")
        work = odds.copy()
        # cria probs implícitas linha a linha
        ph, pd_, pa = [], [], []
        for _, r in work.iterrows():
            h, d, a = implied_probs_from_odds(r.get("odds_home"), r.get("odds_draw"), r.get("odds_away"))
            ph.append(h); pd_.append(d); pa.append(a)
        work["prob_home"] = ph
        work["prob_draw"] = pd_
        work["prob_away"] = pa
    else:
        preds = pd.read_csv(pred_csv)
        # garantir colunas
        needp = ["team_home","team_away","match_key","prob_home","prob_draw","prob_away"]
        missp = [c for c in needp if c not in preds.columns]
        if missp:
            # tenta construir match_key se faltar
            if "match_key" not in preds.columns and {"team_home","team_away"}.issubset(set(preds.columns)):
                preds = preds.copy()
                preds["match_key"] = preds.apply(lambda r: make_match_key(r["team_home"], r["team_away"]), axis=1)
                missp = [c for c in needp if c not in preds.columns]
        if missp:
            print(f"[kelly] AVISO: {os.path.basename(pred_csv)} sem colunas {missp}.")
            print("[kelly]        Caindo para probabilidades implícitas de mercado (sem overround).")
            work = odds.copy()
            ph, pd_, pa = [], [], []
            for _, r in work.iterrows():
                h, d, a = implied_probs_from_odds(r.get("odds_home"), r.get("odds_draw"), r.get("odds_away"))
                ph.append(h); pd_.append(d); pa.append(a)
            work["prob_home"] = ph
            work["prob_draw"] = pd_
            work["prob_away"] = pa
        else:
            work = odds.merge(
                preds[["match_key","prob_home","prob_draw","prob_away"]],
                on="match_key", how="left"
            )
            if work[["prob_home","prob_draw","prob_away"]].isna().all(axis=None):
                print("[kelly] AVISO: join com previsões não trouxe probabilidades.")
                print("[kelly]        Caindo para probabilidades implícitas de mercado (sem overround).")
                work = odds.copy()
                ph, pd_, pa = [], [], []
                for _, r in work.iterrows():
                    h, d, a = implied_probs_from_odds(r.get("odds_home"), r.get("odds_draw"), r.get("odds_away"))
                    ph.append(h); pd_.append(d); pa.append(a)
                work["prob_home"] = ph
                work["prob_draw"] = pd_
                work["prob_away"] = pa

    if debug:
        amostra = work.head(5).to_dict(orient="records")
        print(f"[kelly] AMOSTRA pós-join (top 5): {amostra}")

    # Calcula stakes por seleção com odds não-nan
    rows = []
    for _, r in work.iterrows():
        th, ta = r["team_home"], r["team_away"]
        mk = r["match_key"]
        for sel, pcol, ocol in [
            ("HOME","prob_home","odds_home"),
            ("DRAW","prob_draw","odds_draw"),
            ("AWAY","prob_away","odds_away"),
        ]:
            p = r.get(pcol, np.nan)
            o = r.get(ocol, np.nan)

            if pd.isna(o) or o <= 1.0 or pd.isna(p) or p <= 0.0:
                stake = 0.0; kfull = 0.0; edge = (p*o - 1.0) if (pd.notna(p) and pd.notna(o)) else -1.0
            else:
                stake, kfull, edge = stake_from_kelly(float(p), float(o), cfg)

            rows.append({
                "team_home": th,
                "team_away": ta,
                "match_key": mk,
                "selection": sel,
                "prob": float(p) if pd.notna(p) else 0.0,
                "odds": float(o) if pd.notna(o) else 0.0,
                "kelly_full": float(kfull),
                "stake": float(stake),
                "edge": float(edge)
            })

    df = pd.DataFrame(rows)

    # Ordena por stake desc e limita ao top_n (ignorando stakes zero)
    df_sorted = df.sort_values(["stake","edge","odds"], ascending=[False, False, False])
    top = df_sorted[df_sorted["stake"] > 0].head(cfg.top_n).copy()

    # Salva
    out_csv = os.path.join(out_dir, "kelly_stakes.csv")
    df_sorted.to_csv(out_csv, index=False)
    print(f"[kelly] OK -> {out_csv} ({len(top)} linhas)")

    # imprime top picks para log
    if not top.empty:
        print("[kelly] TOP picks:")
        for i, r in enumerate(top.itertuples(index=False), start=1):
            print(f"[kelly]   #{i}: {r.team_home} x {r.team_away} | {r.selection} | prob={round(r.prob,4)} | odds={r.odds} | kelly={round(r.kelly_full,4)} | stake={r.stake}")
    else:
        print("[kelly] AVISO: sem picks com stake > 0.")

if __name__ == "__main__":
    main()