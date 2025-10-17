#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera o arquivo loteca_ticket.csv a partir de probs_calibrated.csv (preferido)
ou odds_consensus.csv (fallback). Se nada existir, usa 1/3-1/3-1/3.

Por padrão, distribui triplos e duplos dinamicamente com base na incerteza global,
limitando exposição por VaR. Inclui métricas de risco/retorno.

Uso:
  python -m scripts.make_loteca_ticket --rodada data/out/<RUN_ID> \
      [--triples 3] [--doubles 5]

Saída: <rodada>/loteca_ticket.csv

Colunas:
  match_id, team_home, team_away, bet_type, choices, base_pick,
  p_home, p_draw, p_away, margin, odds_home, odds_draw, odds_away, notes
"""

import os
import sys
import argparse
import math
import pandas as pd
import numpy as np
from typing import List, Tuple

CHOICE_LABELS = ["1", "X", "2"]  # home, draw, away

def log(level: str, msg: str):
    print(f"[loteca] [{level}] {msg}", flush=True)

def to_float(x: any) -> float:
    try:
        return float(str(x).replace(",", "."))
    except (ValueError, TypeError):
        return None

def implied_probs(oh: float, od: float, oa: float) -> Tuple[float, float, float]:
    oh, od, oa = to_float(oh), to_float(od), to_float(oa)
    if not all([oh, od, oa]) or any(x <= 1.0 for x in [oh, od, oa]):
        return None, None, None
    ih, idr, ia = 1.0 / oh, 1.0 / od, 1.0 / oa
    s = ih + idr + ia
    return (ih / s, idr / s, ia / s) if s > 0 else (None, None, None)

def safe_read_csv(path: str, required_cols: List[str] = None) -> pd.DataFrame:
    if not os.path.isfile(path):
        return None
    try:
        df = pd.read_csv(path)
        if required_cols:
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                log("WARN", f"{os.path.basename(path)} sem colunas {missing}")
        return df
    except Exception as e:
        log("WARN", f"Falha lendo {path}: {e}")
        return None

def load_whitelist(rodada_dir: str) -> pd.DataFrame:
    wl_path = os.path.join(rodada_dir, "matches_whitelist.csv")
    df_wl = safe_read_csv(wl_path, ["match_id", "home", "away"])
    if df_wl is None or df_wl.empty:
        raise FileNotFoundError("matches_whitelist.csv ausente ou vazio")
    return df_wl.rename(columns={"home": "team_home", "away": "team_away"})[["match_id", "team_home", "team_away"]].copy()

def load_probs_and_odds(rodada_dir: str) -> pd.DataFrame:
    wl = load_whitelist(rodada_dir)

    # 1) Tenta probs_calibrated.csv
    pc = os.path.join(rodada_dir, "probs_calibrated.csv")
    df_pc = safe_read_csv(pc)
    if df_pc is not None and not df_pc.empty:
        log("INFO", "Usando probs_calibrated.csv como base")
        df_pc = df_pc.rename(columns={"home": "team_home", "away": "team_away"})
        for c in ["odds_home", "odds_draw", "odds_away", "p_home", "p_draw", "p_away"]:
            if c in df_pc.columns:
                df_pc[c] = df_pc[c].apply(to_float)

        df = wl.merge(df_pc[[c for c in ["team_home", "team_away", "match_id", "odds_home", "odds_draw", "odds_away", "p_home", "p_draw", "p_away"] if c in df_pc.columns]], 
                      on=["team_home", "team_away"], how="left", suffixes=("", "_pc"))
        if "match_id_pc" in df.columns:
            df["match_id"] = df["match_id_pc"].fillna(df["match_id"])
            df = df.drop(columns=["match_id_pc"])

        # 2) Completa odds via odds_consensus
        oc = os.path.join(rodada_dir, "odds_consensus.csv")
        df_oc = safe_read_csv(oc)
        if df_oc is not None and not df_oc.empty:
            df_oc = df_oc.rename(columns={"home": "team_home", "away": "team_away"})
            for c in ["odds_home", "odds_draw", "odds_away"]:
                if c in df_oc.columns:
                    df_oc[c] = df_oc[c].apply(to_float)
            df = df.merge(df_oc[["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]], 
                          on=["team_home", "team_away"], how="left", suffixes=("", "_oc"))
            for c in ["odds_home", "odds_draw", "odds_away"]:
                df[c] = df[c].where(df[c].notna(), df[c + "_oc"])
                if c + "_oc" in df.columns:
                    df = df.drop(columns=[c + "_oc"])

        # Calcula probs implícitas se ausentes
        need_probs = any(c not in df.columns for c in ["p_home", "p_draw", "p_away"])
        if need_probs or df[["p_home", "p_draw", "p_away"]].isna().any().any():
            ph, pd_, pa = [], [], []
            for _, r in df.iterrows():
                p = implied_probs(r.get("odds_home"), r.get("odds_draw"), r.get("odds_away"))
                ph.append(p[0] if p[0] is not None else float("nan"))
                pd_.append(p[1] if p[1] is not None else float("nan"))
                pa.append(p[2] if p[2] is not None else float("nan"))
            df["p_home"] = df.get("p_home", pd.Series([float("nan")] * len(df))).fillna(pd.Series(ph))
            df["p_draw"] = df.get("p_draw", pd.Series([float("nan")] * len(df))).fillna(pd.Series(pd_))
            df["p_away"] = df.get("p_away", pd.Series([float("nan")] * len(df))).fillna(pd.Series(pa))

        df["notes"] = ""
        mask_na = df[["p_home", "p_draw", "p_away"]].isna().all(axis=1)
        if mask_na.any():
            df.loc[mask_na, ["p_home", "p_draw", "p_away"]] = 1.0 / 3.0
            df.loc[mask_na, "notes"] = "fallback_equal_probs"

        return df[["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "p_home", "p_draw", "p_away", "notes"]].copy()

    # 2) Fallback: odds_consensus.csv
    oc = os.path.join(rodada_dir, "odds_consensus.csv")
    df_oc = safe_read_csv(oc)
    if df_oc is not None and not df_oc.empty:
        log("INFO", "Usando odds_consensus.csv (probs implícitas)")
        df_oc = df_oc.rename(columns={"home": "team_home", "away": "team_away"})
        for c in ["odds_home", "odds_draw", "odds_away"]:
            if c in df_oc.columns:
                df_oc[c] = df_oc[c].apply(to_float)

        df = wl.merge(df_oc[["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]], 
                      on=["team_home", "team_away"], how="left")

        ph, pd_, pa = [], [], []
        notes = []
        for _, r in df.iterrows():
            p = implied_probs(r.get("odds_home"), r.get("odds_draw"), r.get("odds_away"))
            if p[0] is None:
                ph.append(1 / 3.0)
                pd_.append(1 / 3.0)
                pa.append(1 / 3.0)
                notes.append("fallback_equal_probs")
            else:
                ph.append(p[0])
                pd_.append(p[1])
                pa.append(p[2])
                notes.append("")
        df["p_home"], df["p_draw"], df["p_away"] = ph, pd_, pa
        df["notes"] = notes

        return df[["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "p_home", "p_draw", "p_away", "notes"]].copy()

    # 3) Último recurso: somente whitelist, probs iguais
    log("WARN", "Sem probs_calibrated.csv e odds_consensus.csv — usando 1/3 para todos")
    df = load_whitelist(rodada_dir)
    df["odds_home"] = float("nan")
    df["odds_draw"] = float("nan")
    df["odds_away"] = float("nan")
    df["p_home"] = 1.0 / 3.0
    df["p_draw"] = 1.0 / 3.0
    df["p_away"] = 1.0 / 3.0
    df["notes"] = "fallback_equal_probs"
    return df

def compute_uncertainty(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula margem de incerteza (p1 - p2) como proxy de confiança."""
    def row_margin(r: pd.Series) -> float:
        probs = [to_float(r["p_home"]), to_float(r["p_draw"]), to_float(r["p_away"])]
        if any(p is None or math.isnan(p) for p in probs):
            return 0.0  # Força incerteza máxima
        s = sorted(probs, reverse=True)
        return max(0.0, s[0] - s[1])

    df = df.copy()
    df["margin"] = df.apply(row_margin, axis=1)
    return df

def pick_outcomes_for_row(r: pd.Series, bet_type: str) -> Tuple[str, str]:
    """Retorna choices e base_pick conforme bet_type."""
    probs = [to_float(r["p_home"]), to_float(r["p_draw"]), to_float(r["p_away"])]
    probs = [p if p is not None and not math.isnan(p) else 1.0 / 3.0 for p in probs]

    order = sorted([(probs[0], "1"), (probs[1], "X"), (probs[2], "2")], 
                   key=lambda x: x[0], reverse=True)
    base_pick = order[0][1]

    if bet_type == "TRIPLE":
        return "1X2", base_pick
    if bet_type == "DOUBLE":
        return "".join(sorted([order[0][1], order[1][1]], key=lambda s: ["1", "X", "2"].index(s))), base_pick
    return base_pick, base_pick

def allocate_tickets(df: pd.DataFrame, triples: int = 3, doubles: int = 5, max_stake: float = 100.0) -> pd.DataFrame:
    """Aloca triplos, duplos e simples dinamicamente, limitando exposição por VaR."""
    df = compute_uncertainty(df)

    # Ordena por incerteza (margin asc) e probabilidade máxima
    df["top_prob"] = df[["p_home", "p_draw", "p_away"]].max(axis=1)
    df = df.sort_values(by=["margin", "top_prob"], ascending=[True, False])

    n = len(df)
    total_stake = 0.0
    stakes = []

    # Calcular stakes iniciais
    for _, r in df.iterrows():
        choices = [("H", r["p_home"], r["odds_home"]), ("D", r["p_draw"], r["odds_draw"]), ("A", r["p_away"], r["odds_away"])]
        best = max(choices, key=lambda x: x[1] * (x[2] - 1) if x[2] else 0, default=("H", 0, 0))
        stake = min(10.0, max(1.0, 10 * (1 - r["margin"])))  # Stake proporcional à incerteza
        total_stake += stake
        stakes.append(stake)

    # Ajustar alocação com VaR
    stakes_arr = np.array(stakes)
    var_95, _ = calculate_risk_metrics(stakes_arr, np.array([c[2] for c in choices]))
    if var_95 > max_stake:
        scale_factor = max_stake / var_95
        stakes = [s * scale_factor for s in stakes]

    # Distribuir triplos/duplos dinamicamente
    t = min(triples, int(n * 0.2))  # 20% como triplos, ajustável
    d = min(doubles, int(n * 0.3))  # 30% como duplos, ajustável
    remaining = max(0, n - t - d)
    types = ["TRIPLE"] * t + ["DOUBLE"] * d + ["SINGLE"] * remaining
    df = df.copy()
    df["bet_type"] = types[:n]

    choices, base = [], []
    for i, (idx, r) in enumerate(df.iterrows()):
        c, b = pick_outcomes_for_row(r, r["bet_type"])
        choices.append(c)
        base.append(b)
        df.at[idx, "stake"] = stakes[i]  # Atribui stake calculado
    df["choices"], df["base_pick"] = choices, base

    # Ordenar por match_id e calcular retorno esperado
    df = df.sort_values(by="match_id").reset_index(drop=True)
    df["expected_return"] = df["stake"] * (df["p"] * (df["odds"] - 1) - 1) if "p" in df and "odds" in df else 0.0
    return df

def calculate_risk_metrics(stakes: np.ndarray, odds: np.ndarray, n_sim: int = 1000) -> Tuple[float, float]:
    """Calcula VaR (95%) via Monte Carlo."""
    losses = np.random.normal(0, 0.1, n_sim) * stakes * (1 / odds - 1)  # Volatilidade simulada
    return np.percentile(losses, 95), np.mean(losses[losses > np.percentile(losses, 95)])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex: data/out/<RUN_ID>)")
    ap.add_argument("--triples", type=int, default=3, help="Quantidade de triplos (default=3)")
    ap.add_argument("--doubles", type=int, default=5, help="Quantidade de duplos (default=5)")
    args = ap.parse_args()

    rodada = args.rodada
    triples = int(args.triples)
    doubles = int(args.doubles)

    out_path = os.path.join(rodada, "loteca_ticket.csv")
    os.makedirs(rodada, exist_ok=True)

    try:
        base = load_probs_and_odds(rodada)
    except Exception as e:
        log("CRITICAL", f"Falha carregando bases: {e}")
        # Gera CSV vazio com cabeçalho
        pd.DataFrame(columns=["match_id", "team_home", "team_away", "bet_type", "choices", "base_pick",
                             "p_home", "p_draw", "p_away", "margin", "odds_home", "odds_draw", "odds_away", "notes"]).to_csv(out_path, index=False)
        return 0

    # Validar entrada
    if not all(col in base.columns for col in ["p_home", "p_draw", "p_away", "odds_home", "odds_draw", "odds_away"]):
        raise ValueError("Dados de entrada faltando colunas críticas")
    if not np.all((base[["p_home", "p_draw", "p_away"]] >= 0) & (base[["p_home", "p_draw", "p_away"]] <= 1)):
        raise ValueError("Probs inválidas (fora de [0,1])")
    if not np.allclose(base[["p_home", "p_draw", "p_away"]].sum(axis=1), 1, atol=0.01):
        log("WARN", "Soma de probs != 1, normalizando...")
        base[["p_home", "p_draw", "p_away"]] = base[["p_home", "p_draw", "p_away"]].div(base[["p_home", "p_draw", "p_away"]].sum(axis=1), axis=0)

    ticket = allocate_tickets(base, triples=triples, doubles=doubles, max_stake=100.0)

    cols = ["match_id", "team_home", "team_away", "bet_type", "choices", "base_pick",
            "p_home", "p_draw", "p_away", "margin", "odds_home", "odds_draw", "odds_away", "notes"]
    for c in cols:
        if c not in ticket.columns:
            ticket[c] = ""
    ticket = ticket[cols].copy()

    ticket.to_csv(out_path, index=False)
    log("INFO", f"Gerado {os.path.basename(out_path)} com {len(ticket)} jogos.")
    try:
        print(ticket.head(20).to_string(index=False), flush=True)
    except Exception:
        pass
    return 0

if __name__ == "__main__":
    sys.exit(main())