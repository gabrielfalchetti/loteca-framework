#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import json
from typing import Tuple, Optional
import numpy as np
import pandas as pd  # <- IMPORTANTE

def choose_odds_file(out_dir: str, debug: bool) -> Tuple[str, str]:
    c1 = os.path.join(out_dir, "odds_consensus.csv")
    c2 = os.path.join(out_dir, "odds_theoddsapi.csv")
    if os.path.isfile(c1):
        try:
            if pd.read_csv(c1).shape[0] > 0:
                if debug: print(f"[kelly] usando odds_consensus.csv")
                return c1, "consensus"
        except Exception as e:
            if debug: print(f"[kelly] aviso: falha ao ler {c1}: {e}")
    if os.path.isfile(c2):
        try:
            if pd.read_csv(c2).shape[0] > 0:
                if debug: print(f"[kelly] usando odds_theoddsapi.csv")
                return c2, "theoddsapi"
        except Exception as e:
            if debug: print(f"[kelly] aviso: falha ao ler {c2}: {e}")
    raise FileNotFoundError("[kelly] nenhuma fonte de odds disponível (odds_consensus.csv nem odds_theoddsapi.csv).")

def ensure_odds_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normaliza nomes para minúsculas e tenta mapear para o padrão
    df = df.rename(columns={c: c.lower() for c in df.columns})
    candidates = [
        {"team_home":"team_home","team_away":"team_away","match_key":"match_key",
         "odds_home":"odds_home","odds_draw":"odds_draw","odds_away":"odds_away"},
        {"team_home":"home","team_away":"away","match_key":"match_key",
         "odds_home":"odds_home","odds_draw":"odds_draw","odds_away":"odds_away"},
        {"team_home":"home_team","team_away":"away_team","match_key":"match_key",
         "odds_home":"odds_home","odds_draw":"odds_draw","odds_away":"odds_away"},
    ]
    for m in candidates:
        if all(v in df.columns for v in m.values()):
            inv = {v:k for k,v in m.items()}
            df = df.rename(columns=inv)
            break

    # cria match_key se necessário
    if "match_key" not in df.columns:
        if "team_home" in df.columns and "team_away" in df.columns:
            df["match_key"] = (
                df["team_home"].astype(str).str.strip().str.lower()
                + "__vs__" +
                df["team_away"].astype(str).str.strip().str.lower()
            )
        else:
            raise ValueError("[kelly] faltam team_home/team_away para compor match_key")

    req = ["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"[kelly] faltam colunas obrigatórias em odds: {missing}")

    for c in ["odds_home","odds_draw","odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["odds_home","odds_draw","odds_away"])
    df = df[(df["odds_home"]>1.0) & (df["odds_draw"]>1.0) & (df["odds_away"]>1.0)].copy()
    if df.empty:
        raise ValueError("[kelly] nenhuma linha de odds válida (>1.0).")
    return df

def load_predictions(out_dir: str, debug: bool) -> Optional[pd.DataFrame]:
    p = os.path.join(out_dir, "predictions_market.csv")
    if not os.path.isfile(p):
        if debug: print("[kelly] sem predictions_market.csv — usando probabilidades implícitas.")
        return None
    try:
        df = pd.read_csv(p)
        df = df.rename(columns={c: c.lower() for c in df.columns})
        # requer match_key e prob_*
        need = ["match_key","prob_home","prob_draw","prob_away"]
        if not all(c in df.columns for c in need):
            if debug: print("[kelly] predictions_market.csv sem colunas prob_* — ignorando.")
            return None
        # mantenha apenas campos úteis
        return df[["match_key","prob_home","prob_draw","prob_away","pred","pred_conf"]]
    except Exception as e:
        if debug: print(f"[kelly] falha ao ler predictions_market.csv: {e}")
        return None

def merge_probs(odds_df: pd.DataFrame, pred_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    df = odds_df.copy()
    if pred_df is not None:
        df = df.merge(pred_df, on="match_key", how="left", suffixes=("",""))
    # se não tem prob_* válidas, calcule implícitas
    if not all(c in df.columns for c in ["prob_home","prob_draw","prob_away"]):
        imp_home = 1.0 / df["odds_home"]
        imp_draw = 1.0 / df["odds_draw"]
        imp_away = 1.0 / df["odds_away"]
        s = imp_home + imp_draw + imp_away
        df["prob_home"] = (imp_home / s).clip(0,1)
        df["prob_draw"] = (imp_draw / s).clip(0,1)
        df["prob_away"] = (imp_away / s).clip(0,1)
    else:
        for c in ["prob_home","prob_draw","prob_away"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").clip(0,1)
    return df

def kelly_fraction(p: float, odds: float) -> float:
    """
    Kelly fracionário básico: f* = (b*p - q)/b, onde b = odds-1, q = 1-p.
    Retorna 0 se negativo.
    """
    b = max(odds - 1.0, 0.0)
    if b <= 0: 
        return 0.0
    q = 1.0 - p
    fstar = (b * p - q) / b
    return max(fstar, 0.0)

def compute_kelly(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    bankroll = float(cfg["bankroll"])
    frac = float(cfg["kelly_fraction"])
    cap = float(cfg["kelly_cap"])          # máx fração do bankroll por aposta
    min_stake = float(cfg["min_stake"])
    max_stake = float(cfg["max_stake"])
    round_to = float(cfg["round_to"])
    top_n = int(cfg["top_n"])

    # cria linhas por outcome
    rows = []
    for _, r in df.iterrows():
        for side in [("HOME","prob_home","odds_home"),
                     ("DRAW","prob_draw","odds_draw"),
                     ("AWAY","prob_away","odds_away")]:
            label, pcol, ocol = side
            p = float(r[pcol])
            odds = float(r[ocol])
            f_star = kelly_fraction(p, odds)           # fração ótima
            f_star = f_star * frac                     # Kelly fracionário
            f_star = min(f_star, cap)                  # cap no fracionamento
            stake = f_star * bankroll                  # valor absoluto
            if max_stake > 0:
                stake = min(stake, max_stake)
            stake = max(stake, min_stake)
            if round_to > 0:
                stake = np.floor(stake / round_to) * round_to

            edge = p - (1.0/odds)
            rows.append({
                "match_key": r["match_key"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "pick": label,
                "prob": p,
                "odds": odds,
                "edge": edge,
                "kelly_frac_raw": kelly_fraction(p, odds),
                "kelly_frac_applied": min(kelly_fraction(p, odds)*frac, cap),
                "stake": stake
            })

    kdf = pd.DataFrame(rows)

    # Escolhe a melhor seleção por partida (maior stake) e ordena por stake desc
    best = (
        kdf.sort_values(["match_key","stake","edge","prob"], ascending=[True,False,False,False])
           .groupby("match_key", as_index=False)
           .first()
           .sort_values("stake", ascending=False)
    )

    # aplica top_n (se > 0)
    if top_n > 0:
        best = best.head(top_n).copy()

    return best

def load_config_from_env(debug: bool) -> dict:
    def _get(name, default):
        v = os.getenv(name)
        if v is None or v == "":
            return default
        return v
    cfg = {
        "bankroll": float(_get("BANKROLL", 1000.0)),
        "kelly_fraction": float(_get("KELLY_FRACTION", 0.5)),
        "kelly_cap": float(_get("KELLY_CAP", 0.10)),
        "min_stake": float(_get("MIN_STAKE", 0.0)),
        "max_stake": float(_get("MAX_STAKE", 0.0)),  # 0 = sem limite
        "round_to": float(_get("ROUND_TO", 1.0)),
        "top_n": int(float(_get("KELLY_TOP_N", 14)))
    }
    if debug:
        print(f"[kelly] config: {json.dumps(cfg)}")
    return cfg

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="OUT_DIR (ex.: data/out/<RUN_ID>)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    debug = args.debug

    if not os.path.isdir(out_dir):
        print(f"[kelly] ERRO: OUT_DIR inexistente: {out_dir}", file=sys.stderr)
        sys.exit(2)

    cfg = load_config_from_env(debug)

    odds_path, source = choose_odds_file(out_dir, debug)
    odds_df = pd.read_csv(odds_path)
    odds_df = ensure_odds_columns(odds_df)

    pred_df = load_predictions(out_dir, debug)
    merged = merge_probs(odds_df, pred_df)

    best = compute_kelly(merged, cfg)

    # Amostra
    if debug:
        print("[kelly] AMOSTRA pós-join (top 5):")
        print(best.head(5).to_dict(orient="records"))

    out_file = os.path.join(out_dir, "kelly_stakes.csv")
    best.to_csv(out_file, index=False, encoding="utf-8")
    if debug:
        print(f"[kelly] OK -> {out_file} ({len(best)} linhas)")

if __name__ == "__main__":
    main()