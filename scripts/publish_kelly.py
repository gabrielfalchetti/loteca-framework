#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
from typing import Optional, Tuple
import pandas as pd
import numpy as np

def log(msg: str):
    print(f"[kelly] {msg}", flush=True)

def resolve_out_dir(rodada: str) -> str:
    """Se for caminho (começa com data/ ou contém '/'), usa como está; senão, vira data/out/<rodada>."""
    if not rodada or str(rodada).strip() == "":
        raise ValueError("valor vazio para --rodada")
    r = rodada.strip()
    if r.startswith("data/") or (os.sep in r):
        return r
    return os.path.join("data", "out", r)

def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name, "")
    try:
        return float(v) if v != "" else default
    except Exception:
        return default

def _load_csv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        log(f"AVISO: falha ao ler {path}: {e}")
        return None

def pick_odds_file(out_dir: str, debug: bool=False) -> Tuple[str, Optional[pd.DataFrame]]:
    cand = [
        os.path.join(out_dir, "odds_consensus.csv"),
        os.path.join(out_dir, "odds_theoddsapi.csv"),
        os.path.join(out_dir, "odds_apifootball.csv"),
    ]
    for p in cand:
        df = _load_csv(p)
        if df is not None and len(df) > 0:
            if debug: log(f"odds: usando {os.path.basename(p)} ({len(df)} linhas)")
            return p, df
        else:
            if debug: log(f"odds: {p} ausente ou vazio")
    return "", None

def ensure_odds_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # match_key
    if "match_key" not in df.columns:
        th = df.get("team_home")
        ta = df.get("team_away")
        if th is not None and ta is not None:
            df["match_key"] = (
                th.astype(str).str.strip().str.lower()
                + "__vs__"
                + ta.astype(str).str.strip().str.lower()
            )
        else:
            df["match_key"] = np.nan
    # teams
    for c in ["team_home", "team_away"]:
        if c not in df.columns:
            df[c] = np.nan
    # odds
    for c in ["odds_home", "odds_draw", "odds_away"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]]

def implied_probs(df: pd.DataFrame) -> pd.DataFrame:
    """Probabilidades implícitas normalizadas por linha (sem overround explícito)."""
    df = df.copy()
    for c in ["odds_home","odds_draw","odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    inv_home = 1.0 / df["odds_home"]
    inv_draw = 1.0 / df["odds_draw"]
    inv_away = 1.0 / df["odds_away"]
    inv_home = inv_home.replace([np.inf, -np.inf], np.nan)
    inv_draw = inv_draw.replace([np.inf, -np.inf], np.nan)
    inv_away = inv_away.replace([np.inf, -np.inf], np.nan)
    s = inv_home.fillna(0) + inv_draw.fillna(0) + inv_away.fillna(0)
    s = s.replace(0, np.nan)
    df["prob_home"] = inv_home / s
    df["prob_draw"] = inv_draw / s
    df["prob_away"] = inv_away / s
    return df

def load_predictions(out_dir: str) -> Optional[pd.DataFrame]:
    path = os.path.join(out_dir, "predictions_market.csv")
    df = _load_csv(path)
    if df is None or len(df) == 0:
        return None
    needed = {"match_key","team_home","team_away","pred","pred_conf"}
    if not needed.issubset(set(df.columns)):
        return None
    # se não houver prob_* e odds_*, ainda conseguimos usar pred/pred_conf
    return df

def derive_pick_from_probs(row: pd.Series) -> Tuple[str, float, float]:
    """
    Retorna (pick, prob_escolhida, odds_escolhida)
    pick ∈ {"HOME","DRAW","AWAY"}
    """
    probs = {
        "HOME": row.get("prob_home", np.nan),
        "DRAW": row.get("prob_draw", np.nan),
        "AWAY": row.get("prob_away", np.nan),
    }
    pick = max(probs, key=lambda k: (probs[k] if pd.notna(probs[k]) else -1))
    prob = float(probs[pick]) if pd.notna(probs[pick]) else float("nan")
    odds_map = {
        "HOME": row.get("odds_home", np.nan),
        "DRAW": row.get("odds_draw", np.nan),
        "AWAY": row.get("odds_away", np.nan),
    }
    odds = float(odds_map[pick]) if pd.notna(odds_map[pick]) else float("nan")
    return pick, prob, odds

def kelly_fraction(prob: float, odds: float) -> float:
    """
    Kelly: f* = (b*p - q) / b, com b = odds - 1, q = 1 - p.
    Se odds <= 1 ou resultado negativo -> 0.
    """
    if not (pd.notna(prob) and pd.notna(odds)): return 0.0
    b = odds - 1.0
    if b <= 0: return 0.0
    q = 1.0 - prob
    f = (b * prob - q) / b
    return max(0.0, f)

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def round_to_unit(x: float, unit: float) -> float:
    if unit <= 0: return x
    return round(x / unit) * unit

def ensure_names_from_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    need_fix = False
    for c in ["team_home","team_away"]:
        if c not in df.columns:
            df[c] = ""
            need_fix = True
        else:
            if df[c].isna().any() or (df[c].astype(str)=="").any():
                need_fix = True
    if need_fix:
        homes, aways = [], []
        for k in df.get("match_key", pd.Series([""]*len(df))):
            if isinstance(k, str) and "__vs__" in k:
                a, b = k.split("__vs__", 1)
                homes.append(a.strip().title())
                aways.append(b.strip().title())
            else:
                homes.append("")
                aways.append("")
        df["team_home"] = df["team_home"].astype(str)
        df["team_away"] = df["team_away"].astype(str)
        df.loc[:, "team_home"] = np.where(df["team_home"].astype(str).str.strip()=="", homes, df["team_home"])
        df.loc[:, "team_away"] = np.where(df["team_away"].astype(str).str.strip()=="", aways, df["team_away"])
    return df

def main():
    parser = argparse.ArgumentParser(description="Publica stakes via Kelly em kelly_stakes.csv")
    parser.add_argument("--rodada", required=True, help="Identificador ou caminho de saída (ex: 2025-10-04_1214 ou data/out/XYZ)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    if args.debug:
        log(f"out_dir: {out_dir}")

    # Config (com defaults sensatos)
    bankroll      = _env_float("BANKROLL",       1000.0)
    kelly_frac    = _env_float("KELLY_FRACTION",  0.5)
    kelly_cap     = _env_float("KELLY_CAP",       0.10)
    min_stake     = _env_float("MIN_STAKE",       0.0)
    max_stake     = _env_float("MAX_STAKE",       0.0)  # 0 => sem teto
    round_unit    = _env_float("ROUND_TO",        1.0)
    top_n         = int(_env_float("KELLY_TOP_N", 14))

    log(f"config: " + str({
        "bankroll": bankroll, "kelly_fraction": kelly_frac, "kelly_cap": kelly_cap,
        "min_stake": min_stake, "max_stake": max_stake, "round_to": round_unit, "top_n": top_n
    }))
    log(f"out_dir: {out_dir}")

    # 1) Preferimos predictions_market.csv
    pred = load_predictions(out_dir)

    df_out = None
    if pred is not None:
        # Se existir colunas prob_* e odds_*, usamos diretamente.
        # Caso só tenha pred/pred_conf, não dá pra calcular stake sem odds; então caímos para odds.
        has_probs = {"prob_home","prob_draw","prob_away"}.issubset(set(pred.columns))
        has_odds  = {"odds_home","odds_draw","odds_away"}.issubset(set(pred.columns))
        if has_probs and has_odds:
            work = pred.copy()
        else:
            work = None
    else:
        work = None

    # 2) Se não deu pra usar predictions, caímos para odds
    if work is None:
        odds_path, odds_df = pick_odds_file(out_dir, debug=args.debug)
        if not isinstance(odds_df, pd.DataFrame) or len(odds_df) == 0:
            log("ERRO: nenhuma fonte disponível para calcular stakes (predictions/odds ausentes).")
            # Ainda assim, escrever arquivo vazio com cabeçalho:
            empty = pd.DataFrame(columns=[
                "match_key","team_home","team_away","pick","prob","odds",
                "edge","kelly_fraction","stake"
            ])
            empty.to_csv(os.path.join(out_dir, "kelly_stakes.csv"), index=False)
            sys.exit(2)
        odds_df = ensure_odds_columns(odds_df)
        odds_df = implied_probs(odds_df)
        work = odds_df

    # Garante nomes
    work = ensure_names_from_key(work)

    # Monta pick/prob/odds por linha
    picks, probs, odds_sel = [], [], []
    for _, row in work.iterrows():
        p, pr, od = derive_pick_from_probs(row)
        picks.append(p); probs.append(pr); odds_sel.append(od)

    work = work.assign(pick=picks, prob=probs, odds=odds_sel)

    # Calcula Kelly por linha
    fstars, edges, stakes = [], [], []
    for _, r in work.iterrows():
        prob = float(r["prob"]) if pd.notna(r["prob"]) else 0.0
        odds = float(r["odds"]) if pd.notna(r["odds"]) else 0.0
        b = odds - 1.0
        q = 1.0 - prob
        edge = b * prob - q  # e = b*p - q
        f = kelly_fraction(prob, odds)  # 0..1
        # aplica fração global e cap
        f = min(f, kelly_cap) * kelly_frac
        stake = f * bankroll
        # aplica min/max e arredondamento
        if max_stake > 0.0:
            stake = min(stake, max_stake)
        stake = max(stake, min_stake)
        stake = round_to_unit(stake, round_unit)
        fstars.append(f)
        edges.append(edge)
        stakes.append(stake)

    out = pd.DataFrame({
        "match_key": work["match_key"],
        "team_home": work["team_home"],
        "team_away": work["team_away"],
        "pick": work["pick"],
        "prob": work["prob"],
        "odds": work["odds"],
        "edge": edges,
        "kelly_fraction": fstars,
        "stake": stakes,
    })

    # Ordena por stake desc e aplica top_n (ignorando stakes 0)
    out = out.sort_values(by=["stake","edge"], ascending=[False, False]).reset_index(drop=True)
    if top_n > 0 and len(out) > top_n:
        out = out.iloc[:top_n].copy()

    out_path = os.path.join(out_dir, "kelly_stakes.csv")
    out.to_csv(out_path, index=False)

    if len(out) == 0 or (out["stake"] <= 0).all():
        log("AVISO: sem picks com stake > 0.")
    else:
        log(f"TOP {min(top_n, len(out))} PICKS (amostra):")
        for i, r in out.head(5).iterrows():
            log(f"  {i+1}) {r['team_home']} x {r['team_away']} — {r['pick']} | prob={r['prob']:.3f} odds={r['odds']:.2f} stake={r['stake']:.2f}")

    log(f"OK -> {out_path}")

if __name__ == "__main__":
    main()