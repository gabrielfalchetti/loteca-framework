# scripts/publish_kelly.py
# -*- coding: utf-8 -*-
"""
Gera stakes e picks usando Kelly a partir de:
- data/out/<RODADA>/predictions_calibrated.csv (ou predictions_raw.csv como fallback)
- data/out/<RODADA>/odds_consensus.csv

Saídas:
- data/out/<RODADA>/stakes_kelly.csv
- data/out/<RODADA>/picks_final_kelly.csv

Não altera arquivos existentes. Se algum insumo estiver ausente, sai com aviso.
"""

from __future__ import annotations
import os
import sys
import json
import math
from typing import List, Dict, Optional

import pandas as pd

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
OUT_DIR = os.path.join(ROOT, "data", "out")

# Import utilidades Kelly (arquivo entregue junto)
from scripts.kelly import KellyConfig, stake_from_kelly  # type: ignore

def _read_csv_safe(path: str) -> Optional[pd.DataFrame]:
    try:
        if os.path.exists(path):
            return pd.read_csv(path)
        print(f"[kelly] AVISO: não encontrado: {path}")
        return None
    except Exception as e:
        print(f"[kelly] ERRO lendo {path}: {e}")
        return None

def _safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def _first_existing(paths: List[str]) -> Optional[str]:
    for p in paths:
        if os.path.exists(p):
            return p
    return None

def load_predictions(rodada: str) -> Optional[pd.DataFrame]:
    base = os.path.join(OUT_DIR, rodada)
    candidates = [
        os.path.join(base, "predictions_calibrated.csv"),
        os.path.join(base, "predictions_raw.csv"),
    ]
    path = _first_existing(candidates)
    if not path:
        print("[kelly] AVISO: predictions_calibrated.csv e predictions_raw.csv ausentes — nada a fazer.")
        return None
    df = _read_csv_safe(path)
    if df is None or df.empty:
        print(f"[kelly] AVISO: arquivo de predições vazio: {path}")
        return None
    print(f"[kelly] OK: carregado {path} -> {len(df)} linhas")
    return df

def load_odds(rodada: str) -> Optional[pd.DataFrame]:
    base = os.path.join(OUT_DIR, rodada)
    path = os.path.join(base, "odds_consensus.csv")
    df = _read_csv_safe(path)
    if df is None or df.empty:
        print(f"[kelly] AVISO: odds_consensus.csv ausente ou vazio: {path}")
        return None
    print(f"[kelly] OK: carregado {path} -> {len(df)} linhas")
    return df

def infer_columns_pred(df: pd.DataFrame) -> Dict[str, str]:
    """
    Tenta mapear nomes de colunas padrões de probabilidade e chaves do jogo.
    Suporta variações comuns.
    """
    cols = {c.lower(): c for c in df.columns}
    keys = {}

    # IDs/chaves
    for k in ["match_id", "game_id", "id", "fixture_id"]:
        if k in cols:
            keys["match_id"] = cols[k]; break

    # times
    for k in ["home", "home_team", "team_home"]:
        if k in cols:
            keys["home"] = cols[k]; break
    for k in ["away", "away_team", "team_away"]:
        if k in cols:
            keys["away"] = cols[k]; break

    # probabilidades
    p_home = next((cols[k] for k in ["prob_home", "p_home", "home_prob", "prob1", "p1"] if k in cols), None)
    p_draw = next((cols[k] for k in ["prob_draw", "p_draw", "draw_prob", "probx", "px"] if k in cols), None)
    p_away = next((cols[k] for k in ["prob_away", "p_away", "away_prob", "prob2", "p2"] if k in cols), None)

    if not (p_home and p_draw and p_away):
        raise ValueError("Colunas de probabilidade não encontradas (espera-se prob_home/prob_draw/prob_away ou equivalentes).")

    keys["p_home"] = p_home
    keys["p_draw"] = p_draw
    keys["p_away"] = p_away
    return keys

def infer_columns_odds(df: pd.DataFrame) -> Dict[str, str]:
    """
    Tenta mapear nomes de colunas padrões para odds decimais 1X2 e match_id.
    """
    cols = {c.lower(): c for c in df.columns}
    keys = {}

    for k in ["match_id", "game_id", "id", "fixture_id"]:
        if k in cols:
            keys["match_id"] = cols[k]; break
    for k in ["home", "home_team", "team_home"]:
        if k in cols:
            keys["home"] = cols[k]; break
    for k in ["away", "away_team", "team_away"]:
        if k in cols:
            keys["away"] = cols[k]; break

    o_home = next((cols[k] for k in ["home_odds", "odds_home", "o_home", "odd1"] if k in cols), None)
    o_draw = next((cols[k] for k in ["draw_odds", "odds_draw", "o_draw", "odds_x", "oddx"] if k in cols), None)
    o_away = next((cols[k] for k in ["away_odds", "odds_away", "o_away", "odd2"] if k in cols), None)

    if not (o_home and o_draw and o_away):
        raise ValueError("Colunas de odds não encontradas (espera-se home_odds/draw_odds/away_odds ou equivalentes).")

    keys["o_home"] = o_home
    keys["o_draw"] = o_draw
    keys["o_away"] = o_away
    return keys

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Publica stakes/picks via Kelly.")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-09-27_1213")
    ap.add_argument("--bankroll", type=float, default=float(os.getenv("BANKROLL", "1000")))
    ap.add_argument("--kelly-fraction", type=float, default=float(os.getenv("KELLY_FRACTION", "0.5")))
    ap.add_argument("--kelly-cap", type=float, default=float(os.getenv("KELLY_CAP", "0.1")))
    ap.add_argument("--min-stake", type=float, default=float(os.getenv("MIN_STAKE", "0")))
    ap.add_argument("--max-stake", type=float, default=float(os.getenv("MAX_STAKE", "0")))
    ap.add_argument("--round-to", type=float, default=float(os.getenv("ROUND_TO", "1")))
    ap.add_argument("--top-n", type=int, default=int(os.getenv("KELLY_TOP_N", "14")), help="Quantos picks priorizar (default 14).")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    # max_stake: 0 => None
    max_stake = None if args.max_stake <= 0 else args.max_stake

    cfg = KellyConfig(
        bankroll=args.bankroll,
        kelly_fraction=args.kelly_fraction,
        kelly_cap=args.kelly_cap,
        min_stake=args.min_stake,
        max_stake=max_stake,
        round_to=args.round_to,
    )

    # leitura
    preds = load_predictions(args.rodada)
    odds = load_odds(args.rodada)
    if preds is None or odds is None:
        print("[kelly] Nada feito (faltam insumos).")
        sys.exit(0)

    # mapear colunas
    pred_cols = infer_columns_pred(preds)
    odds_cols = infer_columns_odds(odds)

    # join por match_id se houver; senão por (home, away) normalizados
    if "match_id" in pred_cols and "match_id" in odds_cols and pred_cols["match_id"] in preds.columns and odds_cols["match_id"] in odds.columns:
        merged = preds.merge(
            odds,
            left_on=pred_cols["match_id"],
            right_on=odds_cols["match_id"],
            how="inner",
            suffixes=("_pred", "_odds"),
        )
    else:
        # fallback por nomes de times
        merged = preds.merge(
            odds,
            left_on=[pred_cols.get("home"), pred_cols.get("away")],
            right_on=[odds_cols.get("home"), odds_cols.get("away")],
            how="inner",
            suffixes=("_pred", "_odds"),
        )

    if merged.empty:
        print("[kelly] AVISO: merge vazio entre predições e odds — verifique chaves/nomes.")
        sys.exit(0)

    rows = []
    for _, r in merged.iterrows():
        home = str(r.get(pred_cols.get("home"), r.get(odds_cols.get("home"), "")))
        away = str(r.get(pred_cols.get("away"), r.get(odds_cols.get("away"), "")))

        ph = _safe_float(r[pred_cols["p_home"]], 0.0)
        px = _safe_float(r[pred_cols["p_draw"]], 0.0)
        pa = _safe_float(r[pred_cols["p_away"]], 0.0)

        oh = _safe_float(r[odds_cols["o_home"]], None)
        ox = _safe_float(r[odds_cols["o_draw"]], None)
        oa = _safe_float(r[odds_cols["o_away"]], None)

        if oh is None or ox is None or oa is None:
            continue

        # calcular Kelly para cada mercado 1X2
        sh = stake_from_kelly(ph, oh, cfg)
        sx = stake_from_kelly(px, ox, cfg)
        sa = stake_from_kelly(pa, oa, cfg)

        def pack(side, p, o, sdict):
            return {
                "match": f"{home} vs {away}",
                "side": side,                 # H / D / A
                "prob": round(p, 6),
                "odds": round(o, 4),
                "kelly_raw": round(sdict["kelly_raw"], 6),
                "kelly_used": round(sdict["kelly_used"], 6),
                "stake": round(sdict["stake_rounded"], 2),
                "ev_per_unit": round(sdict["ev"], 6),
                "roi": round(sdict["roi"], 6),
            }

        rows.append(pack("H", ph, oh, sh))
        rows.append(pack("D", px, ox, sx))
        rows.append(pack("A", pa, oa, sa))

    df_stakes = pd.DataFrame(rows)
    # ordenar por stake desc, depois EV
    if not df_stakes.empty:
        df_stakes.sort_values(by=["stake", "ev_per_unit"], ascending=[False, False], inplace=True)

    out_dir = os.path.join(OUT_DIR, args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    stakes_path = os.path.join(out_dir, "stakes_kelly.csv")
    df_stakes.to_csv(stakes_path, index=False, encoding="utf-8")
    print(f"[kelly] OK -> {stakes_path} ({len(df_stakes)} linhas)")

    # Picks: top-N com stake > 0
    picks = df_stakes[df_stakes["stake"] > 0].head(args.top_n).copy()
    picks_path = os.path.join(out_dir, "picks_final_kelly.csv")
    picks.to_csv(picks_path, index=False, encoding="utf-8")
    print(f"[kelly] OK -> {picks_path} ({len(picks)} linhas)")

    # Relatinho JSON com config usada (útil p/ artifact)
    report = {
        "bankroll": cfg.bankroll,
        "kelly_fraction": cfg.kelly_fraction,
        "kelly_cap": cfg.kelly_cap,
        "min_stake": cfg.min_stake,
        "max_stake": cfg.max_stake,
        "round_to": cfg.round_to,
        "top_n": args.top_n,
        "input_preds": True,
        "input_odds": True,
    }
    with open(os.path.join(out_dir, "kelly_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()