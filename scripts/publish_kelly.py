#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Calcula stakes via Kelly com base em:
  - {OUT_DIR}/odds_consensus.csv
  - {OUT_DIR}/predictions_market.csv  (se faltar, cai para probs implícitas)

Saída:
  - {OUT_DIR}/kelly_stakes.csv
"""

import os
import sys
import math
import argparse
import pandas as pd

def get_env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)

def die(msg):
    print(f"[kelly] ERRO: {msg}", file=sys.stderr)
    sys.exit(2)

def kelly_fraction(p, b):
    # Kelly: f* = (bp - q) / b, b = odd-1
    q = 1 - p
    if b <= 0: return 0.0
    val = (b * p - q) / b
    return max(0.0, val)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="OUT_DIR")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    bankroll = get_env_float("BANKROLL", 1000.0)
    k_frac   = get_env_float("KELLY_FRACTION", 0.5)
    k_cap    = get_env_float("KELLY_CAP", 0.10)
    min_s    = get_env_float("MIN_STAKE", 0.0)
    max_s    = get_env_float("MAX_STAKE", 0.0)
    round_to = get_env_float("ROUND_TO", 1.0)
    top_n    = int(get_env_float("KELLY_TOP_N", 14))

    odds_p = os.path.join(out_dir, "odds_consensus.csv")
    pred_p = os.path.join(out_dir, "predictions_market.csv")

    if not os.path.isfile(odds_p):
        die(f"{odds_p} ausente")
    df_odds = pd.read_csv(odds_p)

    if os.path.isfile(pred_p):
        df_pred = pd.read_csv(pred_p)
    else:
        print("[kelly] AVISO: nenhum arquivo de previsões encontrado.")
        print("[kelly]        Caindo para probabilidades implícitas de mercado (sem overround).")
        # construir df_pred a partir das odds
        rows = []
        for _, r in df_odds.iterrows():
            try:
                oh, od, oa = float(r["odds_home"]), float(r["odds_draw"]), float(r["odds_away"])
                s = (1/oh + 1/od + 1/oa)
                ph, pd, pa = (1/oh)/s, (1/od)/s, (1/oa)/s
                probs = {"HOME": ph, "DRAW": pd, "AWAY": pa}
                pred = max(probs, key=probs.get)
                rows.append({
                    "match_key": r["match_key"],
                    "team_home": r["team_home"],
                    "team_away": r["team_away"],
                    "prob_home": ph, "prob_draw": pd, "prob_away": pa,
                    "pred": pred, "pred_conf": probs[pred]
                })
            except Exception:
                pass
        df_pred = pd.DataFrame(rows)

    if df_pred.empty:
        die("sem previsões")

    # join
    df = pd.merge(
        df_pred[["match_key","team_home","team_away","prob_home","prob_draw","prob_away","pred","pred_conf"]],
        df_odds[["match_key","odds_home","odds_draw","odds_away"]],
        on="match_key", how="inner"
    )
    if df.empty:
        die("join vazio entre odds e previsões")

    # calcula kelly na odd da predição
    stakes = []
    for _, r in df.iterrows():
        side = r["pred"]
        if side == "HOME":
            p = float(r["prob_home"]); odd = float(r["odds_home"])
        elif side == "DRAW":
            p = float(r["prob_draw"]); odd = float(r["odds_draw"])
        else:
            p = float(r["prob_away"]); odd = float(r["odds_away"])
        b = max(0.0, odd - 1.0)
        k = kelly_fraction(p, b) * k_frac
        k = min(k, k_cap)
        stake = bankroll * k
        if max_s > 0: stake = min(stake, max_s)
        if stake < min_s: stake = 0.0
        if round_to > 0:
            stake = math.floor(stake / round_to + 1e-9) * round_to
        stakes.append({
            "match_key": r["match_key"],
            "team_home": r["team_home"],
            "team_away": r["team_away"],
            "pick": side,
            "pick_odd": odd,
            "pick_prob": p,
            "stake": stake
        })

    df_out = pd.DataFrame(stakes)
    if top_n > 0:
        df_out = df_out.sort_values(by=["stake","pick_prob"], ascending=[False, False]).head(top_n)

    outp = os.path.join(out_dir, "kelly_stakes.csv")
    df_out.to_csv(outp, index=False)

    print(f"[kelly] config: " +
          str({"bankroll": bankroll, "kelly_fraction": k_frac, "kelly_cap": k_cap,
               "min_stake": min_s, "max_stake": max_s, "round_to": round_to, "top_n": top_n}))
    print(f"[kelly] out_dir: {out_dir}")
    print(f"[kelly] odds carregadas: {len(df_odds)}")
    if os.path.isfile(pred_p):
        print(f"[kelly] previsões carregadas: {len(df_pred)}")
    print(f"[kelly] OK -> {outp} ({len(df_out)} linhas)")
    if (df_out["stake"] > 0).sum() == 0:
        print("[kelly] AVISO: sem picks com stake > 0.")

if __name__ == "__main__":
    main()