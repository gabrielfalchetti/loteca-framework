#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Calcula stakes pelo critério de Kelly a partir de odds_consensus.csv.
Se não houver predictions_*.csv com prob_home/prob_draw/prob_away, cai no
fallback de probabilidades implícitas (corrigindo overround) — MAS só
para partidas com ≥2 odds válidas.

Args:
  --rodada RODADA
  --debug
Usa ENV:
  BANKROLL, KELLY_FRACTION, KELLY_CAP, MIN_STAKE, MAX_STAKE, ROUND_TO, KELLY_TOP_N
"""

import os, sys, argparse, math, glob
import pandas as pd

def log(msg): print(f"[kelly] {msg}")
def ddbg(debug, msg):
    if debug: print(f"[kelly][DEBUG] {msg}")

def get_env_float(name, default):
    v = os.getenv(name)
    try: return float(v) if v is not None and v != "" else float(default)
    except: return float(default)

def join_key(h,a): return f"{h}__vs__{a}".lower()

def implied_from_odds(row):
    parts = []
    labs = []
    for lab in ("home","draw","away"):
        o = row.get(f"odds_{lab}")
        if isinstance(o,(int,float)) and o>1.0 and math.isfinite(o):
            p = 1.0/o
            parts.append(p)
            labs.append(lab)
    if len(parts) < 2:
        return float("nan"), float("nan"), float("nan")
    s = sum(parts)
    probs = [p/s for p in parts]
    out = {"home":0.0,"draw":0.0,"away":0.0}
    for lab,p in zip(labs, probs):
        out[lab] = p
    return out["home"], out["draw"], out["away"]

def kelly_fraction(p, o):
    # Kelly fracionário para odds decimais: f* = (o*p - 1)/(o - 1)
    # se edge <= 0 → 0
    if not (isinstance(p,(int,float)) and isinstance(o,(int,float))):
        return 0.0, 0.0
    if not (0.0 <= p <= 1.0) or not (o > 1.0):
        return 0.0, 0.0
    num = o*p - 1.0
    den = o - 1.0
    k = num/den if den>0 else 0.0
    edge = num  # (o*p - 1)
    if k <= 0.0:
        return 0.0, edge
    return k, edge

def compute_stake(p, o, bankroll, frac, cap, round_to):
    k, edge = kelly_fraction(p,o)
    if k <= 0.0:
        return 0.0, k, edge
    k_eff = min(k * frac, cap)
    stake = bankroll * k_eff
    if round_to and round_to>0:
        stake = math.floor(stake/round_to + 1e-9) * round_to
    return stake, k, edge

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    debug = bool(args.debug or os.getenv("DEBUG","").lower()=="true")

    out_dir = os.path.join("data","out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    bankroll = get_env_float("BANKROLL", 1000.0)
    frac     = get_env_float("KELLY_FRACTION", 0.5)
    cap      = get_env_float("KELLY_CAP", 0.10)
    min_st   = get_env_float("MIN_STAKE", 0.0)
    max_st   = get_env_float("MAX_STAKE", 0.0)
    round_to = get_env_float("ROUND_TO", 1.0)
    top_n    = int(float(os.getenv("KELLY_TOP_N","14")))

    odds_csv = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(odds_csv):
        log(f"ERRO: {odds_csv} não encontrado")
        sys.exit(2)
    odds = pd.read_csv(odds_csv)
    log(f"odds carregadas de: {odds_csv}")
    log(f"odds carregadas: {len(odds)}")

    # Carrega previsões, se houver
    preds = pd.DataFrame()
    for p in sorted(glob.glob(os.path.join(out_dir,"predictions_*.csv"))):
        try:
            dfp = pd.read_csv(p)
            if {"prob_home","prob_draw","prob_away"}.issubset(set(dfp.columns)):
                preds = dfp[["team_home","team_away","prob_home","prob_draw","prob_away"]].copy()
                break
        except Exception:
            continue

    # Join por chave
    odds["match_key"] = odds["match_key"].astype(str)
    if not preds.empty:
        preds["match_key"] = preds.apply(lambda r: f"{str(r['team_home']).lower()}__vs__{str(r['team_away']).lower()}", axis=1)
    df = odds.merge(
        preds[["match_key","prob_home","prob_draw","prob_away"]] if not preds.empty else
        pd.DataFrame(columns=["match_key","prob_home","prob_draw","prob_away"]),
        on="match_key", how="left"
    )

    # fallback de probs implícitas (apenas quando NÃO houver prob do modelo)
    if df[["prob_home","prob_draw","prob_away"]].isna().all(axis=None):
        log("AVISO: nenhum arquivo de previsões encontrado.")
        log("       Caindo para probabilidades implícitas de mercado (sem overround).")
        ih, idr, ia = [], [], []
        for _, r in df.iterrows():
            ph, pd, pa = implied_from_odds(r)
            ih.append(ph); idr.append(pd); ia.append(pa)
        df["prob_home"] = ih
        df["prob_draw"] = idr
        df["prob_away"] = ia

    # drop linhas sem ao menos 2 odds válidas > 1.0
    def count_valid_odds(r):
        c = 0
        for oc in ("odds_home","odds_draw","odds_away"):
            v = r.get(oc)
            if isinstance(v,(int,float)) and v>1.0 and math.isfinite(v):
                c += 1
        return c
    df["__valid_odds"] = df.apply(count_valid_odds, axis=1)
    df = df[df["__valid_odds"]>=2].copy()

    # drop linhas com todas probs NaN (implied_from_odds já trata isso)
    df = df[~(df["prob_home"].isna() & df["prob_draw"].isna() & df["prob_away"].isna())].copy()
    if df.empty:
        log("ERRO: nenhuma linha elegível para Kelly (após validação de odds e probs).")
        sys.exit(10)

    # calcula stakes p/ cada outcome e escolhe o melhor (maior edge)
    picks = []
    for _, r in df.iterrows():
        cand = []
        for lab in ("home","draw","away"):
            p = r.get(f"prob_{lab}", float("nan"))
            o = r.get(f"odds_{lab}", float("nan"))
            if not (isinstance(p,(int,float)) and 0.0<=p<=1.0 and isinstance(o,(int,float)) and o>1.0): 
                continue
            stake, kfull, edge = compute_stake(p, o, bankroll, frac, cap, round_to)
            if stake >= max(min_st, 0.0) and (max_st<=0 or stake <= max_st):
                cand.append((lab.upper(), stake, kfull, edge, p, o))
        if not cand:
            continue
        # escolhe por maior edge; se empatar, maior stake
        cand.sort(key=lambda x: (x[3], x[1]), reverse=True)
        best = cand[0]
        picks.append({
            "team_home": r["team_home"],
            "team_away": r["team_away"],
            "match_key": r["match_key"],
            "pick": best[0],
            "prob": best[4],
            "odds": best[5],
            "kelly_full": best[2],
            "edge": best[3],
            "stake": best[1],
        })

    if not picks:
        log("ERRO: nenhuma aposta com stake > 0 após aplicar Kelly.")
        sys.exit(10)

    picks_df = pd.DataFrame(picks)
    picks_df = picks_df.sort_values(by=["stake","edge"], ascending=False).head(max(top_n,1)).reset_index(drop=True)
    out_csv = os.path.join(out_dir, "kelly_stakes.csv")
    picks_df.to_csv(out_csv, index=False)
    log(f"OK -> {out_csv} ({len(picks_df)} linhas)")
    log("TOP picks:")
    for i, r in picks_df.iterrows():
        log(f"  #{i+1}: {r['team_home']} x {r['team_away']} | {r['pick']} | prob={round(r['prob'],4)} | odds={r['odds']} | kelly={round(r['kelly_full'],4)} | stake={round(r['stake'],2)}")

if __name__ == "__main__":
    main()