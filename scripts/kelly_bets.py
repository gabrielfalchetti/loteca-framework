# -*- coding: utf-8 -*-
import argparse, csv, os, pandas as pd, numpy as np

def kelly_fraction(p, o):
    # Kelly para odds decimais
    if not o or o <= 1.0: return 0.0
    b = o - 1.0
    q = 1.0 - p
    edge = (b*p - q)
    f = edge / b if b > 0 else 0.0
    return max(0.0, f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probs", required=True)  # probs_calibrated.csv
    ap.add_argument("--bankroll", type=float, required=True)
    ap.add_argument("--fraction", type=float, required=True)
    ap.add_argument("--cap", type=float, required=True)
    ap.add_argument("--top_n", type=int, required=True)
    ap.add_argument("--round_to", type=float, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.probs)
    # precisamos também de odds (vem do pred_xg.csv, mas a versão calibrada não tem odds)
    # então fazemos um merge se existir pred_xg.csv na mesma pasta
    pxg = os.path.join(os.path.dirname(args.out), "pred_xg.csv")
    odds = None
    if os.path.exists(pxg):
        odds = pd.read_csv(pxg)[["match_id","odds_home","odds_draw","odds_away"]]
        df = df.merge(odds, on="match_id", how="left")

    bets = []
    for _, r in df.iterrows():
        # escolhemos o maior valor esperado entre 1X2
        choices = [
            ("H", float(r["p_home_cal"]), float(r.get("odds_home", 0) or 0)),
            ("D", float(r["p_draw_cal"]), float(r.get("odds_draw", 0) or 0)),
            ("A", float(r["p_away_cal"]), float(r.get("odds_away", 0) or 0)),
        ]
        choices = [(k,p,o, kelly_fraction(p,o)) for (k,p,o) in choices]
        best = max(choices, key=lambda t: t[3])
        k, p, o, f = best
        f_eff = min(args.cap, f * args.fraction)
        stake = round(args.bankroll * f_eff / max(args.round_to,1e-9)) * args.round_to
        bets.append([r["match_id"], r["team_home"], r["team_away"], k, p, o, f, stake])

    bets_df = pd.DataFrame(bets, columns=["match_id","team_home","team_away","pick","p","odds","kelly_f","stake"])
    bets_df = bets_df.sort_values("kelly_f", ascending=False).head(args.top_n)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    bets_df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"[kelly] OK -> {args.out} (linhas={len(bets_df)})")

if __name__ == "__main__":
    main()