# scripts/kelly.py
import argparse, os, sys, math
import pandas as pd
import numpy as np

def kelly_fraction(p, odds):
    # odds decimais -> b = odds-1
    b = odds - 1.0
    if b <= 0: 
        return 0.0
    f = (p*(b+1) - 1) / b
    return max(0.0, f)  # sem shorting

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--bankroll", required=True, type=float)
    ap.add_argument("--fraction", required=True, type=float)  # fração do Kelly
    ap.add_argument("--cap", required=True, type=float)       # teto da fração por aposta (ex: 0.1)
    ap.add_argument("--top-n", required=True, type=int)
    ap.add_argument("--round-to", required=True, type=int)
    ap.add_argument("--debug", dest="debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    probs_file = os.path.join(out_dir, "probs_calibrated.csv")
    cons_file  = os.path.join(out_dir, "odds_consensus.csv")

    if not os.path.exists(probs_file):
        sys.exit("probs_calibrated.csv not found")
    if not os.path.exists(cons_file):
        sys.exit("odds_consensus.csv not found")

    p = pd.read_csv(probs_file)
    o = pd.read_csv(cons_file)

    df = p.merge(o[["team_home","team_away","odds_home","odds_draw","odds_away"]], on=["team_home","team_away"], how="inner")

    rows = []
    for _, r in df.iterrows():
      # calcula Kelly para cada mercado
      cand = []
      for outcome, pcol, ocol in [
          ("HOME", "p_home", "odds_home"),
          ("DRAW", "p_draw", "odds_draw"),
          ("AWAY", "p_away", "odds_away"),
      ]:
          p_ = float(r[pcol]); odds_ = float(r[ocol])
          f_star = kelly_fraction(p_, odds_)
          eff = min(args.cap, f_star*args.fraction)
          stake = round(eff * args.bankroll, int(args.round_to))
          edge = p_ * odds_ - 1.0
          cand.append((outcome, p_, odds_, f_star, eff, stake, edge))
      # escolhe o melhor edge positivo (se houver)
      cand.sort(key=lambda x: x[-1], reverse=True)
      best = cand[0]
      if best[-1] > 0 and best[5] > 0:
          rows.append({
              "team_home": r["team_home"],
              "team_away": r["team_away"],
              "pick": best[0],
              "p": best[1],
              "odds": best[2],
              "kelly_star": best[3],
              "fraction_eff": best[4],
              "stake": best[5],
              "edge": best[6],
          })

    dfk = pd.DataFrame(rows)
    if dfk.empty:
        # gera arquivo vazio mas válido — ainda assim “falha” mais adiante se for obrigatório
        dfk = pd.DataFrame(columns=["team_home","team_away","pick","p","odds","kelly_star","fraction_eff","stake","edge"])

    # Ordena por edge e mantém TOP N
    dfk = dfk.sort_values("edge", ascending=False).head(args.top_n).reset_index(drop=True)

    out = os.path.join(out_dir, "kelly_stakes.csv")
    dfk.to_csv(out, index=False)

    if args.debug:
        print(dfk.head())

if __name__ == "__main__":
    main()