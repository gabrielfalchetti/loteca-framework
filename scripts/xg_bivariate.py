# -*- coding: utf-8 -*-
import argparse, json, os, sys, math, csv, pandas as pd
from _utils_norm import norm_name
from math import exp
from itertools import product

def pois_pmf(k, lam):
    return exp(-lam) * (lam**k) / math.factorial(k)

def outcome_probs(lh, la, max_goals=10):
    ph = pd = pa = 0.0
    for gh, ga in product(range(max_goals+1), repeat=2):
        p = pois_pmf(gh, lh) * pois_pmf(ga, la)
        if gh > ga: ph += p
        elif gh == ga: pd += p
        else: pa += p
    s = ph+pd+pa
    if s <= 0: return 0.0,0.0,0.0
    return ph/s, pd/s, pa/s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--state", required=True)      # state_params.json
    ap.add_argument("--consensus", required=True)  # odds_consensus.csv
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.state, "r", encoding="utf-8") as f:
        states = json.load(f)

    cons = pd.read_csv(args.consensus)
    cons["home_norm"] = cons["team_home"].astype(str).map(norm_name)
    cons["away_norm"] = cons["team_away"].astype(str).map(norm_name)

    rows = []
    for _, r in cons.iterrows():
        hn, an = r["home_norm"], r["away_norm"]
        # procura estado com igualdade fraca
        sH = states.get(hn) or states.get(r["team_home"]) or states.get(r["home_norm"])
        sA = states.get(an) or states.get(r["team_away"]) or states.get(r["away_norm"])

        # fallback: lambdas médios
        lh = (sH["attack"] if isinstance(sH, dict) else 1.2) + (0.15 if isinstance(sH, dict) else 0.15)
        la = (sA["attack"] if isinstance(sA, dict) else 1.1)

        pH, pD, pA = outcome_probs(max(lh,0.05), max(la,0.05), max_goals=10)

        rows.append([
            r.get("match_id", ""),
            r["team_home"], r["team_away"],
            round(pH,6), round(pD,6), round(pA,6),
            r.get("odds_home", ""), r.get("odds_draw",""), r.get("odds_away","")
        ])

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id","team_home","team_away","p_home","p_draw","p_away","odds_home","odds_draw","odds_away"])
        w.writerows(rows)
    print(f"[xg] OK -> {args.out} (linhas={len(rows)})")

if __name__ == "__main__":
    main()