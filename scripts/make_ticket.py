# scripts/make_ticket.py
import argparse, os, sys
import pandas as pd
import numpy as np

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--top-n", type=int, default=14)
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
    c = pd.read_csv(cons_file)

    df = p.merge(c[["team_home","team_away"]], on=["team_home","team_away"], how="inner")

    # Escolhe o resultado mais provável por jogo
    choices = []
    for _, r in df.iterrows():
        probs = {"1": r["p_home"], "X": r["p_draw"], "2": r["p_away"]}
        pick  = max(probs, key=probs.get)
        conf  = probs[pick]
        choices.append({"team_home": r["team_home"], "team_away": r["team_away"], "pick": pick, "confidence": conf})

    dft = pd.DataFrame(choices).sort_values("confidence", ascending=False).head(args.top_n).reset_index(drop=True)

    # Numera 1..N para formato de cartão
    dft.insert(0, "jogo", dft.index + 1)

    out = os.path.join(out_dir, "loteca_ticket.csv")
    dft.to_csv(out, index=False)

    if args.debug:
        print(dft)

if __name__ == "__main__":
    main()