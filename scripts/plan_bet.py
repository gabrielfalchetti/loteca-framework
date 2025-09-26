# scripts/plan_bet.py
import argparse, pandas as pd, numpy as np
from pathlib import Path

def pick_outcome(row):
    """Decide resultado mais provável (1, X, 2) e entropia."""
    odds = [row["odd_home"], row["odd_draw"], row["odd_away"]]
    probs = 1/np.array(odds)
    probs = probs/probs.sum()
    labels = ["1","X","2"]
    idx = int(np.argmax(probs))
    entropy = -np.sum(probs * np.log(probs+1e-12))
    return labels[idx], entropy, probs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--max-duplos", type=int, default=4)
    ap.add_argument("--max-triplos", type=int, default=2)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    joined = pd.read_csv(base/"joined.csv")

    rows = []
    entropies = []
    for _,r in joined.iterrows():
        pick, ent, probs = pick_outcome(r)
        entropies.append((r["match_id"], ent, pick, probs))

    # ordenar por entropia (jogos mais imprevisíveis primeiro)
    entropies.sort(key=lambda x: -x[1])

    duplos = entropies[:args.max-duplos] if args.max_duplos>0 else []
    triplos = entropies[:args.max_triplos] if args.max_triplos>0 else []

    used_duplo, used_triplo = 0, 0
    picks = {}
    for mid, ent, pick, probs in entropies:
        if used_triplo < args.max_triplos and (mid,ent,pick,probs) in triplos:
            picks[mid] = "123"   # triplo = cobre todos
            used_triplo += 1
        elif used_duplo < args.max_duplos and (mid,ent,pick,probs) in duplos:
            # duplo = cobre resultado principal + segundo mais provável
            sec_idx = int(np.argsort(probs)[-2])
            labels = ["1","X","2"]
            picks[mid] = pick + labels[sec_idx]
            used_duplo += 1
        else:
            picks[mid] = pick

    # gerar cartão
    out = []
    for _,r in joined.iterrows():
        out.append({
            "match_id": r["match_id"],
            "home": r["home"], "away": r["away"],
            "pick": picks.get(r["match_id"], "?")
        })
    df = pd.DataFrame(out)
    df.to_csv(base/"cartao.csv", index=False)
    print(f"[plan_bet] Cartão salvo em {base/'cartao.csv'}")
    print(df)

if __name__=="__main__":
    main()
