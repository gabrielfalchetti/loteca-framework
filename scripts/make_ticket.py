from __future__ import annotations
import argparse
import numpy as np
import pandas as pd
from pathlib import Path

def entropy(p: np.ndarray) -> float:
    p = p / p.sum()
    return float(-(p * np.log(p)).sum())

def load_probs(base: Path):
    tries = [
        ("joined_pregame.csv",       ["p_home_final","p_draw_final","p_away_final"]),
        ("joined_stacked_bivar.csv", ["p_home_final","p_draw_final","p_away_final"]),
        ("joined_stacked.csv",       ["p_home_final","p_draw_final","p_away_final"]),
        ("joined.csv",               ["p_home","p_draw","p_away"]),
    ]
    for fn, cols in tries:
        p = base / fn
        if p.exists() and p.stat().st_size > 0:
            df = pd.read_csv(p).rename(columns=str.lower)
            have = [c for c in cols if c in df.columns]
            if len(have) == 3:
                return df.copy(), have, fn
    raise RuntimeError("Nenhum arquivo de probabilidades encontrado.")

def main():
    ap = argparse.ArgumentParser(description="Gera cartão Loteca a partir de probabilidades")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--max-duplos", type=int, default=4)
    ap.add_argument("--max-triplos", type=int, default=2)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    df, cols, used = load_probs(base)

    P = df[cols].to_numpy(float)
    P = np.clip(P, 1e-12, 1.0)
    P = P / P.sum(axis=1, keepdims=True)

    matches = pd.read_csv(base/"matches.csv").rename(columns=str.lower)
    if {"home","away"}.issubset(df.columns):
        M = df.merge(matches[["match_id","home","away"]], on="match_id", how="left", suffixes=("","_m"))
        M["home"] = M["home"].fillna(M["home_m"])
        M["away"] = M["away"].fillna(M["away_m"])
    else:
        M = df.merge(matches[["match_id","home","away"]], on="match_id", how="left")

    # escolhe triplos de maior entropia, depois duplos
    ent = np.array([entropy(P[i]) for i in range(P.shape[0])])
    order = np.argsort(ent)[::-1]

    picks = [{int(np.argmax(P[i]))} for i in range(P.shape[0])]
    used_d = used_t = 0
    for idx in order:
        if used_t < args.max_triplos:
            picks[idx] = {0, 1, 2}
            used_t += 1
        elif used_d < args.max_duplos:
            top2 = np.argsort(P[idx])[::-1][:2]
            picks[idx] = {int(top2[0]), int(top2[1])}
            used_d += 1

    sym = {0:"1", 1:"X", 2:"2"}
    rows = []
    for _, r in M.sort_values("match_id").iterrows():
        mid = int(r["match_id"])
        choice = "".join(sorted(sym[x] for x in sorted(list(picks[mid-1]))))
        rows.append({"match_id": mid, "home": r["home"], "away": r["away"], "pick": choice})

    out_csv = base / "loteca_ticket.csv"
    pd.DataFrame(rows).to_csv(out_csv, index=False)

    combos = 1
    cnt_d = cnt_t = 0
    for r in rows:
        m = len(r["pick"]); combos *= m
        if m == 2: cnt_d += 1
        if m == 3: cnt_t += 1

    out_txt = base / "loteca_ticket.txt"
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(f"Cartão Loteca — {args.rodada}\n")
        f.write(f"(Duplos={cnt_d}, Triplos={cnt_t}, Combinações={combos})\n")
        f.write(f"[Probabilidades usadas: {used}]\n\n")
        for r in rows:
            f.write(f"{r['match_id']:>2}  {r['home']} x {r['away']:<24} → {r['pick']}\n")

    print("[ticket] Fonte de prob:", used, "| Duplos:", cnt_d, "Triplos:", cnt_t, "Combinações:", combos)

if __name__ == "__main__":
    main()
