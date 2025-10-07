#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera features proxy de xG (simplificadas) a partir de probabilidades de resultado.

Entrada: <OUT_DIR>/features_univariado.csv
Saída:   <OUT_DIR>/features_xg.csv
"""

import argparse, os, sys, pandas as pd, numpy as np, json

def log(m): print(f"[xg] {m}")
def die(c,m): log(m); sys.exit(c)

def approx_team_xg(p_home, p_draw, p_away):
    # proxy tosca: maior prob. de vitória aproxima maior xG do lado favorito;
    # usa pesos simples que preservam soma ~ constante por jogo
    base = 1.6  # soma média de gols
    bias = (p_home - p_away)
    xg_h = base/2 + 0.8*bias
    xg_a = base - xg_h
    return max(xg_h,0.1), max(xg_a,0.1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true", default=False)
    args = ap.parse_args()

    od = args.rodada
    uni_p = os.path.join(od, "features_univariado.csv")
    if not os.path.exists(uni_p): die(23,f"features_univariado.csv não encontrado")
    df = pd.read_csv(uni_p)
    if df.empty: die(23,"features_univariado.csv vazio")

    xgh, xga = [], []
    for _, r in df.iterrows():
        h, a = approx_team_xg(r["imp_home"], r["imp_draw"], r["imp_away"])
        xgh.append(h); xga.append(a)

    out = pd.DataFrame({
        "match_key": df["match_key"],
        "home": df["home"],
        "away": df["away"],
        "xg_home_proxy": xgh,
        "xg_away_proxy": xga,
        "xg_diff_proxy": np.array(xgh) - np.array(xga)
    })
    out_p = os.path.join(od, "features_xg.csv")
    out.to_csv(out_p, index=False)
    if not os.path.exists(out_p) or os.path.getsize(out_p)==0: die(23,"features_xg.csv não gerado")

    meta = {"rows": int(out.shape[0]), "source": os.path.relpath(uni_p)}
    with open(os.path.join(od,"features_xg_meta.json"),"w",encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    log(f"OK -> {out_p} ({out.shape[0]} jogos)")
    if args.debug:
        print(out.head(10).to_string(index=False))

if __name__ == "__main__":
    main()