# scripts/odds_movement_watch.py
# Detecta movimento de odds entre snapshot baseline e a coleta atual.
# - Lê data/out/<rodada>/odds.csv (atual)
# - Se NÃO existir data/out/<rodada>/odds_baseline.csv -> cria baseline e sai
# - Se existir, compara e cria alerts_odds_movement.csv com mudanças relevantes
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

THRESH_PP = 0.08  # 8 pontos percentuais
def _probs_from_odds(oh, od, oa):
    arr = np.array([oh,od,oa], dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0/arr
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s<=0: return np.array([np.nan,np.nan,np.nan], dtype=float)
    return inv/s

def _fav(p):
    if not np.isfinite(p).all(): return ""
    i = int(np.argmax(p))
    return ["1","X","2"][i]

def main():
    ap = argparse.ArgumentParser(description="Monitor de movimento de odds entre snapshots")
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    cur = base/"odds.csv"
    if not cur.exists() or cur.stat().st_size==0:
        raise RuntimeError(f"[odds_watch] odds.csv ausente/vazio: {cur}")
    dfc = pd.read_csv(cur)

    # baseline
    basefile = base/"odds_baseline.csv"
    if not basefile.exists() or basefile.stat().st_size==0:
        dfc.to_csv(basefile, index=False)
        print(f"[odds_watch] baseline criado: {basefile} (primeira execução)")
        # sem alertas na primeira vez
        return

    dfb = pd.read_csv(basefile)

    # alinhamento por match_id
    key = "match_id"
    if key not in dfc.columns or key not in dfb.columns:
        raise RuntimeError("[odds_watch] odds.csv sem coluna match_id")
    cur_map = dfc.set_index(key)
    base_map = dfb.set_index(key)

    alerts=[]
    for mid in sorted(set(cur_map.index).intersection(set(base_map.index))):
        rc = cur_map.loc[mid]
        rb = base_map.loc[mid]
        try:
            pc = _probs_from_odds(rc["odd_home"], rc["odd_draw"], rc["odd_away"])
            pb = _probs_from_odds(rb["odd_home"], rb["odd_draw"], rb["odd_away"])
        except Exception:
            continue
        if not (np.isfinite(pc).all() and np.isfinite(pb).all()):
            continue

        fav_c = _fav(pc); fav_b = _fav(pb)
        delta = pc - pb
        # maior variação absoluta
        max_abs = float(np.nanmax(np.abs(delta)))
        flip = (fav_c != fav_b)
        if max_abs >= THRESH_PP or flip:
            alerts.append({
                "match_id": mid,
                "fav_before": fav_b,
                "fav_now": fav_c,
                "delta_home_pp": round(float(delta[0]), 4),
                "delta_draw_pp": round(float(delta[1]), 4),
                "delta_away_pp": round(float(delta[2]), 4),
                "max_abs_pp": round(max_abs, 4),
                "favorite_flip": int(flip)
            })

    out = base/"alerts_odds_movement.csv"
    if alerts:
        pd.DataFrame(alerts).to_csv(out, index=False)
        print(f"[odds_watch] {len(alerts)} alertas -> {out}")
    else:
        # cria arquivo vazio com header
        pd.DataFrame([], columns=["match_id","fav_before","fav_now","delta_home_pp","delta_draw_pp","delta_away_pp","max_abs_pp","favorite_flip"]).to_csv(out, index=False)
        print("[odds_watch] nenhum movimento relevante.")

if __name__ == "__main__":
    main()
