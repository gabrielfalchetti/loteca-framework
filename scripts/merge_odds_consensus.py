# scripts/merge_odds_consensus.py
from __future__ import annotations
import argparse, glob
from pathlib import Path
import numpy as np
import pandas as pd

# ----------------- Shin devig -----------------
def shin_devig(odds):
    """
    odds: array-like (oh, od, oa) > 1.0
    Retorna probabilidades devigadas via método de Shin (aprox. numérica 1D).
    Referência: Shin, H.S. (1991, 1993) - insider trading in betting markets.
    """
    o = np.array(odds, dtype=float)
    if np.any(~np.isfinite(o)) or np.any(o <= 1.0):
        return np.full(3, np.nan)
    iv = 1.0 / o
    s = iv.sum()
    # bisseção simples em z \in [0, zmax] tal que somatório p_i = 1
    # p_i = (sqrt(iv_i^2 + 4*z*iv_i) - iv_i) / (2*z)   (forma comum do Shin)
    # Para z -> 0, p_i -> iv_i / sum(iv) (sem devig)
    def p_of(z):
        if z <= 0:
            return iv / iv.sum()
        root = np.sqrt(iv*iv + 4.0*z*iv)
        p = (root - iv) / (2.0*z)
        return p
    # procura zmax onde p_i fica definido e soma>1
    z_lo, z_hi = 0.0, 1.0
    for _ in range(40):
        p_hi = p_of(z_hi)
        if np.any(~np.isfinite(p_hi)) or (p_hi.sum() < 1.0):
            z_hi *= 0.5
        else:
            break
    # bisseção para somar ~1
    target = 1.0
    for _ in range(80):
        z_mid = 0.5*(z_lo+z_hi)
        p_mid = p_of(z_mid)
        s_mid = p_mid.sum()
        if not np.isfinite(s_mid):
            z_hi = z_mid
            continue
        if s_mid > target:
            z_lo = z_mid
        else:
            z_hi = z_mid
    p = p_of(0.5*(z_lo+z_hi))
    # saneamento
    p = np.clip(p, 1e-9, 1.0)
    p /= p.sum()
    return p

# ----------------- Util -----------------
def load_weights(path: Path) -> dict:
    if path.exists() and path.stat().st_size>0:
        df = pd.read_csv(path)
        if {"bookmaker","weight"}.issubset(df.columns):
            w = {str(r["bookmaker"]).strip().lower(): float(r["weight"]) for _,r in df.iterrows()}
            return w
    return {}  # igual para todos

def read_all_odds(base: Path) -> pd.DataFrame:
    files = sorted([f for f in glob.glob(str(base/"odds_*.csv"))])
    rows=[]
    for f in files:
        try:
            df = pd.read_csv(f)
            # esperado: match_id,home,away,bookmaker,odd_home,odd_draw,odd_away (mínimo)
            need = {"match_id","home","away","bookmaker","odd_home","odd_draw","odd_away"}
            if not need.issubset(df.columns):
                continue
            df["source_file"] = Path(f).name
            rows.append(df[["match_id","home","away","bookmaker","odd_home","odd_draw","odd_away","source_file"]].copy())
        except Exception:
            continue
    if not rows:
        raise RuntimeError("[consensus] Nenhum odds_*.csv encontrado para merge.")
    return pd.concat(rows, ignore_index=True)

def main():
    ap = argparse.ArgumentParser(description="Merge de odds com devig Shin + pesos por bookmaker")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--weights-file", default="config/bookmaker_weights.csv")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    all_odds = read_all_odds(base)
    weights = load_weights(Path(args.weights_file))

    # normaliza nomes
    all_odds["bookmaker_norm"] = all_odds["bookmaker"].astype(str).str.strip().str.lower()

    # aplica devig Shin por linha
    P = []
    for _, r in all_odds.iterrows():
        p = shin_devig([r["odd_home"], r["odd_draw"], r["odd_away"]])
        P.append(p)
    P = np.vstack(P)
    all_odds[["p_home_bm","p_draw_bm","p_away_bm"]] = P

    # remove linhas sem p válidas
    all_odds = all_odds.dropna(subset=["p_home_bm","p_draw_bm","p_away_bm"])

    # pesos
    all_odds["weight"] = all_odds["bookmaker_norm"].map(lambda x: float(weights.get(x, 1.0)))

    # agrega por match_id (média ponderada)
    def wmean(g, cols):
        w = g["weight"].values.reshape(-1,1)
        X = g[cols].values
        num = (w*X).sum(axis=0)
        den = w.sum()
        return num/den if den>0 else X.mean(axis=0)

    agg = []
    for mid, g in all_odds.groupby("match_id"):
        ph, pd, pa = wmean(g, ["p_home_bm","p_draw_bm","p_away_bm"])
        # renormaliza e gera odds coerentes
        ps = np.array([ph,pd,pa], dtype=float)
        ps = np.clip(ps, 1e-9, 1.0); ps = ps/ps.sum()
        oh, od, oa = 1.0/ps
        row = {
            "match_id": int(mid),
            "home": g["home"].iloc[0],
            "away": g["away"].iloc[0],
            "p_home": ps[0], "p_draw": ps[1], "p_away": ps[2],
            "odd_home": oh, "odd_draw": od, "odd_away": oa,
            "n_bookmakers": int(g["bookmaker"].nunique())
        }
        agg.append(row)
    out = pd.DataFrame(agg).sort_values("match_id")

    out_path = base/"odds.csv"
    out.to_csv(out_path, index=False)
    print(f"[consensus] odds de consenso com Shin+pesos -> {out_path} (n={len(out)})")

if __name__ == "__main__":
    main()
