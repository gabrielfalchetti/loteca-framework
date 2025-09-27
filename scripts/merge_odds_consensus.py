# scripts/merge_odds_consensus.py
# Consenso de odds com devig Shin + pesos, com FALLBACKS:
# - Se existir qualquer odds_*.csv -> usa todos e agrega
# - Se NÃO existir odds_*.csv -> usa data/out/<rodada>/odds.csv como fonte única
# - Se Shin falhar em alguma linha -> fallback para probs por inverso das odds (devig simples)
from __future__ import annotations
import argparse, glob
from pathlib import Path
import numpy as np
import pandas as pd

# ----------------- Shin devig -----------------
def shin_devig(odds):
    """
    odds: array-like (oh, od, oa) > 1.0
    Retorna probabilidades devigadas via método de Shin (aprox. bisseção em z).
    """
    o = np.array(odds, dtype=float)
    if np.any(~np.isfinite(o)) or np.any(o <= 1.0):
        return np.full(3, np.nan)
    iv = 1.0 / o

    def p_of(z):
        if z <= 0:
            p = iv / iv.sum()
            return p
        root = np.sqrt(iv*iv + 4.0*z*iv)
        p = (root - iv) / (2.0*z)
        return p

    # encontra intervalo simples para bisseção
    z_lo, z_hi = 0.0, 1.0
    for _ in range(40):
        p_hi = p_of(z_hi)
        if not np.isfinite(p_hi).all() or p_hi.sum() < 1.0:
            z_hi *= 0.5
        else:
            break

    target = 1.0
    for _ in range(80):
        z_mid = 0.5*(z_lo+z_hi)
        p_mid = p_of(z_mid)
        s_mid = p_mid.sum() if np.isfinite(p_mid).all() else np.inf
        if s_mid > target:
            z_lo = z_mid
        else:
            z_hi = z_mid

    p = p_of(0.5*(z_lo+z_hi))
    if not np.isfinite(p).all():
        return np.full(3, np.nan)
    p = np.clip(p, 1e-9, 1.0)
    p = p / p.sum()
    return p

def inv_probs(odds):
    o = np.array(odds, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0 / o
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s <= 0:
        return np.array([np.nan, np.nan, np.nan])
    p = inv / s
    p = np.clip(p, 1e-9, 1.0)
    p = p / p.sum()
    return p

# ----------------- Util -----------------
def load_weights(path: Path) -> dict:
    if path.exists() and path.stat().st_size>0:
        df = pd.read_csv(path)
        if {"bookmaker","weight"}.issubset(df.columns):
            return {str(r["bookmaker"]).strip().lower(): float(r["weight"]) for _,r in df.iterrows()}
    return {}  # peso=1 por padrão

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Garante colunas mínimas; cria bookmaker "unknown" se não existir
    rename_map = {
        "home_team":"home","away_team":"away",
        "odd_h":"odd_home","odd_d":"odd_draw","odd_a":"odd_away",
        "bk":"bookmaker"
    }
    for a,b in rename_map.items():
        if a in df.columns and b not in df.columns:
            df = df.rename(columns={a:b})
    need = {"match_id","home","away","odd_home","odd_draw","odd_away"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"[consensus] arquivo de odds sem colunas necessárias: faltam {missing}")
    if "bookmaker" not in df.columns:
        df["bookmaker"] = "unknown"
    return df[["match_id","home","away","bookmaker","odd_home","odd_draw","odd_away"]].copy()

def read_sources(base: Path) -> pd.DataFrame:
    files = sorted([f for f in glob.glob(str(base/"odds_*.csv"))])
    rows=[]
    if files:
        for f in files:
            try:
                df = pd.read_csv(f)
                df = standardize_columns(df)
                df["source_file"] = Path(f).name
                rows.append(df)
            except Exception as e:
                print(f"[consensus] pulando {f}: {e}")
    else:
        # FALLBACK: tenta usar data/out/<rodada>/odds.csv
        f = base/"odds.csv"
        if f.exists() and f.stat().st_size>0:
            df = pd.read_csv(f)
            df = standardize_columns(df)
            df["source_file"] = f.name
            rows.append(df)
    if not rows:
        raise RuntimeError("[consensus] Nenhum arquivo de odds encontrado (odds_*.csv ou odds.csv).")
    return pd.concat(rows, ignore_index=True)

def main():
    ap = argparse.ArgumentParser(description="Merge de odds com devig Shin + pesos (com fallbacks)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--weights-file", default="config/bookmaker_weights.csv")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    all_odds = read_sources(base)
    weights = load_weights(Path(args.weights_file))
    all_odds["bookmaker_norm"] = all_odds["bookmaker"].astype(str).str.strip().str.lower()

    # calcula p_bm por linha com Shin; se der NaN, usa inverso das odds
    P = []
    for _, r in all_odds.iterrows():
        p = shin_devig([r["odd_home"], r["odd_draw"], r["odd_away"]])
        if not np.isfinite(p).all():
            p = inv_probs([r["odd_home"], r["odd_draw"], r["odd_away"]])
        P.append(p)
    P = np.vstack(P)
    all_odds[["p_home_bm","p_draw_bm","p_away_bm"]] = P

    # filtra inválidos
    all_odds = all_odds.dropna(subset=["p_home_bm","p_draw_bm","p_away_bm"])

    # aplica pesos
    all_odds["weight"] = all_odds["bookmaker_norm"].map(lambda x: float(weights.get(x, 1.0)))

    # agrega por match
    def wmean(g, cols):
        w = g["weight"].values.reshape(-1,1)
        X = g[cols].values
        num = (w*X).sum(axis=0)
        den = w.sum()
        if den <= 0:  # fallback média simples
            return X.mean(axis=0)
        return num/den

    agg = []
    for mid, g in all_odds.groupby("match_id"):
        ph, pd, pa = wmean(g, ["p_home_bm","p_draw_bm","p_away_bm"])
        ps = np.array([ph,pd,pa], dtype=float)
        ps = np.clip(ps, 1e-9, 1.0); ps = ps/ps.sum()
        oh, od, oa = 1.0/ps
        agg.append({
            "match_id": int(mid),
            "home": str(g["home"].iloc[0]),
            "away": str(g["away"].iloc[0]),
            "p_home": ps[0], "p_draw": ps[1], "p_away": ps[2],
            "odd_home": oh, "odd_draw": od, "odd_away": oa,
            "n_bookmakers": int(g["bookmaker"].nunique())
        })

    out = pd.DataFrame(agg).sort_values("match_id")
    out_path = base/"odds.csv"
    out.to_csv(out_path, index=False)
    print(f"[consensus] odds de consenso -> {out_path} (n={len(out)})")

if __name__ == "__main__":
    main()
