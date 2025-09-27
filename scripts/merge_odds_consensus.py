# scripts/merge_odds_consensus.py
# Consenso de odds com devig Shin + pesos, com FALLBACKS robustos:
# - Se existir odds_*.csv -> usa todos; se não, usa data/out/<rodada>/odds.csv
# - Se faltar colunas home/away, completa a partir de data/out/<rodada>/matches.csv via match_id
# - Se Shin falhar em alguma linha, usa inverso das odds
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
RENAME_CANDIDATES = {
    "home_team":"home","away_team":"away",
    "home_name":"home","away_name":"away",
    "time_casa":"home","time_fora":"away",
    "h":"odd_home","d":"odd_draw","a":"odd_away",
    "odd_h":"odd_home","odd_d":"odd_draw","odd_a":"odd_away",
    "bk":"bookmaker","book":"bookmaker","sportsbook":"bookmaker"
}

def load_weights(path: Path) -> dict:
    if path.exists() and path.stat().st_size>0:
        df = pd.read_csv(path)
        if {"bookmaker","weight"}.issubset(df.columns):
            return {str(r["bookmaker"]).strip().lower(): float(r["weight"]) for _,r in df.iterrows()}
    return {}  # peso=1 por padrão

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normaliza cabeçalhos
    lower = {c: c.lower() for c in df.columns}
    df = df.rename(columns=lower)
    for a,b in RENAME_CANDIDATES.items():
        if a in df.columns and b not in df.columns:
            df = df.rename(columns={a:b})
    # garantir colunas de odds
    need_odds = {"match_id","odd_home","odd_draw","odd_away"}
    missing_odds = [c for c in need_odds if c not in df.columns]
    if missing_odds:
        raise RuntimeError(f"[consensus] arquivo de odds sem colunas obrigatórias: faltam {missing_odds}")
    # bookmaker opcional
    if "bookmaker" not in df.columns:
        df["bookmaker"] = "unknown"
    # home/away podem faltar (vamos completar depois via matches)
    cols = ["match_id","bookmaker","odd_home","odd_draw","odd_away"]
    if "home" in df.columns: cols.append("home")
    if "away" in df.columns: cols.append("away")
    return df[cols].copy()

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

def attach_home_away(df: pd.DataFrame, base: Path) -> pd.DataFrame:
    """Completa colunas home/away usando matches.csv quando necessário."""
    has_home = "home" in df.columns
    has_away = "away" in df.columns
    if has_home and has_away:
        return df
    mpath = base/"matches.csv"
    if not mpath.exists() or mpath.stat().st_size == 0:
        raise RuntimeError(f"[consensus] faltam colunas home/away e matches.csv não existe: {mpath}")
    M = pd.read_csv(mpath)
    # normaliza colunas em matches
    mcols = {c.lower(): c for c in M.columns}
    M = M.rename(columns={mcols.get("home","home"):"home", mcols.get("away","away"):"away", mcols.get("match_id","match_id"):"match_id"})
    need = {"match_id","home","away"}
    if not need.issubset(M.columns):
        raise RuntimeError(f"[consensus] matches.csv inválido, precisa de colunas: {need}")
    merged = pd.merge(df, M[["match_id","home","away"]], on="match_id", how="left", suffixes=("","_m"))
    # se já tinha alguma coluna, preenche faltantes
    if "home" not in df.columns:
        merged["home"] = merged["home_m"]
    if "away" not in df.columns:
        merged["away"] = merged["away_m"]
    merged = merged.drop(columns=[c for c in ["home_m","away_m"] if c in merged.columns])
    # última checagem
    if merged["home"].isna().any() or merged["away"].isna().any():
        # ainda faltou algum nome — preenche com vazio para não quebrar (não é crítico para consenso)
        merged["home"] = merged["home"].fillna("")
        merged["away"] = merged["away"].fillna("")
        print("[consensus] aviso: alguns jogos ficaram sem nome home/away; usando vazio.")
    return merged

def main():
    ap = argparse.ArgumentParser(description="Merge de odds com devig Shin + pesos (com fallbacks)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--weights-file", default="config/bookmaker_weights.csv")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    all_odds = read_sources(base)
    # completa home/away caso falte
    all_odds = attach_home_away(all_odds, base)

    # pesos por bookmaker
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
            "home": str(g["home"].iloc[0]) if "home" in g.columns else "",
            "away": str(g["away"].iloc[0]) if "away" in g.columns else "",
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
