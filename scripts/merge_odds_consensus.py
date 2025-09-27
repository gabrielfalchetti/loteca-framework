# scripts/merge_odds_consensus.py
# Consenso de odds com devig Shin + pesos, com FALLBACKS robustos:
# - Usa odds_*.csv quando houver; senão usa data/out/<rodada>/odds.csv
# - Completa home/away via matches.csv quando faltar
# - Se Shin falhar em alguma linha, usa inverso das odds
from __future__ import annotations
import argparse, glob
from pathlib import Path
import numpy as np
import pandas as pd

# ----------------- Shin devig -----------------
def shin_devig(odds):
    o = np.array(odds, dtype=float)
    if np.any(~np.isfinite(o)) or np.any(o <= 1.0):
        return np.full(3, np.nan)
    iv = 1.0 / o
    def p_of(z):
        if z <= 0:
            return iv / iv.sum()
        root = np.sqrt(iv*iv + 4.0*z*iv)
        return (root - iv) / (2.0*z)
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
        if s_mid > target: z_lo = z_mid
        else:              z_hi = z_mid
    p = p_of(0.5*(z_lo+z_hi))
    if not np.isfinite(p).all():
        return np.full(3, np.nan)
    p = np.clip(p, 1e-9, 1.0); p /= p.sum()
    return p

def inv_probs(odds):
    o = np.array(odds, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0 / o
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s <= 0: return np.array([np.nan, np.nan, np.nan])
    p = inv / s
    p = np.clip(p, 1e-9, 1.0); p /= p.sum()
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
    return {}

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    lower = {c: c.lower() for c in df.columns}
    df = df.rename(columns=lower)
    for a,b in RENAME_CANDIDATES.items():
        if a in df.columns and b not in df.columns:
            df = df.rename(columns={a:b})
    need_odds = {"match_id","odd_home","odd_draw","odd_away"}
    miss = [c for c in need_odds if c not in df.columns]
    if miss:
        raise RuntimeError(f"[consensus] arquivo de odds sem colunas obrigatórias: faltam {miss}")
    if "bookmaker" not in df.columns:
        df["bookmaker"] = "unknown"
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
    """Completa colunas home/away usando matches.csv quando necessário (sem depender de sufixos)."""
    need_home = "home" not in df.columns
    need_away = "away" not in df.columns
    if not (need_home or need_away):
        return df

    mpath = base/"matches.csv"
    if not mpath.exists() or mpath.stat().st_size == 0:
        raise RuntimeError(f"[consensus] faltam colunas home/away e matches.csv não existe: {mpath}")

    M = pd.read_csv(mpath)
    # normaliza
    M_cols_lower = {c: c.lower() for c in M.columns}
    M = M.rename(columns=M_cols_lower)
    # padroniza nomes esperados
    rename_M = {}
    if "home_team" in M.columns and "home" not in M.columns: rename_M["home_team"] = "home"
    if "away_team" in M.columns and "away" not in M.columns: rename_M["away_team"] = "away"
    if rename_M: M = M.rename(columns=rename_M)
    # checa colunas
    if not {"match_id","home","away"}.issubset(M.columns):
        raise RuntimeError("[consensus] matches.csv inválido; precisa de colunas: match_id,home,away")

    # merge simples (sem sufixos): se df já tiver 'home'/'away', usamos fillna
    merged = pd.merge(df, M[["match_id","home","away"]], on="match_id", how="left")

    if need_home:
        # não tinha 'home' no df original -> usa a 'home' do matches diretamente
        merged["home"] = merged["home_y"]
    else:
        # já tinha 'home' -> preenche nulos com a versão do matches
        merged["home_x"] = merged["home_x"].where(merged["home_x"].notna(), merged["home_y"])
        merged["home"] = merged["home_x"]

    if need_away:
        merged["away"] = merged["away_y"]
    else:
        merged["away_x"] = merged["away_x"].where(merged["away_x"].notna(), merged["away_y"])
        merged["away"] = merged["away_x"]

    # limpa colunas auxiliares se existirem
    for c in ["home_x","home_y","away_x","away_y"]:
        if c in merged.columns:
            merged = merged.drop(columns=[c])

    # segurança final
    merged["home"] = merged["home"].fillna("")
    merged["away"] = merged["away"].fillna("")
    return merged

def main():
    ap = argparse.ArgumentParser(description="Merge de odds com devig Shin + pesos (com fallbacks)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--weights-file", default="config/bookmaker_weights.csv")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    all_odds = read_sources(base)
    all_odds = attach_home_away(all_odds, base)

    weights = load_weights(Path(args.weights_file))
    all_odds["bookmaker_norm"] = all_odds["bookmaker"].astype(str).str.strip().str.lower()

    # p_bm por linha: Shin -> fallback inverso
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

    # pesos
    all_odds["weight"] = all_odds["bookmaker_norm"].map(lambda x: float(weights.get(x, 1.0)))

    # agrega por match (média ponderada)
    def wmean(g, cols):
        w = g["weight"].values.reshape(-1,1)
        X = g[cols].values
        num = (w*X).sum(axis=0); den = w.sum()
        return (num/den) if den>0 else X.mean(axis=0)

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
