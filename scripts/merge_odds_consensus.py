# scripts/merge_odds_consensus.py
# Consenso de odds com devig Shin + pesos, com FALLBACKS robustos:
# - Usa odds_*.csv quando houver; senão usa data/out/<rodada>/odds.csv
# - Completa home/away via matches.csv quando faltar (sem depender de sufixos do pandas)
# - Se Shin falhar em alguma linha, usa inverso das odds
# - Se não houver NENHUMA linha válida, gera fallback UNIFORME (1/3) a partir de matches.csv para não quebrar o pipeline
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
    if o.shape[-1] != 3:
        return np.array([np.nan, np.nan, np.nan])
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
                tmp = pd.read_csv(f)
                tmp = standardize_columns(tmp)
                tmp["source_file"] = Path(f).name
                rows.append(tmp)
            except Exception as e:
                print(f"[consensus] pulando {f}: {e}")
    else:
        f = base/"odds.csv"
        if f.exists() and f.stat().st_size>0:
            tmp = pd.read_csv(f)
            tmp = standardize_columns(tmp)
            tmp["source_file"] = f.name
            rows.append(tmp)
    if not rows:
        raise RuntimeError("[consensus] Nenhum arquivo de odds encontrado (odds_*.csv ou odds.csv).")
    return pd.concat(rows, ignore_index=True)

def _pick_series(merged: pd.DataFrame, base: str) -> pd.Series:
    for cand in [base, f"{base}_match", f"{base}_y", f"{base}_x"]:
        if cand in merged.columns:
            return merged[cand]
    return pd.Series([""]*len(merged), index=merged.index)

def attach_home_away(df: pd.DataFrame, base: Path) -> pd.DataFrame:
    need_home = "home" not in df.columns
    need_away = "away" not in df.columns
    if not (need_home or need_away):
        return df

    mpath = base/"matches.csv"
    if not mpath.exists() or mpath.stat().st_size == 0:
        raise RuntimeError(f"[consensus] faltam colunas home/away e matches.csv não existe: {mpath}")

    M = pd.read_csv(mpath)
    M_cols_lower = {c: c.lower() for c in M.columns}
    M = M.rename(columns=M_cols_lower)
    if "home_team" in M.columns and "home" not in M.columns: M = M.rename(columns={"home_team":"home"})
    if "away_team" in M.columns and "away" not in M.columns: M = M.rename(columns={"away_team":"away"})
    if not {"match_id","home","away"}.issubset(M.columns):
        raise RuntimeError("[consensus] matches.csv inválido; precisa de colunas: match_id,home,away")

    merged = pd.merge(df, M[["match_id","home","away"]], on="match_id", how="left", suffixes=("", "_match"))

    final_home = _pick_series(merged, "home")
    final_away = _pick_series(merged, "away")
    merged["home"] = final_home.fillna("").astype(str)
    merged["away"] = final_away.fillna("").astype(str)

    for c in ["home_match","away_match","home_x","home_y","away_x","away_y"]:
        if c in merged.columns and c not in ["home","away"]:
            try: merged = merged.drop(columns=[c])
            except Exception: pass

    return merged

def build_uniform_from_matches(base: Path) -> pd.DataFrame:
    """Fallback: cria odds uniformes (1/3) a partir de matches.csv para não quebrar pipeline."""
    mpath = base/"matches.csv"
    if not mpath.exists() or mpath.stat().st_size == 0:
        raise RuntimeError(f"[consensus] fallback uniforme falhou: matches.csv ausente/vazio ({mpath})")
    M = pd.read_csv(mpath)
    M = M.rename(columns=str.lower)
    if not {"match_id","home","away"}.issubset(M.columns):
        # tenta mapear nomes comuns
        if "home_team" in M.columns and "home" not in M.columns: M = M.rename(columns={"home_team":"home"})
        if "away_team" in M.columns and "away" not in M.columns: M = M.rename(columns={"away_team":"away"})
    if not {"match_id","home","away"}.issubset(M.columns):
        raise RuntimeError("[consensus] fallback uniforme: matches.csv inválido; precisa de match_id,home,away")
    out = M[["match_id","home","away"]].copy()
    out["p_home"] = 1/3.0; out["p_draw"] = 1/3.0; out["p_away"] = 1/3.0
    out["odd_home"] = 3.0; out["odd_draw"] = 3.0; out["odd_away"] = 3.0
    out["n_bookmakers"] = 0
    return out.sort_values("match_id")

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
    probs_rows = []
    valid_rows = []
    for _, r in all_odds.iterrows():
        try:
            oh, od, oa = float(r["odd_home"]), float(r["odd_draw"]), float(r["odd_away"])
        except Exception:
            continue
        if not (oh > 1.0 and od > 1.0 and oa > 1.0 and np.isfinite([oh,od,oa]).all()):
            continue
        p_line = shin_devig([oh, od, oa])
        if not np.isfinite(p_line).all():
            p_line = inv_probs([oh, od, oa])
        if np.isfinite(p_line).all():
            probs_rows.append(p_line)
            valid_rows.append(True)
        else:
            valid_rows.append(False)

    if len(probs_rows) == 0:
        # Fallback total: gerar odds uniformes a partir de matches.csv
        out_df = build_uniform_from_matches(base)
        out_path = base/"odds.csv"
        out_df.to_csv(out_path, index=False)
        print(f"[consensus] WARNING: nenhuma linha de odds válida; gerando odds uniformes como fallback -> {out_path} (n={len(out_df)})")
        return

    probs_rows = np.vstack(probs_rows)
    # Mantém somente linhas válidas
    all_odds = all_odds.iloc[np.where(np.array(valid_rows))[0]].copy()

    all_odds[["p_home_bm","p_draw_bm","p_away_bm"]] = probs_rows

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
        ph, pdraw, pa = wmean(g, ["p_home_bm","p_draw_bm","p_away_bm"])
        ps = np.array([ph,pdraw,pa], dtype=float)
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

    if not agg:
        # Sem grupos (p.ex., odds de múltiplos jogos inválidas) -> fallback uniforme
        out_df = build_uniform_from_matches(base)
        out_path = base/"odds.csv"
        out_df.to_csv(out_path, index=False)
        print(f"[consensus] WARNING: agregação vazia; gerando odds uniformes como fallback -> {out_path} (n={len(out_df)})")
        return

    out_df = pd.DataFrame(agg).sort_values("match_id")
    out_path = base/"odds.csv"
    out_df.to_csv(out_path, index=False)
    print(f"[consensus] odds de consenso -> {out_path} (n={len(out_df)})")

if __name__ == "__main__":
    main()
