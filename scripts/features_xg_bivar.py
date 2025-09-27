# scripts/features_xg_bivar.py
# Dixon-Coles (bivariado) a partir de histórico -> probs 1/X/2 com ajuste de dependência
from __future__ import annotations
import argparse, math
from pathlib import Path
from typing import Optional, Tuple, List
import numpy as np
import pandas as pd

# ---------- util ----------
def _norm(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower().strip()
    for a,b in [("ã","a"),("õ","o"),("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ç","c"),("/"," "),("-"," ")]:
        s = s.replace(a,b)
    return " ".join(s.split())

def _norm_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(_norm)

def _poisson_pmf_vec(lam: float, kmax: int = 10) -> np.ndarray:
    k = np.arange(0, kmax+1)
    # factorial via log gamma (estável)
    lg = np.cumsum(np.log(np.maximum(1, np.arange(1, kmax+1)))).astype(float)
    lg = np.insert(lg, 0, 0.0)  # 0! = 1
    pmf = np.exp(-lam + k * np.log(np.maximum(lam, 1e-12)) - lg)
    return pmf

def _tau_dc(x: int, y: int, lam_h: float, lam_a: float, rho: float) -> float:
    # Dixon-Coles tau adjustment for low scores
    # Common practical approximation as in original paper:
    if x == 0 and y == 0:
        return max(1e-9, 1.0 - lam_h*lam_a*rho)
    if x == 0 and y == 1:
        return max(1e-9, 1.0 + lam_h*rho)
    if x == 1 and y == 0:
        return max(1e-9, 1.0 + lam_a*rho)
    if x == 1 and y == 1:
        return max(1e-9, 1.0 - rho)
    return 1.0  # outros placares: sem ajuste

def _dc_matrix(lh: float, la: float, rho: float, kmax: int = 10) -> np.ndarray:
    ph = _poisson_pmf_vec(lh, kmax)
    pa = _poisson_pmf_vec(la, kmax)
    M = np.outer(ph, pa)
    # aplica tau nos quadrantes de baixa contagem
    for x in (0,1):
        for y in (0,1):
            M[x,y] *= _tau_dc(x,y,lh,la,rho)
    S = M.sum()
    if S <= 0:
        return np.full((kmax+1, kmax+1), 1.0/((kmax+1)**2))
    return M / S

def _probs_1x2_from_matrix(M: np.ndarray) -> Tuple[float,float,float]:
    # home>away, draw, away>home
    p1 = np.tril(M, -1).sum()
    px = np.trace(M)
    p2 = np.triu(M, +1).sum()
    s = p1+px+p2
    if s <= 0: return (1/3, 1/3, 1/3)
    return (p1/s, px/s, p2/s)

# ---------- ratings (mesmo núcleo do univariado, simples e robusto) ----------
def _load_history_results() -> pd.DataFrame:
    base = Path("data/out")
    rows=[]
    if not base.exists(): return pd.DataFrame()
    for rodada_dir in sorted(base.iterdir()):
        if not rodada_dir.is_dir(): continue
        f = rodada_dir / "results.csv"
        if not f.exists() or f.stat().st_size == 0: continue
        try:
            df = pd.read_csv(f).rename(columns=str.lower)
            if {"home","away","home_goals","away_goals"}.issubset(df.columns):
                df["rodada"] = rodada_dir.name
                rows.append(df[["rodada","home","away","home_goals","away_goals"]].copy())
        except Exception:
            continue
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def _team_tables(df: pd.DataFrame) -> pd.DataFrame:
    teams=set()
    for c in ["home","away"]:
        teams.update(map(_norm, df[c].unique()))
    rec=[]
    for t in sorted(teams):
        h = df[_norm_series(df["home"])==t]
        a = df[_norm_series(df["away"])==t]
        gf = h["home_goals"].sum() + a["away_goals"].sum()
        ga = h["away_goals"].sum() + a["home_goals"].sum()
        n  = len(h)+len(a)
        rec.append({"team_n": t, "GF": float(gf), "GA": float(ga), "N": int(n)})
    return pd.DataFrame(rec)

def _fit_attack_defense(df: pd.DataFrame, ridge: float=5.0) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=["team_n","atk","def"])
    tbl = _team_tables(df)
    liga_gm = (tbl["GF"].sum()) / max(tbl["N"].sum(), 1)
    liga_gm = max(liga_gm, 0.5)
    gf_r = np.log((tbl["GF"] + ridge) / (tbl["N"] + ridge))
    ga_r = np.log((tbl["GA"] + ridge) / (tbl["N"] + ridge))
    base = math.log(liga_gm)
    atk = gf_r - base
    dfn = -(ga_r - base)
    out = pd.DataFrame({"team_n": tbl["team_n"], "atk": atk, "def": dfn})
    out["atk"] -= out["atk"].mean()
    out["def"] -= out["def"].mean()
    return out

# ---------- estimação rho ----------
def _dc_loglik_pair(hg: int, ag: int, lh: float, la: float, rho: float) -> float:
    # log P(X=hg, Y=ag) up to normalization with tau
    # Poisson independentes + ajuste tau
    # log pmf pois(hg;lh)*pois(ag;la) = -lh + hg*log(lh) - log(hg!) + -la + ag*log(la) - log(ag!)
    # usamos log factorial via math.lgamma
    if lh <= 0 or la <= 0: return -1e9
    from math import lgamma, log
    logp = -lh + hg*log(max(lh,1e-12)) - lgamma(hg+1) \
         + -la + ag*log(max(la,1e-12)) - lgamma(ag+1)
    tau = _tau_dc(hg, ag, lh, la, rho)
    return logp + math.log(max(tau, 1e-12))

def _estimate_rho(history: pd.DataFrame, ratings: pd.DataFrame, home_adv: float, grid: List[float]) -> float:
    # Estima rho por simple grid search robusto. history: cols home,away,home_goals,away_goals
    if history.empty or ratings.empty:
        return 0.0
    rmap = ratings.set_index("team_n")
    def lam_for(row):
        h = _norm(row["home"]); a=_norm(row["away"])
        atk_h = float(rmap.at[h,"atk"]) if h in rmap.index else 0.0
        def_h = float(rmap.at[h,"def"]) if h in rmap.index else 0.0
        atk_a = float(rmap.at[a,"atk"]) if a in rmap.index else 0.0
        def_a = float(rmap.at[a,"def"]) if a in rmap.index else 0.0
        # baseline de gols médios da liga ~1.2 por lado
        lh = max(0.05, 1.25 * math.exp(atk_h - def_a + home_adv))
        la = max(0.05, 1.10 * math.exp(atk_a - def_h))
        return lh, la

    best_rho, best_ll = 0.0, -1e99
    hist = history[["home","away","home_goals","away_goals"]].dropna()
    hist = hist.iloc[-min(len(hist), 5000):]  # limita para performance
    for rho in grid:
        ll = 0.0
        for _, r in hist.iterrows():
            lh, la = lam_for(r)
            ll += _dc_loglik_pair(int(r["home_goals"]), int(r["away_goals"]), lh, la, rho)
        if ll > best_ll:
            best_ll, best_rho = ll, rho
    return float(best_rho)

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Dixon-Coles bivariado -> probs 1/X/2 por rodada")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--home-adv", type=float, default=0.15, help="vantagem de mando em log-escala")
    ap.add_argument("--kmax", type=int, default=10, help="máximo de gols (grade de convolução)")
    ap.add_argument("--rho-min", type=float, default=-0.12)
    ap.add_argument("--rho-max", type=float, default=0.12)
    ap.add_argument("--rho-steps", type=int, default=49)
    args = ap.parse_args()

    # 1) histórico e ratings
    hist = _load_history_results()
    ratings = _fit_attack_defense(hist)

    # 2) estima rho no histórico
    grid = list(np.linspace(args.rho_min, args.rho_max, num=max(3, args.rho_steps)))
    rho_hat = _estimate_rho(hist, ratings, args.home_adv, grid) if not hist.empty and not ratings.empty else 0.0

    # 3) jogos da rodada
    base = Path(f"data/out/{args.rodada}"); base.mkdir(parents=True, exist_ok=True)
    matches = pd.read_csv(base/"matches.csv").rename(columns=str.lower)
    if not {"match_id","home","away"}.issubset(matches.columns):
        raise RuntimeError("[xg_bivar] matches.csv precisa de match_id,home,away")

    rmap = ratings.set_index("team_n") if not ratings.empty else None

    rows=[]
    for _, r in matches.iterrows():
        mid = int(r["match_id"]); h = str(r["home"]); a = str(r["away"])
        hn, an = _norm(h), _norm(a)
        atk_h = float(rmap.at[hn,"atk"]) if (rmap is not None and hn in rmap.index) else 0.0
        def_h = float(rmap.at[hn,"def"]) if (rmap is not None and hn in rmap.index) else 0.0
        atk_a = float(rmap.at[an,"atk"]) if (rmap is not None and an in rmap.index) else 0.0
        def_a = float(rmap.at[an,"def"]) if (rmap is not None and an in rmap.index) else 0.0

        lh = max(0.05, 1.25 * math.exp(atk_h - def_a + args.home_adv))
        la = max(0.05, 1.10 * math.exp(atk_a - def_h))
        M  = _dc_matrix(lh, la, rho_hat, kmax=args.kmax)
        p1, px, p2 = _probs_1x2_from_matrix(M)

        rows.append({
            "match_id": mid, "home": h, "away": a,
            "lambda_home_bv": round(lh,4), "lambda_away_bv": round(la,4),
            "rho_hat": round(rho_hat,6),
            "p1_bv": round(p1,6), "px_bv": round(px,6), "p2_bv": round(p2,6)
        })

    out = pd.DataFrame(rows).sort_values("match_id")
    out.to_csv(base/"xg_bivar.csv", index=False)
    print(f"[xg_bivar] OK -> {base/'xg_bivar.csv'}  (rho_hat={rho_hat:+.5f})")

if __name__ == "__main__":
    main()
