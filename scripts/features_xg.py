# scripts/features_xg.py
# Estima xG simples (Poisson) a partir de histórico real e gera probs 1/X/2 por jogo.
from __future__ import annotations
import argparse, math
from pathlib import Path
from typing import Dict, Tuple, List
import numpy as np
import pandas as pd

# ---------------- utils ----------------
def _norm(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower().strip()
    for a,b in [("ã","a"),("õ","o"),("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ç","c"),("/"," "),("-"," ")]:
        s = s.replace(a,b)
    return " ".join(s.split())

def _load_history_results() -> pd.DataFrame:
    """Varre data/out/*/results.csv e concatena; retorna colunas: date, home, away, home_goals, away_goals."""
    base = Path("data/out")
    rows=[]
    if not base.exists(): return pd.DataFrame()
    for rodada_dir in sorted(base.iterdir()):
        if not rodada_dir.is_dir(): continue
        f = rodada_dir / "results.csv"
        if not f.exists() or f.stat().st_size==0: continue
        try:
            df = pd.read_csv(f).rename(columns=str.lower)
            if {"home","away","home_goals","away_goals"}.issubset(df.columns):
                df["rodada"] = rodada_dir.name
                rows.append(df[["rodada","home","away","home_goals","away_goals"]].copy())
        except Exception:
            continue
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def _team_tables(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega GF/GA e jogos por time."""
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

def _norm_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(_norm)

def _fit_attack_defense(df: pd.DataFrame, ridge: float=5.0) -> pd.DataFrame:
    """
    Estima ataque/defesa por time via método simples:
    atk = log(GF/N) - liga_gm; def = -(log(GA/N) - liga_gm); shrink com ridge.
    """
    if df.empty: return pd.DataFrame(columns=["team_n","atk","def"])
    tbl = _team_tables(df)
    liga_gm = (tbl["GF"].sum()) / max(tbl["N"].sum(), 1)  # gols por time-jogo
    liga_gm = max(liga_gm, 0.5)  # piso
    # log rates
    gf_r = np.log((tbl["GF"] + ridge) / (tbl["N"] + ridge))
    ga_r = np.log((tbl["GA"] + ridge) / (tbl["N"] + ridge))
    base = math.log(liga_gm)
    atk = gf_r - base
    dfn = -(ga_r - base)
    out = pd.DataFrame({"team_n": tbl["team_n"], "atk": atk, "def": dfn})
    # centraliza média ~0
    out["atk"] -= out["atk"].mean()
    out["def"] -= out["def"].mean()
    return out

def _poisson_matrix(lam: float, kmax: int=10) -> np.ndarray:
    k = np.arange(0, kmax+1)
    pmf = np.exp(-lam) * np.power(lam, k) / np.maximum(1, np.array([math.factorial(int(x)) for x in k], dtype=float))
    return pmf

def _probs_1x2(lh: float, la: float, kmax: int=10) -> Tuple[float,float,float]:
    """Convolução de Poisson independentes."""
    ph = _poisson_matrix(lh, kmax)
    pa = _poisson_matrix(la, kmax)
    M = np.outer(ph, pa)  # score grid
    p1 = np.tril(M, -1).sum()       # home > away
    px = np.trace(M)                # draw
    p2 = np.triu(M, +1).sum()       # away > home
    s = p1+px+p2
    if s<=0: return (1/3,1/3,1/3)
    return (p1/s, px/s, p2/s)

# --------------- main -------------------
def main():
    ap = argparse.ArgumentParser(description="Gera xG e probs 1X2 Poisson por rodada")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--home-adv", type=float, default=0.15, help="vantagem de mando (log-escala)")
    ap.add_argument("--kmax", type=int, default=10, help="máximo de gols no somatório Poisson")
    args = ap.parse_args()

    # 1) histórico
    hist = _load_history_results()
    ratings = _fit_attack_defense(hist) if not hist.empty else pd.DataFrame(columns=["team_n","atk","def"])

    # 2) jogos da rodada
    base = Path(f"data/out/{args.rodada}"); base.mkdir(parents=True, exist_ok=True)
    matches = pd.read_csv(base/"matches.csv").rename(columns=str.lower)
    if not {"match_id","home","away"}.issubset(matches.columns):
        raise RuntimeError("[xg] matches.csv precisa de match_id,home,away")

    # 3) liga_gm baseline (piso) para casos sem histórico
    if hist.empty:
        liga_gm_home, liga_gm_away = 1.30, 1.10  # baseline conservador
    else:
        gm = (hist["home_goals"].sum() + hist["away_goals"].sum()) / max(len(hist),1)
        # pequeno viés domiciliar
        gh = hist["home_goals"].mean() if len(hist) else gm/2
        ga = hist["away_goals"].mean() if len(hist) else gm/2
        liga_gm_home, liga_gm_away = max(0.6, gh), max(0.6, ga)

    # 4) junta ratings
    rmap = ratings.set_index("team_n") if not ratings.empty else None

    rows=[]
    for _, r in matches.iterrows():
        mid = int(r["match_id"]); h = str(r["home"]); a = str(r["away"])
        hn, an = _norm(h), _norm(a)

        atk_h = float(rmap.at[hn,"atk"]) if (rmap is not None and hn in rmap.index) else 0.0
        def_h = float(rmap.at[hn,"def"]) if (rmap is not None and hn in rmap.index) else 0.0
        atk_a = float(rmap.at[an,"atk"]) if (rmap is not None and an in rmap.index) else 0.0
        def_a = float(rmap.at[an,"def"]) if (rmap is not None and an in rmap.index) else 0.0

        # λ estimados (log-escala + home advantage)
        lh = max(0.05, liga_gm_home * math.exp(atk_h - def_a + args.home_adv))
        la = max(0.05, liga_gm_away * math.exp(atk_a - def_h))

        p1,px,p2 = _probs_1x2(lh, la, kmax=args.kmax)
        rows.append({
            "match_id": mid, "home": h, "away": a,
            "lambda_home": round(lh,4), "lambda_away": round(la,4),
            "p1_xg": round(p1,6), "px_xg": round(px,6), "p2_xg": round(p2,6)
        })

    out = pd.DataFrame(rows).sort_values("match_id")
    out.to_csv(base/"xg_features.csv", index=False)
    print(f"[xg] OK -> {base/'xg_features.csv'}")

if __name__ == "__main__":
    main()
