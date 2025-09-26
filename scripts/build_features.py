# scripts/build_features.py
# Constrói features a partir de matches (e opcionalmente odds) só para exemplo.
# Gera data/out/<RODADA>/features.csv com validação.

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

REQUIRED_COLS = ["match_id", "feat_example"]

def main():
    ap = argparse.ArgumentParser(description="Build features -> data/out/<rodada>/features.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--matches", default=None, help="CSV de matches (opcional)")
    ap.add_argument("--odds", default=None, help="CSV de odds (opcional)")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    matches_path = Path(args.matches) if args.matches else base / "matches.csv"
    odds_path    = Path(args.odds) if args.odds else base / "odds.csv"
    out_path     = base / "features.csv"

    if not matches_path.exists():
        raise RuntimeError(f"[build_features] matches não encontrado: {matches_path}")

    m = pd.read_csv(matches_path)
    if "match_id" not in m.columns:
        raise RuntimeError("[build_features] matches.csv precisa de 'match_id'.")

    # exemplo mínimo de feature (só para pipeline rodar)
    f = m[["match_id"]].copy()
    f["feat_example"] = 1.0

    # se existir odds, crie mais 1 feature de exemplo
    if odds_path.exists() and odds_path.stat().st_size > 0:
        o = pd.read_csv(odds_path)
        if "odd_home" in o.columns and "odd_away" in o.columns and "match_id" in o.columns:
            o2 = o[["match_id","odd_home","odd_away"]].copy()
            f = f.merge(o2, on="match_id", how="left")
            f["feat_odds_diff"] = (f["odd_home"] - f["odd_away"]).abs()

    missing = [c for c in REQUIRED_COLS if c not in f.columns]
    if missing:
        raise RuntimeError(f"[build_features] Colunas ausentes nas features: {missing}")
    if f.empty:
        raise RuntimeError("[build_features] Features vazias.")

    f.to_csv(out_path, index=False)
    print(f"[build_features] OK: {len(f)} linhas -> {out_path}")

if __name__ == "__main__":
    main()
