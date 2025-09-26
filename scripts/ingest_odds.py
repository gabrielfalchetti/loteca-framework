# scripts/ingest_odds.py
# Ingestor simples: lê de --source (CSV) ou implemente a coleta real no TODO.
# Gera data/out/<RODADA>/odds.csv com validação (pode ficar vazio? aqui exigimos != vazio para saúde do pipeline).

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

REQUIRED_COLS = ["match_id", "odd_home", "odd_draw", "odd_away"]

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.rename(columns={c: c.lower() for c in df.columns})
    maps = {
        "home_odds":"odd_home", "draw_odds":"odd_draw", "away_odds":"odd_away",
        "casa":"odd_home", "empate":"odd_draw", "fora":"odd_away",
        "id":"match_id"
    }
    df = df.rename(columns={k:v for k,v in maps.items() if k in df.columns})
    # tipos numéricos
    for c in ("odd_home","odd_draw","odd_away"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "match_id" not in df.columns and len(df)>0:
        df = df.reset_index(drop=True)
        df["match_id"] = df.index + 1
    return df

def main():
    ap = argparse.ArgumentParser(description="Ingest odds -> data/out/<rodada>/odds.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source", default=None, help="CSV de origem (opcional, p/ teste)")
    args = ap.parse_args()

    out_path = Path(f"data/out/{args.rodada}/odds.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.source:
        df = pd.read_csv(args.source)
    else:
        # ===== TODO: implemente aqui sua coleta real das odds =====
        # Exemplo mínimo (COERENTE com matches de exemplo):
        df = pd.DataFrame({
            "match_id":[1],
            "odd_home":[2.10],
            "odd_draw":[3.10],
            "odd_away":[3.50],
        })
        # ==========================================================

    df = normalize(df)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"[ingest_odds] Colunas ausentes: {missing}")
    if df.empty:
        raise RuntimeError("[ingest_odds] Nenhuma odd coletada.")

    df.to_csv(out_path, index=False)
    print(f"[ingest_odds] OK: {len(df)} linhas -> {out_path}")

if __name__ == "__main__":
    main()
