# scripts/ingest_matches.py
# Ingestor simples: lê de --source (CSV) ou implemente a coleta real no TODO.
# Gera data/out/<RODADA>/matches.csv com validação (não salva vazio).

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

REQUIRED_COLS = ["match_id", "home", "away", "date"]

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.rename(columns={c: c.lower() for c in df.columns})
    maps = {
        "mandante": "home", "visitante": "away",
        "time_casa": "home", "time_fora": "away",
        "casa": "home", "fora": "away",
        "home_team": "home", "away_team": "away",
        "data_jogo": "date", "data": "date", "matchdate": "date",
        "id": "match_id"
    }
    df = df.rename(columns={k:v for k,v in maps.items() if k in df.columns})
    for col in ("home","away"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.replace(r"\s+"," ",regex=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # cria match_id se faltar
    if "match_id" not in df.columns:
        df = df.reset_index(drop=True)
        df["match_id"] = df.index + 1
    return df

def main():
    ap = argparse.ArgumentParser(description="Ingest matches -> data/out/<rodada>/matches.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source", default=None, help="CSV de origem (opcional, p/ teste)")
    args = ap.parse_args()

    out_path = Path(f"data/out/{args.rodada}/matches.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.source:
        df = pd.read_csv(args.source)
    else:
        # ===== TODO: implemente aqui sua coleta real das fontes =====
        # Exemplo mínimo (COMENTE/REMOVA depois):
        df = pd.DataFrame({
            "match_id":[1],
            "home":["Time A"],
            "away":["Time B"],
            "date":["2025-09-20 16:00:00"]
        })
        # ============================================================

    df = normalize(df)

    # validações
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"[ingest_matches] Colunas ausentes: {missing}")
    if df.empty:
        raise RuntimeError("[ingest_matches] Nenhuma partida coletada.")

    df.to_csv(out_path, index=False)
    print(f"[ingest_matches] OK: {len(df)} linhas -> {out_path}")

if __name__ == "__main__":
    main()
