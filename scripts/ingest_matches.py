# scripts/ingest_matches.py
# Lê data/in/<RODADA>/matches_source.csv e gera data/out/<RODADA>/matches.csv

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

REQUIRED = ["match_id", "home", "away"]

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
        "id": "match_id",
    }
    df = df.rename(columns={k: v for k, v in maps.items() if k in df.columns})
    for col in ("home", "away"):
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
            )
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "match_id" not in df.columns:
        df = df.reset_index(drop=True)
        df["match_id"] = df.index + 1
    return df

def main():
    ap = argparse.ArgumentParser(description="Ingest matches -> data/out/<RODADA>/matches.csv")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-09-20_21")
    args = ap.parse_args()

    in_path = Path(f"data/in/{args.rodada}/matches_source.csv")
    out_path = Path(f"data/out/{args.rodada}/matches.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists() or in_path.stat().st_size == 0:
        raise RuntimeError(
            f"[ingest_matches] Arquivo-fonte ausente/vazio: {in_path}\n"
            f"Crie-o com as colunas: match_id,home,away[,date]"
        )

    df = pd.read_csv(in_path)
    df = normalize(df)

    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise RuntimeError(f"[ingest_matches] Colunas faltando no fonte: {missing}")
    if df.empty:
        raise RuntimeError("[ingest_matches] Nenhuma partida no arquivo-fonte.")

    cols = ["match_id","home","away"] + (["date"] if "date" in df.columns else [])
    df[cols].to_csv(out_path, index=False)
    print(f"[ingest_matches] OK: {len(df)} linhas -> {out_path}")

if __name__ == "__main__":
    main()
