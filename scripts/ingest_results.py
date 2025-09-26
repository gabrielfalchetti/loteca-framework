# scripts/ingest_results.py
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Ingestão de resultados oficiais para a rodada")
    ap.add_argument("--rodada", required=True, help='Ex.: 2025-10-05_14')
    args = ap.parse_args()

    src = Path(f"data/in/{args.rodada}/results.csv")
    dst = Path(f"data/out/{args.rodada}/results.csv")
    if not src.exists() or src.stat().st_size == 0:
        raise RuntimeError(f"[ingest_results] Arquivo ausente/vazio: {src}\nFormato: match_id,resultado (em {{1,X,2}})")

    df = pd.read_csv(src)
    need = {"match_id","resultado"}
    if not need.issubset(df.columns):
        raise RuntimeError(f"[ingest_results] Colunas faltando em {src}. Esperado: {need}")

    df["match_id"] = pd.to_numeric(df["match_id"], errors="coerce").astype("Int64")
    df["resultado"] = df["resultado"].astype(str).str.upper().str.strip()
    if not set(df["resultado"].dropna().unique()).issubset({"1","X","2"}):
        raise RuntimeError("[ingest_results] 'resultado' precisa estar em {1,X,2}")

    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False)
    print(f"[ingest_results] OK -> {dst}")

if __name__ == "__main__":
    main()
