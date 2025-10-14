# scripts/enrich_api_football.py
from __future__ import annotations
import argparse
import os
import sys
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True, help="caminho do features.parquet de entrada")
    ap.add_argument("--features_out", required=True, help="caminho do features.parquet de saída (enriquecido)")
    args = ap.parse_args()

    # Pass-through seguro: se não existir, não explode — deixa o upstream acusar
    if not os.path.isfile(args.features_in):
        print(f"[enrich][WARN] arquivo não encontrado: {args.features_in}", file=sys.stderr)
        # cria um parquet vazio com schema mínimo para não quebrar o pipeline
        df = pd.DataFrame()
        df.to_parquet(args.features_out, index=False)
        print(f"[enrich] wrote empty parquet to {args.features_out}")
        return

    try:
        df = pd.read_parquet(args.features_in)
    except Exception as e:
        print(f"[enrich][ERROR] falha lendo {args.features_in}: {e}", file=sys.stderr)
        # ainda assim escrevemos um parquet vazio para não quebrar downstream
        pd.DataFrame().to_parquet(args.features_out, index=False)
        print(f"[enrich] wrote empty parquet to {args.features_out}")
        return

    # Aqui futuramente: chamadas reais ao API-Football (injuries, suspensões, ratings, H2H, etc.)
    # Por enquanto, pass-through:
    df.to_parquet(args.features_out, index=False)
    print(f"[enrich] pass-through: {args.features_in} -> {args.features_out} (sem alterações)")

if __name__ == "__main__":
    main()