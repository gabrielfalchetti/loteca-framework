#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper "seguro" para TheOddsAPI.
- Exige THEODDS_API_KEY no ambiente (fail-fast)
- Chama o script original ingest_odds_theoddsapi.py
- Confere se o CSV de saída tem linhas (>0). Se vazio -> erro
Uso:
  python scripts/ingest_odds_theoddsapi_safe.py --rodada RODADA --regions "uk,eu,us,au" [--debug]
"""

import argparse
import os
import subprocess
import sys
import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True)
    parser.add_argument("--regions", default="uk,eu,us,au")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    api_key = os.getenv("THEODDS_API_KEY")
    if not api_key:
        print("[theoddsapi-safe] ERRO: THEODDS_API_KEY não definido. Configure o secret no GitHub.", flush=True)
        sys.exit(1)

    # Executa o script original
    cmd = [
        sys.executable, "scripts/ingest_odds_theoddsapi.py",
        "--rodada", args.rodada,
        "--regions", args.regions
    ]
    if args.debug:
        cmd.append("--debug")

    print(f"[theoddsapi-safe] Executando: {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if args.debug:
        if proc.stdout:
            print("[theoddsapi-safe] STDOUT do original:\n" + proc.stdout, flush=True)
        if proc.stderr:
            print("[theoddsapi-safe] STDERR do original:\n" + proc.stderr, flush=True)

    if proc.returncode != 0:
        print("[theoddsapi-safe] ERRO: ingest_odds_theoddsapi.py retornou código diferente de zero.", flush=True)
        sys.exit(proc.returncode)

    out_path = f"data/out/{args.rodada}/odds_theoddsapi.csv"
    if not os.path.exists(out_path):
        print(f"[theoddsapi-safe] ERRO: saída {out_path} não foi criada.", flush=True)
        sys.exit(1)

    try:
        df = pd.read_csv(out_path)
    except Exception as e:
        print(f"[theoddsapi-safe] ERRO lendo {out_path}: {e}", flush=True)
        sys.exit(1)

    if df.empty:
        print("[theoddsapi-safe] ERRO: nenhuma odd retornada pelo TheOddsAPI para os jogos listados.", flush=True)
        sys.exit(1)

    print(f"[theoddsapi-safe] OK. Arquivo garantido em {out_path}", flush=True)

if __name__ == "__main__":
    main()
