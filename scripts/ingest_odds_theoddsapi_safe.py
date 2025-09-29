#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper 'safe' para o coletor do TheOddsAPI.
- Garante variáveis, diretórios e logs.
- NUNCA derruba o job se o provedor retornar 0 linhas: emite AVISO e segue o pipeline
  (o RapidAPI pode cobrir).
"""

import os
import sys
import subprocess
import argparse
import pandas as pd

def log(msg: str):
    print(f"[theoddsapi-safe] {msg}", flush=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True)
    parser.add_argument("--regions", default="uk,eu,us,au")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    rodada = args.rodada
    regions = args.regions

    api_key = os.getenv("THEODDS_API_KEY", "").strip()
    if not api_key:
        print("THEODDS_API_KEY nao definido", file=sys.stderr)
        sys.exit(1)

    # Executa o coletor principal
    cmd = [
        sys.executable, "scripts/ingest_odds_theoddsapi.py",
        "--rodada", rodada,
        "--regions", regions
    ]
    if args.debug:
        cmd.append("--debug")

    log(f"Executando: {' '.join(cmd)}")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.stdout:
            print(proc.stdout)
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
    except Exception as e:
        log(f"ERRO ao executar coletor principal: {e}")
        # Segue como warning para nao quebrar pipeline
        sys.exit(0)

    # Verifica se gerou arquivo e se tem linhas
    out_csv = f"data/out/{rodada}/odds_theoddsapi.csv"
    rows = 0
    if os.path.exists(out_csv):
        try:
            rows = len(pd.read_csv(out_csv))
        except Exception:
            rows = 0

    if rows == 0:
        log("AVISO: sem odds retornadas pelo TheOddsAPI (0 linhas). Seguindo pipeline; outro provedor pode cobrir.")
        # Continua o pipeline normalmente
        sys.exit(0)

    log(f"OK. Arquivo garantido em {out_csv} ({rows} linhas)")
    sys.exit(0)

if __name__ == "__main__":
    main()
