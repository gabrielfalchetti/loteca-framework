#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper "seguro" para API-Football (RapidAPI).
- Exige RAPIDAPI_KEY no ambiente (fail-fast)
- Chama o script original ingest_odds_apifootball_rapidapi.py
- Confere se o CSV de saída tem linhas (>0). Se vazio -> erro
Uso:
  python scripts/ingest_odds_apifootball_rapidapi_safe.py --rodada RODADA [demais args repassados]
"""

import argparse
import os
import subprocess
import sys
import pandas as pd

def passthrough_unknown(args_list):
    # Repassa quaisquer flags/pares ao script original
    return args_list

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True)
    # Recebe quaisquer outros args e repassa ao original
    known, unknown = parser.parse_known_args()

    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("[apifootball-safe] ERRO: RAPIDAPI_KEY não definido. Configure o secret no GitHub.", flush=True)
        sys.exit(1)

    cmd = [sys.executable, "scripts/ingest_odds_apifootball_rapidapi.py", "--rodada", known.rodada]
    cmd += passthrough_unknown(unknown)

    print(f"[apifootball-safe] Executando: {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, capture_output=True, text=True)

    # Sempre espelha logs do original para facilitar debug
    if proc.stdout:
        print("[apifootball-safe] STDOUT do original:\n" + proc.stdout, flush=True)
    if proc.stderr:
        print("[apifootball-safe] STDERR do original:\n" + proc.stderr, flush=True)

    if proc.returncode != 0:
        print("[apifootball-safe] ERRO: ingest_odds_apifootball_rapidapi.py retornou erro.", flush=True)
        sys.exit(proc.returncode)

    out_path = f"data/out/{known.rodada}/odds_apifootball.csv"
    if not os.path.exists(out_path):
        print(f"[apifootball-safe] ERRO: saída {out_path} não foi criada.", flush=True)
        sys.exit(1)

    try:
        df = pd.read_csv(out_path)
    except Exception as e:
        print(f"[apifootball-safe] ERRO lendo {out_path}: {e}", flush=True)
        sys.exit(1)

    if df.empty:
        print("[apifootball-safe] ERRO: nenhuma odd retornada pelo API-Football para os jogos listados.", flush=True)
        sys.exit(1)

    print(f"[apifootball-safe] OK. Arquivo garantido em {out_path}", flush=True)

if __name__ == "__main__":
    main()
