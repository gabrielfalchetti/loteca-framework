#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper seguro para ingestão das odds do TheOddsAPI.
- FALHA se THEODDS_API_KEY não estiver definido.
- Executa o scripts/ingest_odds_theoddsapi.py com os mesmos argumentos.
- FALHA se o CSV final (data/out/<rodada>/odds_theoddsapi.csv) existir mas tiver 0 linhas de dados.

Uso:
  python scripts/ingest_odds_theoddsapi_safe.py --rodada RODADA --regions "uk,eu,us,au" --debug
"""

import argparse
import os
import sys
import subprocess
from pathlib import Path

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="Identificador da rodada (ex: 2025-09-27_1213)")
    p.add_argument("--regions", default="uk,eu,us,au", help="Regiões para TheOddsAPI (ex: uk,eu,us,au)")
    p.add_argument("--debug", action="store_true", help="Modo verboso")
    return p.parse_args()

def main():
    args = parse_args()
    api_key = os.environ.get("THEODDS_API_KEY", "")
    if not api_key:
        print("[theoddsapi-safe] ERRO: THEODDS_API_KEY não definido. Configure o secret no GitHub.", file=sys.stderr)
        sys.exit(1)

    # Caminho de saída esperado pelo script original
    out_csv = Path(f"data/out/{args.rodada}/odds_theoddsapi.csv")
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # Monta comando para o script original
    cmd = [
        sys.executable, "scripts/ingest_odds_theoddsapi.py",
        "--rodada", args.rodada,
        "--regions", args.regions
    ]
    if args.debug:
        cmd.append("--debug")

    if args.debug:
        print(f"[theoddsapi-safe] Executando: {' '.join(cmd)}")

    # Executa repassando o ambiente (inclui THEODDS_API_KEY)
    proc = subprocess.run(cmd, env=os.environ.copy())
    if proc.returncode != 0:
        print(f"[theoddsapi-safe] ERRO: processo original retornou código {proc.returncode}.", file=sys.stderr)
        sys.exit(proc.returncode)

    # Checa se o CSV foi gerado e possui ao menos 1 linha de dados
    if not out_csv.exists():
        print(f"[theoddsapi-safe] ERRO: arquivo não gerado: {out_csv}", file=sys.stderr)
        sys.exit(2)

    try:
        with out_csv.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        num_data = max(0, len(lines) - 1)  # desconta cabeçalho
    except Exception as e:
        print(f"[theoddsapi-safe] ERRO lendo {out_csv}: {e}", file=sys.stderr)
        sys.exit(2)

    if num_data < 1:
        print(f"[theoddsapi-safe] ERRO: sem odds retornadas pelo TheOddsAPI (0 linhas de dados em {out_csv}).", file=sys.stderr)
        sys.exit(3)

    print(f"[theoddsapi-safe] OK. Arquivo com odds reais em {out_csv} (linhas de dados: {num_data})")
    sys.exit(0)

if __name__ == "__main__":
    main()
