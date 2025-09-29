#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wrapper seguro para TheOddsAPI.
- Repassa os args ao scripts/ingest_odds_theoddsapi.py original.
- Se o script original falhar (exit != 0 ou exceção), cria um CSV vazio
  com header em data/out/<RODADA>/odds_theoddsapi.csv para não quebrar o fluxo.

Uso:
  python scripts/ingest_odds_theoddsapi_safe.py --rodada "2025-09-27_1213" --regions "uk,eu,us,au" --debug
"""

import argparse
import csv
import os
import subprocess
import sys
from datetime import datetime

DEFAULT_HEADER = [
    # header genérico e estável; consumidores podem ignorar colunas extras
    "match_id",
    "home_team",
    "away_team",
    "bookmaker",
    "market",
    "selection",
    "price",
    "source",
    "last_update",
]

def ensure_dirs(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def write_empty_csv(csv_path: str, header=DEFAULT_HEADER) -> None:
    ensure_dirs(csv_path)
    # cria um CSV válido com apenas o header
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Identificador da rodada (ex: 2025-09-27_1213)")
    parser.add_argument("--regions", default="uk,eu,us,au", help="Regiões TheOddsAPI")
    parser.add_argument("--debug", action="store_true", help="Modo debug")
    args, unknown = parser.parse_known_args()

    rodada = args.rodada
    regions = args.regions
    debug = args.debug

    out_csv = os.path.join("data", "out", rodada, "odds_theoddsapi.csv")

    # Comando para chamar o script original (mantém compatibilidade com o repo atual)
    cmd = [
        sys.executable,
        os.path.join("scripts", "ingest_odds_theoddsapi.py"),
        "--rodada", rodada,
        "--regions", regions,
    ]
    if debug:
        cmd.append("--debug")
    # repassa quaisquer args desconhecidos (mantém forwards-compat)
    cmd += unknown

    if debug:
        print(f"[theoddsapi-safe] Executando: {' '.join(cmd)}")

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if debug:
            print("[theoddsapi-safe] STDOUT do original:\n" + (proc.stdout or ""))
            print("[theoddsapi-safe] STDERR do original:\n" + (proc.stderr or ""))

        if proc.returncode != 0:
            # Falhou → garante CSV vazio
            if debug:
                print(f"[theoddsapi-safe] Processo retornou código {proc.returncode}. "
                      f"Criando CSV vazio em {out_csv}")
            write_empty_csv(out_csv)
            # Não propaga erro para não quebrar o job
            return 0

        # Sucesso do original → ainda assim garante existência do arquivo (por segurança)
        if not os.path.exists(out_csv):
            if debug:
                print(f"[theoddsapi-safe] Script original terminou OK, mas {out_csv} não foi gerado. "
                      "Criando CSV vazio com header.")
            write_empty_csv(out_csv)

        if debug:
            print(f"[theoddsapi-safe] OK. Arquivo garantido em {out_csv}")
        return 0

    except Exception as e:
        # Qualquer exceção inesperada → garante CSV e segue
        if debug:
            print(f"[theoddsapi-safe] Exceção: {e!r}. Criando CSV vazio em {out_csv}")
        write_empty_csv(out_csv)
        return 0

if __name__ == "__main__":
    sys.exit(main())
