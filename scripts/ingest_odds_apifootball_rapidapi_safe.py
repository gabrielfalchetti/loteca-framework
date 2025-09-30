#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper SAFE para API-Football (RapidAPI).
Objetivo: rodar o módulo interno com parâmetros controlados, padronizar logs e
nunca derrubar o job. Saída final sempre inclui o resumo de linhas dos CSVs.

Uso típico (o workflow já faz isso):
  python scripts/ingest_odds_apifootball_rapidapi_safe.py \
    --rodada 2025-09-27_1213 \
    --season 2025 \
    --window 2 \
    --fuzzy 0.90 \
    --aliases data/aliases_br.json \
    --debug
"""

from __future__ import annotations
import argparse
import csv
import json
import os
import shlex
import subprocess
import sys
from typing import Dict, Tuple

# 9:Marcador requerido pelo workflow: "apifootball-safe"

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Wrapper SAFE para ingest RapidAPI (API-Football).")
    p.add_argument("--rodada", required=True, help="Carimbo da rodada (ex.: 2025-09-27_1213)")
    p.add_argument("--season", required=False, default=None, help="Ano da temporada (ex.: 2025)")
    p.add_argument("--window", type=int, default=2, help="Janela (dias) para matching (default: 2)")
    p.add_argument("--fuzzy", type=float, default=0.90, help="Threshold de similaridade (default: 0.90)")
    p.add_argument("--aliases", default="data/aliases_br.json", help="Arquivo de aliases (default: data/aliases_br.json)")
    p.add_argument("--debug", action="store_true", help="Liga logs detalhados")
    return p.parse_args()


def _count_csv_rows(path: str) -> int:
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            rdr = csv.reader(f)
            rows = list(rdr)
        if not rows:
            return 0
        # desconta header se existir
        return max(0, len(rows) - 1)
    except Exception:
        return 0


def _target_paths(rodada: str) -> Tuple[str, str]:
    out_dir = os.path.join("data", "out", rodada)
    odds_path = os.path.join(out_dir, "odds_apifootball.csv")
    unmatched_path = os.path.join(out_dir, "unmatched_apifootball.csv")
    return odds_path, unmatched_path


def main() -> int:
    ns = parse_args()

    odds_path, unmatched_path = _target_paths(ns.rodada)

    # Monta comando do módulo interno
    cmd = [
        sys.executable,
        "-m",
        "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada", ns.rodada,
        "--window", str(ns.window),
        "--fuzzy", str(ns.fuzzy),
        "--aliases", ns.aliases,
    ]

    if ns.season:
        cmd += ["--season", str(ns.season)]
    if ns.debug:
        cmd.append("--debug")

    print(f"[apifootball-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")

    try:
        # Se o módulo interno falhar, NÃO derruba o job; apenas loga e segue.
        completed = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        # Espelha a saída do módulo (útil para debug no Actions)
        if completed.stdout:
            # Para não poluir demais, ainda assim mostramos tudo (você pediu logs completos)
            print(completed.stdout.rstrip())
        ret = completed.returncode
        if ret != 0:
            print(f"[apifootball-safe] AVISO: módulo interno retornou código {ret}.")
    except Exception as e:
        print(f"[apifootball-safe] ERRO ao executar módulo interno: {e}")

    # Contabiliza arquivos de saída
    odds_rows = _count_csv_rows(odds_path)
    unmatched_rows = _count_csv_rows(unmatched_path)

    counts: Dict[str, int] = {
        "odds_apifootball.csv": odds_rows,
        "unmatched_apifootball.csv": unmatched_rows,
    }
    print(f"[apifootball-safe] linhas -> {json.dumps(counts)}")

    # Nunca falha: padronizamos exit 0 para não interromper o pipeline
    return 0


if __name__ == "__main__":
    sys.exit(main())
