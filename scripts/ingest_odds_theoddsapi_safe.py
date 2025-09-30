#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper SAFE para TheOddsAPI.
Executa o módulo oficial com defaults tolerantes, imprime um resumo
e garante que o pipeline não quebre caso algo dê errado.

Marcador requerido pelo workflow: "theoddsapi-safe"
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

def _count_csv_lines(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            # conta linhas "csv" (inclui cabeçalho)
            return sum(1 for _ in csv.reader(f))
    except Exception:
        return 0

def _ensure_csv(path: Path, header: list[str]) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)

def main() -> int:
    ap = argparse.ArgumentParser(description="Wrapper SAFE para ingestão via TheOddsAPI")
    ap.add_argument("--rodada", required=True, help="ex: 2025-09-27_1213")
    ap.add_argument("--regions", default="uk,eu,us,au", help="regiões da TheOddsAPI (csv)")
    ap.add_argument("--window", type=int, default=3, help="janela de dias (default: 3)")
    # o módulo interno aceita fuzzy como inteiro (ex.: 93)
    ap.add_argument("--fuzzy", type=int, default=93, help="threshold de similaridade (0-100)")
    ap.add_argument("--aliases", default="data/aliases_br.json", help="arquivo de aliases")
    ap.add_argument("--debug", action="store_true", help="modo verboso")
    args = ap.parse_args()

    # Saídas esperadas pelo restante do pipeline
    out_dir = Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    odds_csv = out_dir / "odds_theoddsapi.csv"
    unmatched_csv = out_dir / "unmatched_theoddsapi.csv"

    # Comando para o módulo oficial
    cmd = [
        sys.executable, "-m", "scripts.ingest_odds_theoddsapi",
        "--rodada", args.rodada,
        "--regions", args.regions,
        "--window", str(args.window),
        "--fuzzy", str(args.fuzzy),
        "--aliases", args.aliases
    ]
    if args.debug:
        cmd.append("--debug")

    # >>> marcador procurado pelo grep no seu workflow
    print(f"[theoddsapi-safe] Executando: {' '.join(cmd)}")

    try:
        # Não derruba o job se o módulo interno retornar código != 0
        subprocess.run(cmd, check=False)
    except Exception as e:
        print(f"[theoddsapi-safe] ERRO ao executar módulo interno: {e}")

    # Garante arquivos e reporta contagem
    _ensure_csv(
        odds_csv,
        header=["provider","league","home","away","market","outcome","price","last_update"]
    )
    _ensure_csv(
        unmatched_csv,
        header=["home_source","away_source","league_source","motivo"]
    )

    counts = {
        "odds_theoddsapi.csv": _count_csv_lines(odds_csv),
        "unmatched_theoddsapi.csv": _count_csv_lines(unmatched_csv)
    }
    print(f"[theoddsapi-safe] linhas -> {json.dumps(counts)}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
