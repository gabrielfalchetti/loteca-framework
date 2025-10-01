# scripts/ingest_odds_apifootball_rapidapi_safe.py
"""
Wrapper SAFE para o ingestor do API-Football via RapidAPI.

- Executa o módulo real com timeout controlado (padrão 120s, alterável por env APIFOOT_TIMEOUT).
- Nunca falha o job por exceção do módulo interno.
- Imprime contagens de linhas geradas ao final.
- Sem dependências externas (contagem de CSV inline).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict

def _count_csv_rows(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            # conta linhas de dados (ignora header)
            n = -1
            for n, _ in enumerate(r, start=0):
                pass
            return max(0, n)  # n será -1 se vazio -> 0
    except Exception:
        return 0

def _collect_counts(out_dir: Path) -> Dict[str, int]:
    files = {
        "odds_apifootball.csv": out_dir / "odds_apifootball.csv",
        "unmatched_apifootball.csv": out_dir / "unmatched_apifootball.csv",
    }
    return {name: _count_csv_rows(path) for name, path in files.items()}

def _build_cmd(
    rodada: str,
    season: str | int | None,
    *,
    debug: bool,
    window: int = 2,
    fuzzy: float = 0.90,
    aliases_path: str = "data/aliases_br.json",
    python_bin: str | None = None,
) -> list[str]:
    py = python_bin or sys.executable
    cmd: list[str] = [
        py, "-m", "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada", rodada,
        "--window", str(window),
        "--fuzzy", str(fuzzy),
        "--aliases", aliases_path,
    ]
    if season is not None:
        cmd += ["--season", str(season)]
    if debug:
        cmd.append("--debug")
    return cmd

def main() -> int:
    parser = argparse.ArgumentParser(description="SAFE wrapper para ingest_odds_apifootball_rapidapi")
    parser.add_argument("--rodada", required=True)
    parser.add_argument("--season", default=None)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--timeout", type=int, default=int(os.getenv("APIFOOT_TIMEOUT", "120")))
    args = parser.parse_args()

    cmd = _build_cmd(args.rodada, args.season, debug=args.debug)
    print(f"[apifootball-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")

    try:
        subprocess.run(
            cmd,
            check=False,
            timeout=args.timeout,
            env={**os.environ},
        )
    except subprocess.TimeoutExpired:
        print(f"[apifootball-safe] TIMEOUT após {args.timeout}s — seguindo com contagens (SAFE).")
    except Exception as e:
        print(f"[apifootball-safe] ERRO ao executar módulo interno: {e}")

    out_dir = Path("data/out") / args.rodada
    counts = _collect_counts(out_dir)
    print(f"[apifootball-safe] linhas -> {json.dumps(counts, ensure_ascii=False)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
