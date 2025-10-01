# scripts/ingest_odds_theoddsapi_safe.py
"""
Wrapper SAFE para o ingestor da TheOddsAPI.

- Executa o módulo real com timeout controlado.
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
            reader = csv.reader(f)
            rows = list(reader)
            if not rows:
                return 0
            return max(0, len(rows) - 1)
    except Exception:
        return 0


def _collect_counts(out_dir: Path) -> Dict[str, int]:
    files = {
        "odds_theoddsapi.csv": out_dir / "odds_theoddsapi.csv",
        "unmatched_theoddsapi.csv": out_dir / "unmatched_theoddsapi.csv",
    }
    return {name: _count_csv_rows(path) for name, path in files.items()}


def _build_cmd(
    rodada: str,
    regions: str,
    *,
    debug: bool,
    window: int = 3,
    fuzzy: int = 93,
    aliases_path: str = "data/aliases_br.json",
    python_bin: str | None = None,
) -> list[str]:
    py = python_bin or sys.executable
    cmd: list[str] = [
        py, "-m", "scripts.ingest_odds_theoddsapi",
        "--rodada", rodada,
        "--regions", regions,
        "--window", str(window),
        "--fuzzy", str(fuzzy),
        "--aliases", aliases_path,
    ]
    if debug:
        cmd.append("--debug")
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(description="SAFE wrapper para ingest_odds_theoddsapi")
    parser.add_argument("--rodada", required=True)
    parser.add_argument("--regions", default="uk,eu,us,au")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    print('9:Marcador requerido pelo workflow: "theoddsapi-safe"')
    cmd = _build_cmd(args.rodada, args.regions, debug=args.debug)
    print(f"[theoddsapi-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")

    try:
        subprocess.run(
            cmd,
            check=False,
            timeout=args.timeout,
            env={**os.environ},
        )
    except subprocess.TimeoutExpired:
        print(f"[theoddsapi-safe] TIMEOUT após {args.timeout}s — seguindo com contagens (SAFE).")
    except Exception as e:
        print(f"[theoddsapi-safe] ERRO ao executar módulo interno: {e}")

    out_dir = Path("data/out") / args.rodada
    counts = _collect_counts(out_dir)
    print(f"[theoddsapi-safe] linhas -> {json.dumps(counts, ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
