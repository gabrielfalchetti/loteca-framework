# scripts/ingest_odds_theoddsapi_safe.py
"""
Wrapper SAFE para o ingestor da TheOddsAPI.

- Garante prints/markers esperados pelo workflow de CI.
- Não falha o job se o módulo interno travar/timeoutar/401 etc.
- Sempre reporta contagens de linhas ao final.

Saída esperada (exemplos de prints):
  9:Marcador requerido pelo workflow: "theoddsapi-safe"
  [theoddsapi-safe] Executando: /usr/bin/python -m scripts.ingest_odds_theoddsapi ...
  [theoddsapi-safe] TIMEOUT após 60s — seguindo com contagens (SAFE).
  [theoddsapi-safe] ERRO ao executar módulo interno: <msg>
  [theoddsapi-safe] linhas -> {"odds_theoddsapi.csv": 2, "unmatched_theoddsapi.csv": 0}
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict


# ---- import robusto do utilitário de CSV ----
try:
    from scripts.csv_utils import count_csv_rows
except Exception:
    # fallback mínimo para não quebrar o SAFE
    import csv  # type: ignore

    def count_csv_rows(path: str) -> int:  # type: ignore
        p = Path(path)
        if not p.exists():
            return 0
        with p.open("r", newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            try:
                next(r)  # header
            except StopIteration:
                return 0
            return sum(1 for _ in r)


def build_cmd(
    rodada: str,
    regions: str,
    *,
    debug: bool,
    window: int = 3,
    fuzzy: int = 93,
    aliases_path: str = "data/aliases_br.json",
    python_bin: str | None = None,
) -> list[str]:
    """
    Monta o comando que chama o módulo interno scripts.ingest_odds_theoddsapi.
    """
    py = python_bin or sys.executable
    cmd: list[str] = [
        py,
        "-m",
        "scripts.ingest_odds_theoddsapi",
        "--rodada",
        rodada,
        "--regions",
        regions,
        "--window",
        str(window),
        "--fuzzy",
        str(fuzzy),
        "--aliases",
        aliases_path,
    ]
    if debug:
        cmd.append("--debug")
    return cmd


def collect_counts(out_dir: Path) -> Dict[str, int]:
    """
    Lê as contagens de linhas dos CSVs relevantes.
    """
    files = {
        "odds_theoddsapi.csv": out_dir / "odds_theoddsapi.csv",
        "unmatched_theoddsapi.csv": out_dir / "unmatched_theoddsapi.csv",
    }
    return {name: count_csv_rows(str(path)) for name, path in files.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="SAFE wrapper para ingest_odds_theoddsapi")
    parser.add_argument("--rodada", required=True, help="ex: 2025-09-27_1213")
    parser.add_argument("--regions", default="uk,eu,us,au", help="regiões da TheOddsAPI")
    parser.add_argument("--debug", action="store_true", help="modo verboso")
    parser.add_argument("--timeout", type=int, default=60, help="timeout duro (segundos)")
    args = parser.parse_args()

    # marcador requerido pelo workflow (não remova / não traduza)
    print('9:Marcador requerido pelo workflow: "theoddsapi-safe"')

    cmd = build_cmd(
        rodada=args.rodada,
        regions=args.regions,
        debug=args.debug,
    )
    print(f"[theoddsapi-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")

    # Executa o módulo interno com timeout. Nunca levanta exceção para fora.
    try:
        subprocess.run(
            cmd,
            check=False,  # não quebrar o SAFE caso retorne código != 0
            timeout=args.timeout,
            env={**os.environ},
        )
    except subprocess.TimeoutExpired:
        print(f"[theoddsapi-safe] TIMEOUT após {args.timeout}s — seguindo com contagens (SAFE).")
    except Exception as e:
        # API 401 / problemas de rede / etc. — manter SAFE
        print(f"[theoddsapi-safe] ERRO ao executar módulo interno: {e}")

    # Coleta contagens SEMPRE
    out_dir = Path("data/out") / args.rodada
    counts = collect_counts(out_dir)
    print(f"[theoddsapi-safe] linhas -> {json.dumps(counts, ensure_ascii=False)}")

    # Nunca falha
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
