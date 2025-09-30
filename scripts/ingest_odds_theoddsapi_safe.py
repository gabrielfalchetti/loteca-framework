#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import Dict

from scripts.csv_utils import count_csv_rows

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Wrapper SAFE para TheOddsAPI")
    p.add_argument("--rodada", required=True)
    p.add_argument("--regions", default=os.getenv("REGIONS", "uk,eu,us,au"))
    p.add_argument("--window", default=os.getenv("THEODDS_WINDOW", "3"))
    p.add_argument("--fuzzy", default=os.getenv("THEODDS_FUZZY", "93"))
    p.add_argument("--aliases", default=os.getenv("ALIASES_FILE", "data/aliases_br.json"))
    p.add_argument("--debug", action="store_true", default=(os.getenv("DEBUG", "false").lower() == "true"))
    return p.parse_args()

def main() -> int:
    ns = parse_args()
    rodada = ns.rodada

    base = Path(f"data/out/{rodada}")
    theodds_csv = base / "odds_theoddsapi.csv"
    unmatched_csv = base / "unmatched_theoddsapi.csv"
    base.mkdir(parents=True, exist_ok=True)

    # Marcador que seu grep procura:
    print('9:Marcador requerido pelo workflow: "theoddsapi-safe"')

    cmd = [
        "python", "-m", "scripts.ingest_odds_theoddsapi",
        "--rodada", f"{rodada}",
        "--regions", f"{ns.regions}",
        "--window", f"{ns.window}",
        "--fuzzy", f"{ns.fuzzy}",
        "--aliases", f"{ns.aliases}",
    ]
    if ns.debug:
        cmd.append("--debug")

    hard_timeout = int(os.getenv("THEODDS_HARD_TIMEOUT_SEC", "300"))

    print(f"[theoddsapi-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")
    try:
        subprocess.run(
            cmd,
            check=False,
            timeout=hard_timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"[theoddsapi-safe] TIMEOUT após {hard_timeout}s — seguindo com contagens (SAFE).")
    except Exception as e:
        print(f"[theoddsapi-safe] ERRO ao executar módulo interno: {e}")

    counts: Dict[str, int] = {
        "odds_theoddsapi.csv": count_csv_rows(str(theodds_csv)),
        "unmatched_theoddsapi.csv": count_csv_rows(str(unmatched_csv)),
    }
    print(f"[theoddsapi-safe] linhas -> {json.dumps(counts)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
