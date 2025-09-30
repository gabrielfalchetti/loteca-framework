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
    p = argparse.ArgumentParser(description="Wrapper SAFE para API-Football via RapidAPI")
    p.add_argument("--rodada", required=True)
    p.add_argument("--season", required=False, default=os.getenv("SEASON", "2025"))
    p.add_argument("--window", required=False, default=os.getenv("RAPIDAPI_WINDOW", "2"))
    p.add_argument("--fuzzy", required=False, default=os.getenv("RAPIDAPI_FUZZY", "0.90"))
    p.add_argument("--aliases", required=False, default=os.getenv("ALIASES_FILE", "data/aliases_br.json"))
    p.add_argument("--debug", action="store_true", default=(os.getenv("DEBUG", "false").lower() == "true"))
    return p.parse_args()

def main() -> int:
    ns = parse_args()
    rodada = ns.rodada

    # Onde os CSVs são gerados pelo módulo “quente”
    base = Path(f"data/out/{rodada}")
    apifoot_csv = base / "odds_apifootball.csv"
    unmatched_csv = base / "unmatched_apifootball.csv"
    base.mkdir(parents=True, exist_ok=True)

    # Comando do módulo “quente”
    cmd = [
        "python", "-m", "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada", f"{rodada}",
        "--window", f"{ns.window}",
        "--fuzzy", f"{ns.fuzzy}",
        "--aliases", f"{ns.aliases}",
        "--season", f"{ns.season}",
    ]
    if ns.debug:
        cmd.append("--debug")

    # Timeout duro configurável
    hard_timeout = int(os.getenv("RAPIDAPI_HARD_TIMEOUT_SEC", "300"))

    print(f"[apifootball-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")
    try:
        # Captura stdio para manter o log compacto
        subprocess.run(
            cmd,
            check=False,
            timeout=hard_timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"[apifootball-safe] TIMEOUT após {hard_timeout}s — seguindo com contagens (SAFE).")
    except Exception as e:
        print(f"[apifootball-safe] ERRO ao executar módulo interno: {e}")

    counts: Dict[str, int] = {
        "odds_apifootball.csv": count_csv_rows(str(apifoot_csv)),
        "unmatched_apifootball.csv": count_csv_rows(str(unmatched_csv)),
    }
    print(f"[apifootball-safe] linhas -> {json.dumps(counts)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
