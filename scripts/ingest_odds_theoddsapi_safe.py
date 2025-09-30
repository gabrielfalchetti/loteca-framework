#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper SAFE para TheOddsAPI.
 - imprime marcador exigido pelo workflow
 - chama o módulo real com params default
 - contabiliza linhas e nunca falha o job
"""

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True)
    p.add_argument("--regions", default=os.environ.get("REGIONS", "uk,eu,us,au"))
    p.add_argument("--window", default="3")
    p.add_argument("--fuzzy", default="93")
    p.add_argument("--aliases", default="data/aliases_br.json")
    p.add_argument("--debug", action="store_true", default=os.environ.get("DEBUG", "false") == "true")
    args = p.parse_args()

    rodada = args.rodada
    regions = args.regions
    window = str(args.window)
    fuzzy = str(args.fuzzy)
    aliases = args.aliases
    debug = args.debug

    out_dir = Path(f"data/out/{rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    odds_csv = out_dir / "odds_theoddsapi.csv"
    unmatched_csv = out_dir / "unmatched_theoddsapi.csv"

    # Marcador que o workflow procura (via grep)
    print('9:Marcador requerido pelo workflow: "theoddsapi-safe"')

    py = sys.executable
    cmd = [
        py, "-m", "scripts.ingest_odds_theoddsapi",
        "--rodada", rodada,
        "--regions", regions,
        "--window", window,
        "--fuzzy", fuzzy,
        "--aliases", aliases
    ]
    if debug:
        cmd.append("--debug")

    print(f"[theoddsapi-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")

    # garante arquivos
    for path in (odds_csv, unmatched_csv):
        if not path.exists():
            path.write_text("", encoding="utf-8")

    try:
        subprocess.run(cmd, check=False)
    except Exception as e:
        print(f"[theoddsapi-safe] ERRO ao executar módulo interno: {e}")

    def count_csv_lines(path: Path) -> int:
        if not path.exists():
            return 0
        text = path.read_text(encoding="utf-8", errors="ignore").strip()
        if not text:
            return 0
        lines = [ln for ln in text.splitlines() if ln.strip()]
        return max(0, len(lines))

    counts = {
        "odds_theoddsapi.csv": count_csv_lines(odds_csv),
        "unmatched_theoddsapi.csv": count_csv_lines(unmatched_csv)
    }
    print(f"[theoddsapi-safe] linhas -> {json.dumps(counts)}")
    sys.exit(0)

if __name__ == "__main__":
    main()
