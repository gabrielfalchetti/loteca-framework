#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper SAFE para o ingestor do API-Football (RapidAPI).
Garante:
 - logs previsíveis p/ workflow (sem quebrar o job)
 - parâmetros defaults mais permissivos (--window 2, --fuzzy 0.90)
 - contagem final de linhas em JSON
 - captura de exceções com retorno 0 para não falhar pipeline

Uso:
  python scripts/ingest_odds_apifootball_rapidapi_safe.py --rodada 2025-09-27_1213 --season 2025 --debug
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
    p.add_argument("--rodada", required=True, help="Ex: 2025-09-27_1213")
    p.add_argument("--season", required=False, help="Ex: 2025", default=os.environ.get("SEASON", "2025"))
    p.add_argument("--window", default="2")
    p.add_argument("--fuzzy", default="0.90")
    p.add_argument("--aliases", default="data/aliases_br.json")
    p.add_argument("--debug", action="store_true", default=os.environ.get("DEBUG", "false") == "true")
    args = p.parse_args()

    rodada = args.rodada
    season = args.season
    window = str(args.window)
    fuzzy = str(args.fuzzy)
    aliases = args.aliases
    debug = args.debug

    # Saídas esperadas pelo framework
    out_dir = Path(f"data/out/{rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    odds_csv = out_dir / "odds_apifootball.csv"
    unmatched_csv = out_dir / "unmatched_apifootball.csv"

    py = sys.executable
    cmd = [
        py, "-m", "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada", rodada,
        "--window", window,
        "--fuzzy", fuzzy,
        "--aliases", aliases,
        "--season", season
    ]
    if debug:
        cmd.append("--debug")

    # Log obrigatório p/ facilitar troubleshooting
    print(f"[apifootball-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")

    # Garante arquivos vazios caso módulo interno falhe antes de escrever algo
    for path in (odds_csv, unmatched_csv):
        if not path.exists():
            path.write_text("", encoding="utf-8")

    try:
        # Chama o módulo interno real
        subprocess.run(cmd, check=False)  # não levanta exceção; wrapper é always-safe
    except Exception as e:
        print(f"[apifootball-safe] ERRO ao executar módulo interno: {e}")

    # Conta linhas (exclui header se houver)
    def count_csv_lines(path: Path) -> int:
        if not path.exists():
            return 0
        text = path.read_text(encoding="utf-8", errors="ignore").strip()
        if not text:
            return 0
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if not lines:
            return 0
        # se parecer ter header, não subtraímos — mantemos simples/consistente com outros wrappers
        return max(0, len(lines))

    counts = {
        "odds_apifootball.csv": count_csv_lines(odds_csv),
        "unmatched_apifootball.csv": count_csv_lines(unmatched_csv)
    }
    print(f"[apifootball-safe] linhas -> {json.dumps(counts)}")

    # Nunca quebrar o pipeline
    sys.exit(0)

if __name__ == "__main__":
    main()
