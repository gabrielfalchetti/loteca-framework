# scripts/ingest_odds_apifootball_rapidapi_safe.py
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from typing import Dict

from scripts.csv_utils import count_csv_rows


def main() -> None:
    ap = argparse.ArgumentParser(description="[SAFE] Wrapper para ingest_apifootball via RapidAPI")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--debug", action="store_true")
    # flags legadas (repasse)
    ap.add_argument("--window", type=int, default=2)
    ap.add_argument("--fuzzy", type=float, default=0.90)
    ap.add_argument("--aliases", default="data/aliases_br.json")

    args = ap.parse_args()

    # Comando alvo
    cmd = [
        sys.executable,
        "-m",
        "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada",
        args.rodada,
        "--season",
        str(args.season),
        "--window",
        str(args.window),
        "--fuzzy",
        str(args.fuzzy),
        "--aliases",
        args.aliases,
    ]
    if args.debug:
        cmd.append("--debug")

    print(f"[apifootball-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")

    counts: Dict[str, int] = {
        "odds_apifootball.csv": 0,
        "unmatched_apifootball.csv": 0,
    }
    try:
        subprocess.run(cmd, check=True, timeout=args.timeout)
    except subprocess.TimeoutExpired:
        print(f"[apifootball-safe] TIMEOUT após {args.timeout}s — seguindo com contagens (SAFE).")
    except Exception as e:
        print(f"[apifootball-safe] ERRO ao executar módulo interno: {e}")

    # Contagens (para o step do workflow)
    base = f"data/out/{args.rodada}"
    counts["odds_apifootball.csv"] = count_csv_rows(f"{base}/odds_apifootball.csv")
    counts["unmatched_apifootball.csv"] = count_csv_rows(f"{base}/unmatched_apifootball.csv")
    print(f"[apifootball-safe] linhas -> {json.dumps(counts, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
