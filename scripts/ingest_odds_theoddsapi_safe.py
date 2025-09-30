#!/usr/bin/env python
from __future__ import annotations

import sys, subprocess, json
from pathlib import Path
import argparse

def run(cmd: list[str]) -> int:
    print(f"[theoddsapi-safe] Executando: {' '.join(cmd)}")
    return subprocess.call(cmd)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--window", type=int, default=3)
    ap.add_argument("--fuzzy", type=int, default=93)
    ap.add_argument("--aliases", type=str, default="data/aliases_br.json")
    args = ap.parse_args()

    rc = run([
        sys.executable, "-m", "scripts.ingest_odds_theoddsapi",
        "--rodada", args.rodada,
        "--regions", args.regions,
        "--window", str(args.window),
        "--fuzzy", str(args.fuzzy),
        "--aliases", args.aliases
    ] + (["--debug"] if args.debug else []))

    base_out = Path("data/out") / args.rodada
    counts = {
        "odds_theoddsapi.csv": (base_out / "odds_theoddsapi.csv").exists() and sum(1 for _ in open(base_out / "odds_theoddsapi.csv", "r", encoding="utf-8")) - 1 or 0,
        "unmatched_theoddsapi.csv": (base_out / "unmatched_theoddsapi.csv").exists() and sum(1 for _ in open(base_out / "unmatched_theoddsapi.csv", "r", encoding="utf-8")) - 1 or 0
    }
    print(f"[theoddsapi-safe] linhas -> {json.dumps(counts)}")
    raise SystemExit(0 if rc == 0 else 0)

if __name__ == "__main__":
    main()
