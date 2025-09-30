#!/usr/bin/env python
from __future__ import annotations

import sys, subprocess, json
from pathlib import Path
import argparse

def run(cmd: list[str]) -> int:
    print(f"[apifootball-safe] Executando: {' '.join(cmd)}")
    return subprocess.call(cmd)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--window", type=int, default=1)     # curto é melhor pra BR
    ap.add_argument("--fuzzy", type=float, default=0.92) # 0.90–0.94
    ap.add_argument("--aliases", type=str, default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    cmd = [
        sys.executable, "-m", "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada", args.rodada,
        "--window", str(args.window),
        "--fuzzy", str(args.fuzzy),
        "--aliases", args.aliases
    ]
    if args.season: cmd += ["--season", str(args.season)]
    if args.debug:  cmd += ["--debug"]

    rc = run(cmd)

    base_out = Path("data/out") / args.rodada
    counts = {
        "odds_apifootball.csv": (base_out / "odds_apifootball.csv").exists() and sum(1 for _ in open(base_out / "odds_apifootball.csv", "r", encoding="utf-8")) - 1 or 0,
        "unmatched_apifootball.csv": (base_out / "unmatched_apifootball.csv").exists() and sum(1 for _ in open(base_out / "unmatched_apifootball.csv", "r", encoding="utf-8")) - 1 or 0
    }
    print(f"[apifootball-safe] linhas -> {json.dumps(counts)}")

    raise SystemExit(0 if rc == 0 else 0)

if __name__ == "__main__":
    main()
