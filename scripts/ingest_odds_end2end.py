#!/usr/bin/env python
from __future__ import annotations

# HOTFIX de import
import sys, subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse

def run(cmd: list[str]) -> int:
    print(f"[end2end] RUN: {' '.join(cmd)}")
    return subprocess.call(cmd)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--consensus-allow-empty", action="store_true", help="Não aborta se não houver odds.")
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--window", type=int, default=14)
    ap.add_argument("--fuzzy", type=float, default=0.90)
    ap.add_argument("--aliases", type=str, default="data/aliases_br.json")
    args = ap.parse_args()

    # 1) TheOddsAPI (usar mesmos parâmetros de matching)
    rc1 = run([sys.executable, "-m", "scripts.ingest_odds_theoddsapi",
               "--rodada", args.rodada,
               "--regions", args.regions,
               "--window", str(args.window),
               "--fuzzy", str(int(args.fuzzy*100)),
               "--aliases", args.aliases] + (["--debug"] if args.debug else []))
    if rc1 != 0:
        print("[end2end] AVISO: TheOddsAPI falhou (segue).")

    # 2) API-Football (RapidAPI)
    cmd2 = [sys.executable, "-m", "scripts.ingest_odds_apifootball_rapidapi",
            "--rodada", args.rodada,
            "--window", str(args.window),
            "--fuzzy", str(args.fuzzy),
            "--aliases", args.aliases]
    if args.season: cmd2 += ["--season", str(args.season)]
    if args.debug: cmd2 += ["--debug"]
    rc2 = run(cmd2)
    if rc2 != 0:
        print("[end2end] AVISO: API-Football falhou (segue).")

    # 3) Consenso (sempre com allow-empty quando pedido ou em debug)
    cmd3 = [sys.executable, "-m", "scripts.consensus_odds", "--rodada", args.rodada]
    if args.consensus_allow_empty or args.debug:
        cmd3.append("--allow-empty")
    rc3 = run(cmd3)
    if rc3 != 0:
        print("[end2end] ERRO: nenhum provedor retornou odds (consenso).")
        raise SystemExit(1)

    print("[end2end] OK — ingest + consenso concluídos.")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
