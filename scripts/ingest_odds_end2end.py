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
    args = ap.parse_args()

    # 1) TheOddsAPI
    rc1 = run([sys.executable, "-m", "scripts.ingest_odds_theoddsapi",
               "--rodada", args.rodada, "--regions", args.regions] + (["--debug"] if args.debug else []))
    if rc1 != 0:
        print("[end2end] AVISO: TheOddsAPI falhou (segue).")

    # 2) API-Football (RapidAPI)
    rc2 = run([sys.executable, "-m", "scripts.ingest_odds_apifootball_rapidapi",
               "--rodada", args.rodada] + (["--debug"] if args.debug else []))
    if rc2 != 0:
        print("[end2end] AVISO: API-Football falhou (segue).")

    # 3) Consenso (com tolerância)
    cmd = [sys.executable, "-m", "scripts.consensus_odds", "--rodada", args.rodada]
    if args.consensus_allow_empty or args.debug:
        cmd.append("--allow-empty")  # no modo debug também não derruba
    rc3 = run(cmd)
    if rc3 != 0:
        print("[end2end] ERRO: nenhum provedor retornou odds (consenso).")
        raise SystemExit(1)

    print("[end2end] OK — ingest + consenso concluídos.")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
