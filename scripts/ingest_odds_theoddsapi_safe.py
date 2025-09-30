#!/usr/bin/env python3
import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

def count_lines(p: Path) -> int:
    if not p.exists():
        return 0
    with p.open("r", encoding="utf-8") as f:
        try:
            return sum(1 for _ in csv.reader(f))
        except Exception:
            return 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ex: 2025-09-27_1213")
    ap.add_argument("--season", required=True, help="ex: 2025")
    ap.add_argument("--window", type=int, default=3, help="dias para varrer (default: 3)")
    ap.add_argument("--fuzzy", type=float, default=0.92, help="threshold de similaridade (0-1)")
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    odds_csv = out_dir / "odds_apifootball.csv"
    unmatched_csv = out_dir / "unmatched_apifootball.csv"

    cmd = [
        sys.executable, "-m", "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada", args.rodada,
        "--season", str(args.season),
        "--window", str(args.window),
        "--fuzzy", str(args.fuzzy),
        "--aliases", args.aliases
    ]
    if args.debug:
        cmd.append("--debug")

    print(f"[apifootball-safe] Executando: {' '.join(cmd)}")
    try:
        # Não falha o pipeline se o módulo interno retornar != 0
        subprocess.run(cmd, check=False)
    except Exception as e:
        print(f"[apifootball-safe] ERRO ao executar módulo interno: {e}")

    counts = {
        "odds_apifootball.csv": count_lines(odds_csv),
        "unmatched_apifootball.csv": count_lines(unmatched_csv)
    }
    print(f"[apifootball-safe] linhas -> {json.dumps(counts)}")

    # Se nenhum arquivo existir, cria vazios com cabeçalho mínimo
    if not odds_csv.exists():
        with odds_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["provider","league","home","away","market","outcome","price","last_update"])
    if not unmatched_csv.exists():
        with unmatched_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["home_source","away_source","league_source","motivo"])

if __name__ == "__main__":
    main()
