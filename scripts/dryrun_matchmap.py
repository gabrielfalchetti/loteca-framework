#!/usr/bin/env python
from __future__ import annotations

# HOTFIX de import
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse, csv
from typing import List, Dict
from utils.match_normalize import canonical

def read_matches(path: Path) -> List[Dict[str,str]]:
    if not path.exists(): print(f"[ERRO] {path}"); raise SystemExit(2)
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        needed = {"match_id","home","away"}
        if not needed.issubset(reader.fieldnames or []):
            print("Error: matches_source.csv precisa de colunas: match_id,home,away[,date]."); raise SystemExit(2)
        return list(reader)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    ms = Path("data/in")/args.rodada/"matches_source.csv"
    rows = read_matches(ms)
    print(f"[dryrun] {len(rows)} jogos lidos.")
    for m in rows[:10]:
        print(f"  - {m['match_id']}: {m['home']} vs {m['away']}  ->  {canonical(m['home'])} vs {canonical(m['away'])}")

if __name__ == "__main__":
    main()
