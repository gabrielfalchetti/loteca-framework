#!/usr/bin/env python
# scripts/dryrun_matchmap.py
# Verifica casamento de nomes entre sua grade (matches_source.csv) e provedores
# sem bater nas APIs de odds (apenas para diagnóstico rápido).

from __future__ import annotations
import argparse, csv, sys
from pathlib import Path
from typing import List, Dict, Tuple
from utils.match_normalize import canonical, fuzzy_match

def read_matches(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print(f"[ERRO] Arquivo não encontrado: {path}", file=sys.stderr)
        sys.exit(2)
    rows = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        needed = {"match_id", "home", "away"}
        if not needed.issubset(reader.fieldnames or []):
            print("Error: matches_source.csv precisa de colunas: match_id,home,away[,date].", file=sys.stderr)
            sys.exit(2)
        for r in reader:
            rows.append(r)
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--provider", choices=["theoddsapi","apifootball"], required=False, default=None,
                    help="Opcional: apenas documenta como seria o casamento; não chama a API.")
    args = ap.parse_args()

    base_in = Path("data/in") / args.rodada
    ms_path = base_in / "matches_source.csv"
    matches = read_matches(ms_path)

    print(f"[dryrun] {len(matches)} jogos lidos.")
    print("[dryrun] Amostras normalizadas:")
    for m in matches[:10]:
        print(f"  - {m['match_id']}: {m['home']} vs {m['away']}  ->  {canonical(m['home'])} vs {canonical(m['away'])}")

    print("\n[dryrun] Dica: rode os ingests e confira os CSVs 'unmatched_*' em data/out/<rodada>/.")

if __name__ == "__main__":
    main()
