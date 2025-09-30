#!/usr/bin/env python
from __future__ import annotations

# HOTFIX de import
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse, csv
from typing import Dict, List
from collections import defaultdict

def read_csv(path: Path) -> List[Dict[str,str]]:
    if not path.exists():
        print(f"[consensus] AVISO: arquivo não encontrado: {path}")
        return []
    with path.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"[consensus] lido {path} -> {len(rows)} linhas")
    return rows

def mean(xs: List[float]) -> float:
    return sum(xs)/len(xs) if xs else float("nan")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--strict-empty", action="store_true",
                    help="Se nenhum provedor tiver odds, aborta com código 1 (comportamento antigo).")
    args = ap.parse_args()

    base_out = Path("data/out") / args.rodada
    base_out.mkdir(parents=True, exist_ok=True)

    the = read_csv(base_out / "odds_theoddsapi.csv")
    api = read_csv(base_out / "odds_apifootball.csv")
    rows = the + api
    out_csv = base_out / "odds_consensus.csv"

    if not rows:
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=[
                "match_id","home","away","market","selection","price_consensus","num_feeds"
            ])
            wr.writeheader()
        print("[consensus] AVISO: nenhum provedor retornou odds. CSV vazio gerado.")
        if args.strict_empty:
            raise SystemExit(1)
        raise SystemExit(0)

    grouped: Dict[tuple, List[float]] = defaultdict(list)
    meta: Dict[tuple, Dict[str,str]] = {}
    for r in rows:
        key = (r["match_id"], r["market"], r["selection"])
        try:
            px = float(r["price"])
        except:
            continue
        grouped[key].append(px)
        if key not in meta:
            meta[key] = {"match_id": r["match_id"], "home": r["home"], "away": r["away"],
                         "market": r["market"], "selection": r["selection"]}

    consensus_rows: List[Dict[str,str]] = []
    for key, prices in grouped.items():
        m = meta[key]
        consensus_rows.append({
            "match_id": m["match_id"], "home": m["home"], "away": m["away"],
            "market": m["market"], "selection": m["selection"],
            "price_consensus": f"{mean(prices):.6f}",
            "num_feeds": str(len(prices))
        })

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=[
            "match_id","home","away","market","selection","price_consensus","num_feeds"
        ])
        wr.writeheader(); wr.writerows(consensus_rows)

    print(f"[consensus] OK -> {out_csv} ({len(consensus_rows)} linhas)")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
