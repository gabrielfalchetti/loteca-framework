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

FIELDS = ["match_id","home","away","bookmaker","market","selection","price"]

def read_csv(path: Path) -> List[Dict[str,str]]:
    if not path.exists(): return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def mean(xs: List[float]) -> float:
    return sum(xs)/len(xs) if xs else float("nan")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--allow-empty", action="store_true",
                    help="Se nenhum provedor tiver odds, não aborta; grava arquivo vazio e sai com código 0.")
    args = ap.parse_args()

    base_out = Path("data/out") / args.rodada
    base_out.mkdir(parents=True, exist_ok=True)

    the = read_csv(base_out / "odds_theoddsapi.csv")
    api = read_csv(base_out / "odds_apifootball.csv")

    rows = the + api
    out_csv = base_out / "odds_consensus.csv"

    if not rows:
        # Gera arquivo vazio (com cabeçalho) para manter pipeline vivo se assim desejado
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=["match_id","home","away","market","selection","price_consensus","num_feeds"])
            wr.writeheader()
        msg = "[consensus] AVISO: nenhum provedor retornou odds. Arquivo vazio gerado."
        if args.allow-empty:
            print(msg)
            raise SystemExit(0)
        else:
            print(msg + " (use --allow-empty para não abortar)")
            raise SystemExit(1)

    # Consenso simples: média por (match_id, market, selection)
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
            "market": m["market"], "selection": m["selection"], "price_consensus": f"{mean(prices):.6f}",
            "num_feeds": str(len(prices))
        })

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","market","selection","price_consensus","num_feeds"])
        wr.writeheader(); wr.writerows(consensus_rows)

    print(f"[consensus] OK -> {out_csv} ({len(consensus_rows)} linhas)")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
