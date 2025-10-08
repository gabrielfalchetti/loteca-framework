# -*- coding: utf-8 -*-
"""
Gera uma 'whitelist' de partidas a partir de data/in/matches_source.csv.
Saída: data/in/matches_whitelist.csv (com match_key canônica).

Execute ANTES das ingestões:
  python -m scripts.match_whitelist
"""

import csv
import sys
from pathlib import Path

from scripts._common_norm import match_key_from_teams

IN_PATH  = Path("data/in/matches_source.csv")
OUT_PATH = Path("data/in/matches_whitelist.csv")

REQUIRED = ["match_id", "home", "away", "source", "lat", "lon"]

def fail(msg: str, code: int = 91):
    print(f"::error::{msg}")
    sys.exit(code)

def main():
    if not IN_PATH.exists():
        fail(f"Entrada {IN_PATH} não encontrada. Esperado cabeçalho: {','.join(REQUIRED)}", 91)

    with IN_PATH.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        header = [h.strip() for h in (r.fieldnames or [])]
        miss = [c for c in REQUIRED if c not in header]
        if miss:
            fail(f"Cabeçalhos ausentes em {IN_PATH.name}: {miss}", 92)

        rows = []
        for row in r:
            home = (row.get("home") or "").strip()
            away = (row.get("away") or "").strip()
            mid  = (row.get("match_id") or "").strip()
            lat  = (row.get("lat") or "").strip()
            lon  = (row.get("lon") or "").strip()
            if not (home and away and mid):
                continue
            key = match_key_from_teams(home, away)
            rows.append({
                "match_id": mid,
                "home": home,
                "away": away,
                "lat": lat,
                "lon": lon,
                "match_key": key,
            })

    if not rows:
        fail(f"Nenhum jogo válido em {IN_PATH.name}", 93)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["match_id", "home", "away", "lat", "lon", "match_key"])
        w.writeheader()
        w.writerows(rows)

    print(f"[whitelist] OK -> {OUT_PATH} ({len(rows)} jogos)")

if __name__ == "__main__":
    main()