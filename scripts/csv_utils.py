# scripts/csv_utils.py
from __future__ import annotations
import csv
from pathlib import Path

def count_csv_rows(path: str | Path) -> int:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return 0
    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            # subtrai o header
            return max(sum(1 for _ in reader) - 1, 0)
        except Exception:
            return 0
