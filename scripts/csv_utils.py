# scripts/csv_utils.py
from __future__ import annotations

import csv
import os
from typing import Any, Dict, Iterable, List, Optional
from Unidecode import unidecode

def ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

def count_csv_rows(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        r = csv.reader(f)
        try:
            header = next(r)
        except StopIteration:
            return 0
        return sum(1 for _ in r)

def read_csv_rows(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(dict(row))
    return rows

def write_csv_rows(path: str, rows: Iterable[Dict[str, Any]], fieldnames: Iterable[str]) -> int:
    ensure_dir(path)
    n = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames), extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)
            n += 1
    return n

def lower_all(x: Optional[str]) -> str:
    s = (x or "").strip()
    return unidecode(s).lower()

def safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return default

def safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return default
