#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import csv
import os
from typing import Dict, Iterable, List, Sequence

def read_csv_rows(path: str) -> List[Dict[str, str]]:
    """Lê CSV para lista de dicts. Retorna [] se não existir ou em erro."""
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            rows = [{(k or "").strip(): (v or "").strip() for k, v in row.items()} for row in rdr]
        return rows
    except Exception:
        return []

def write_csv_rows(path: str, rows: Sequence[Dict[str, str]]) -> int:
    """Escreve CSV garantindo cabeçalho por união de chaves. Retorna nº de linhas."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        # escreve cabeçalho mínimo
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["source"])
        return 0

    # União ordenada das chaves
    keys: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                keys.append(k)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)

def count_csv_rows(path: str) -> int:
    """Conta linhas (desconta header)."""
    rows = read_csv_rows(path)
    return len(rows)

def lower_keys(row: Dict[str, str]) -> Dict[str, str]:
    return { (k or "").strip().lower(): v for k, v in row.items() }

def lower_all(rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    return [lower_keys(r) for r in rows]
