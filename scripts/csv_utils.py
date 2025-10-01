# scripts/csv_utils.py
from __future__ import annotations

import csv
import os
from typing import Iterable, Dict, List, Any


def ensure_dir(path: str) -> None:
    """Garante que a pasta do arquivo exista."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


def write_csv_rows(path: str, rows: Iterable[Dict[str, Any]], fieldnames: List[str]) -> int:
    """Escreve um CSV com cabeçalho; retorna número de linhas escritas (sem contar cabeçalho)."""
    ensure_dir(path)
    count = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
            count += 1
    return count


def read_csv_rows(path: str) -> List[Dict[str, str]]:
    """Lê um CSV de cabeçalho; retorna lista de dicts. Se não existir, retorna lista vazia."""
    if not os.path.exists(path):
        return []
    out: List[Dict[str, str]] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append(dict(row))
    return out


def count_csv_rows(path: str) -> int:
    """Conta linhas (sem cabeçalho). Se não existir, retorna 0."""
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        # conta linhas - 1 (header)
        n = sum(1 for _ in f)
    return max(0, n - 1)


def lower_all(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte chaves e valores str para minúsculas (útil em normalização).
    Objetos não-str são mantidos.
    """
    out: Dict[str, Any] = {}
    for k, v in d.items():
        kk = k.lower() if isinstance(k, str) else k
        if isinstance(v, str):
            out[kk] = v.lower()
        else:
            out[kk] = v
    return out
