# -*- coding: utf-8 -*-
"""
csv_utils.py
Utilitários simples e estáveis, usados por scripts de ingest e safe.
"""

from __future__ import annotations

import csv
import os
from typing import Iterable, List, Dict, Any

def ensure_dir(path: str) -> None:
    """Garante que o diretório de 'path' exista."""
    d = path if os.path.isdir(path) else os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def write_csv_rows(path: str, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> int:
    """Escreve linhas em CSV, retornando a contagem de linhas escritas (excluindo cabeçalho)."""
    ensure_dir(path)
    n = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
            n += 1
    return n

def count_csv_rows(path: str) -> int:
    """Conta linhas de dados (exclui cabeçalho) se o arquivo existir."""
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            next(reader)  # cabeçalho
        except StopIteration:
            return 0
        return sum(1 for _ in reader)
