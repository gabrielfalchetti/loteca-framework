# scripts/csv_utils.py
from __future__ import annotations
import csv
from pathlib import Path
from typing import Iterable, List, Dict, Optional

def ensure_dir(p: Path | str) -> None:
    Path(p).parent.mkdir(parents=True, exist_ok=True)

def read_csv_rows(path: str | Path, *, encoding: str = "utf-8") -> List[Dict[str, str]]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", newline="", encoding=encoding) as f:
        reader = csv.DictReader(f)
        return list(reader)

def write_csv_rows(path: str | Path, rows: Iterable[Dict[str, object]], fieldnames: Optional[Iterable[str]] = None, *, encoding: str = "utf-8") -> int:
    p = Path(path)
    ensure_dir(p)
    rows = list(rows)
    if not rows and not fieldnames:
        # nada a escrever e sem header explícito
        ensure_dir(p)  # garante diretório
        # cria um CSV vazio com header inexistente? mantém vazio.
        p.touch(exist_ok=True)
        return 0
    if fieldnames is None:
        # pega header pela união das chaves (ordem estável por primeira linha)
        first = rows[0] if rows else {}
        fieldnames = list(first.keys())
    with p.open("w", newline="", encoding=encoding) as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames))
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)

def count_csv_rows(path: str | Path, *, encoding: str = "utf-8") -> int:
    p = Path(path)
    if not p.exists():
        return 0
    with p.open("r", newline="", encoding=encoding) as f:
        # conta linhas de dados (ignora header)
        it = csv.reader(f)
        try:
            next(it)  # header
        except StopIteration:
            return 0
        return sum(1 for _ in it)
