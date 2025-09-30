# scripts/csv_utils.py
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, List, Dict, Optional, Union, Any

PathLike = Union[str, Path]


def ensure_dir(p: PathLike) -> None:
    """
    Garante que o diretório pai do caminho exista.
    """
    Path(p).parent.mkdir(parents=True, exist_ok=True)


def read_csv_rows(path: PathLike, *, encoding: str = "utf-8") -> List[Dict[str, str]]:
    """
    Lê um CSV (com header) e retorna uma lista de dicionários (linhas).
    Se o arquivo não existir, retorna lista vazia.
    """
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", newline="", encoding=encoding) as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv_rows(
    path: PathLike,
    rows: Iterable[Dict[str, Any]],
    fieldnames: Optional[Iterable[str]] = None,
    *,
    encoding: str = "utf-8",
) -> int:
    """
    Escreve linhas (dicionários) em um CSV. Se 'fieldnames' não for passado,
    usa as chaves da primeira linha. Retorna a quantidade de linhas escritas.
    Cria diretórios automaticamente.
    """
    p = Path(path)
    ensure_dir(p)
    rows = list(rows)

    if not rows and not fieldnames:
        # Sem linhas e sem header explícito -> cria arquivo vazio (touch) e retorna 0
        p.touch(exist_ok=True)
        return 0

    if fieldnames is None:
        first = rows[0] if rows else {}
        fieldnames = list(first.keys())

    with p.open("w", newline="", encoding=encoding) as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames))
        w.writeheader()
        for r in rows:
            w.writerow(r)

    return len(rows)


def count_csv_rows(path: PathLike, *, encoding: str = "utf-8") -> int:
    """
    Conta as linhas de dados (ignora o header). Retorna 0 se não existir.
    """
    p = Path(path)
    if not p.exists():
        return 0
    with p.open("r", newline="", encoding=encoding) as f:
        it = csv.reader(f)
        try:
            next(it)  # header
        except StopIteration:
            return 0
        return sum(1 for _ in it)


def lower_all(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Retorna uma cópia do dicionário com TODOS os valores string em minúsculas/strip.
    Útil para normalização antes de merges ou comparações.
    """
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, str):
            out[k] = v.strip().lower()
        else:
            out[k] = v
    return out
