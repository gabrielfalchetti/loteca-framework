from __future__ import annotations
import csv
from pathlib import Path

_ALIAS_CACHE: dict[str, str] | None = None

def load_aliases(path: str | Path = "data/refs/team_aliases.csv") -> dict[str, str]:
    global _ALIAS_CACHE
    if _ALIAS_CACHE is not None:
        return _ALIAS_CACHE
    p = Path(path)
    mapping: dict[str, str] = {}
    if not p.exists() or p.stat().st_size == 0:
        _ALIAS_CACHE = {}
        return _ALIAS_CACHE
    with p.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        # tenta detectar colunas
        alias_idx = 0
        canonical_idx = 1
        if header and "alias" in [h.strip().lower() for h in header]:
            alias_idx = [h.strip().lower() for h in header].index("alias")
            canonical_idx = [h.strip().lower() for h in header].index("canonical")
        else:
            # sem header válido; tratar como alias,canonical
            f.seek(0)
            reader = csv.reader(f)

        for row in reader:
            if not row or len(row) < 2:
                continue
            a = row[alias_idx].strip()
            c = row[canonical_idx].strip()
            if not a or not c or a.startswith("#"):
                continue
            mapping[a.lower()] = c
    _ALIAS_CACHE = mapping
    return mapping

def normalize_team(name: str, mapping: dict[str, str] | None = None) -> str:
    if not name:
        return name
    m = mapping or load_aliases()
    n = name.strip()
    key = n.lower()
    if key in m:
        return m[key]
    # tentativas leves (remove /UF e espaços duplos)
    key2 = key.replace("/sp", "").replace("/rj", "").replace("/mg", "").replace("/rs", "").replace("/sc", "").replace("/ba", "")
    key2 = " ".join(key2.split())
    if key2 in m:
        return m[key2]
    return n
