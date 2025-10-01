# scripts/consensus_odds_safe.py
"""
Gera odds_consensus.csv de forma SAFE e de baixo uso de memória.

Regra SAFE:
- Se existir data/out/<rodada>/odds_theoddsapi.csv e/ou odds_apifootball.csv,
  o script faz um "merge por concatenação" (append) preservando o header do
  primeiro arquivo encontrado e adicionando uma coluna "source" no final.
- Se nenhum provedor existir, cria odds_consensus.csv com header "source" e 0 linhas.
- Sempre imprime mensagens padronizadas e sai com código 0.

Objetivo: robustez no CI. A lógica de consenso sofisticada pode rodar no script não-SAFE.
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable, Optional, Tuple

def _read_header(path: Path) -> Optional[list[str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            return next(r, None)
    except Exception:
        return None

def _iter_rows(path: Path) -> Iterable[list[str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            header_skipped = False
            for row in r:
                if not header_skipped:
                    header_skipped = True
                    continue
                yield row
    except Exception:
        # SAFE: silenciosamente ignora problemas de leitura
        return

def _safe_concat(
    out_path: Path,
    providers: Iterable[Tuple[str, Path]],
) -> int:
    written = 0
    chosen_header: Optional[list[str]] = None

    # escolhe header do primeiro arquivo válido
    for _, p in providers:
        if p.exists() and p.is_file():
            h = _read_header(p)
            if h:
                chosen_header = h[:]
                break

    # fallback de header mínimo
    if chosen_header is None:
        chosen_header = ["source"]

    # garante coluna "source" no final
    if "source" not in chosen_header:
        chosen_header = chosen_header + ["source"]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f_out:
        w = csv.writer(f_out)
        w.writerow(chosen_header)

        for src, p in providers:
            if not p.exists() or not p.is_file():
                print(f"[consensus-safe] AVISO: arquivo não encontrado: {p}")
                continue

            # mapeia colunas: se provider tiver menos colunas, completa com vazios
            header = _read_header(p) or []
            idx_source = len(chosen_header) - 1

            col_map = list(range(len(chosen_header)))
            # constrói um mapa de nomes -> índice para o arquivo atual
            name_to_idx = {name: i for i, name in enumerate(header)}
            # para cada coluna (menos a "source"), pega índice correspondente no provider
            for j, col in enumerate(chosen_header):
                if col == "source":
                    col_map[j] = -1
                else:
                    col_map[j] = name_to_idx.get(col, -2)  # -2 => coluna inexistente

            for row in _iter_rows(p):
                out_row = [""] * len(chosen_header)
                for j, src_idx in enumerate(col_map):
                    if src_idx == -1:
                        out_row[j] = src
                    elif src_idx >= 0 and src_idx < len(row):
                        out_row[j] = row[src_idx]
                    else:
                        out_row[j] = ""
                w.writerow(out_row)
                written += 1

    return written

def _read_and_count(path: Path) -> int:
    n = 0
    for _ in _iter_rows(path):
        n += 1
    print(f"[consensus-safe] lido {path} -> {n} linhas")
    return n

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    base = Path("data/out") / args.rodada
    theodds = base / "odds_theoddsapi.csv"
    apifoot = base / "odds_apifootball.csv"
    out_path = base / "odds_consensus.csv"

    # logs de leitura (se existirem)
    if theodds.exists():
        _read_and_count(theodds)
    else:
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {theodds}")

    if apifoot.exists():
        _read_and_count(apifoot)
    else:
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {apifoot}")

    written = _safe_concat(
        out_path,
        providers=[
            ("theoddsapi", theodds),
            ("apifootball", apifoot),
        ],
    )

    if written == 0:
        print("[consensus-safe] AVISO: nenhum provedor retornou odds. CSV vazio gerado.")
    print(f"[consensus-safe] OK -> {out_path} ({written} linhas)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
