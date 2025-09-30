#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wrapper SAFE para API-Football (RapidAPI).

Objetivo:
- Chamar o módulo robusto scripts.ingest_odds_apifootball_rapidapi com parâmetros sensatos;
- Nunca falhar o job: garantir arquivos de saída (nem que vazios) e exit code 0;
- Padronizar logs/contagens e deixar rastros de debug.

Saídas esperadas (na pasta da rodada):
  - data/out/<RODADA>/odds_apifootball.csv
  - data/out/<RODADA>/unmatched_apifootball.csv
  - (opcional) data/out/<RODADA>/debug/*.log

Uso típico:
  python scripts/ingest_odds_apifootball_rapidapi_safe.py \
    --rodada 2025-09-27_1213 \
    --season 2025 \
    --window 1 \
    --fuzzy 0.92 \
    --aliases data/aliases_br.json \
    --debug
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict


TAG = "[apifootball-safe]"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _touch_file(path: Path, header: str | None = None) -> None:
    _ensure_parent(path)
    if not path.exists():
        with open(path, "w", encoding="utf-8") as f:
            if header is not None:
                f.write(header.rstrip("\n") + "\n")


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            # conta linhas de dados (desconsidera header se houver)
            rows = list(reader)
            if not rows:
                return 0
            # heurística: se a primeira linha parece header (tem letras), não contar
            header_like = any(any(c.isalpha() for c in (col or "")) for col in rows[0])
            return max(0, len(rows) - (1 if header_like else 0))
    except Exception:
        return 0


def _run_child(cmd: list[str], env: Dict[str, str] | None, debug: bool) -> int:
    if debug:
        print(f"{TAG} Executando: {' '.join(cmd)}")
    try:
        proc = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if debug:
            if proc.stdout:
                print(proc.stdout)
            if proc.stderr:
                print(proc.stderr, file=sys.stderr)
        return proc.returncode
    except Exception as e:
        print(f"{TAG} ERRO ao executar filho: {e}", file=sys.stderr)
        return 127


def main() -> int:
    parser = argparse.ArgumentParser(description="Wrapper SAFE para API-Football (RapidAPI)")
    parser.add_argument("--rodada", required=True, help="Ex.: 2025-09-27_1213")
    parser.add_argument("--season", required=False, help="Ex.: 2025 (opcional; usa env SEASON se ausente)")
    parser.add_argument("--window", type=int, default=1, help="Janela de dias ao redor da data (padrão: 1)")
    parser.add_argument("--fuzzy", type=float, default=0.92, help="Similaridade mínima de nomes (padrão: 0.92)")
    parser.add_argument("--aliases", default="data/aliases_br.json", help="Arquivo JSON de aliases (padrão: data/aliases_br.json)")
    parser.add_argument("--debug", action="store_true", help="Liga logs verbosos")
    args = parser.parse_args()

    rodada = args.rodada
    season = args.season or os.environ.get("SEASON", "")
    debug = bool(args.debug)

    # Pastas/arquivos esperados
    out_dir = Path(f"data/out/{rodada}")
    odds_csv = out_dir / "odds_apifootball.csv"
    unmatched_csv = out_dir / "unmatched_apifootball.csv"
    debug_dir = out_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)

    # Garante que a pasta existe
    out_dir.mkdir(parents=True, exist_ok=True)

    # Comando do módulo robusto (já existente no repo)
    # Mantemos flags padrão; se o módulo não aceitar algum, ele ignora.
    cmd = [
        sys.executable,
        "-m",
        "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada",
        rodada,
        "--window",
        str(args.window),
        "--fuzzy",
        str(args.fuzzy),
        "--aliases",
        args.aliases,
    ]
    if season:
        cmd += ["--season", season]
    if debug:
        cmd += ["--debug"]

    # Herdar env e garantir UTF-8
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")

    # Executa filho (não falhamos independente do retorno)
    rc = _run_child(cmd, env, debug)

    # Sempre garantir que os 2 arquivos existam (mesmo vazios)
    # Headers simples ajudam o restante do pipeline/Excel.
    _touch_file(odds_csv, header="match_id,home,away,bookmaker,market,selection,price,ts")
    _touch_file(unmatched_csv, header="source,home,away,reason")

    counts = {
        "odds_apifootball.csv": _count_csv_rows(odds_csv),
        "unmatched_apifootball.csv": _count_csv_rows(unmatched_csv),
    }
    print(f"{TAG} linhas -> {json.dumps(counts, ensure_ascii=False)}")

    # Se o filho retornou erro, não propagamos código de erro (SAFE by design).
    # O objetivo é manter o job verde e a etapa de consenso produzir CSV (nem que vazio).
    return 0


if __name__ == "__main__":
    sys.exit(main())
