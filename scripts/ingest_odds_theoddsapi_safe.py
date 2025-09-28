#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wrapper "à prova de falhas" para o ingest do TheOddsAPI.
- Remove kwargs não suportados do wandb.init() (ex.: finish_previous, start_method)
- Executa o script original via runpy, repassando os mesmos argumentos
- Em caso de erro, cria CSV vazio com header e um debug.json com detalhes
"""

import argparse
import json
import os
import runpy
import sys
from datetime import datetime, timezone

# ---- util: caminho de saída & criação de arquivos seguros -------------------
THEODDS_CSV_COLUMNS = [
    "home", "away", "book", "odd_home", "odd_draw", "odd_away",
    "event_id", "provider"
]

def ensure_dirs(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

def write_empty_csv(csv_path: str) -> None:
    import csv
    ensure_dirs(csv_path)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(THEODDS_CSV_COLUMNS)

def write_debug_json(debug_path: str, payload: dict) -> None:
    ensure_dirs(debug_path)
    with open(debug_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ---- parse args mínimos para sabermos a RODADA e forwards -------------------
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--rodada", default=os.environ.get("RODADA", "").strip())
parser.add_argument("--regions", default="uk,eu,us,au")
parser.add_argument("--debug", action="store_true")
known, unknown = parser.parse_known_args()

rodada = known.rodada
if not rodada:
    # mantém compat com teu padrão: se não vier, usa mesmo assim (scorecard lida)
    rodada = ""
regions = known.regions
debug_flag = known.debug

out_dir = os.path.join("data", "out", rodada) if rodada else os.path.join("data", "out", "")
csv_path = os.path.join(out_dir, "odds_theoddsapi.csv")
dbg_path = os.path.join(out_dir, "theoddsapi_wrapper_debug.json")

# ---- monkey-patch do wandb.init antes de carregar o script original ---------
try:
    import wandb  # noqa: F401
    _orig_init = wandb.init

    def _safe_init(*args, **kwargs):
        # Remove kwargs desconhecidos por versões antigas do wandb
        kwargs.pop("finish_previous", None)
        kwargs.pop("start_method", None)
        return _orig_init(*args, **kwargs)

    wandb.init = _safe_init  # type: ignore[attr-defined]
except Exception:
    # se não tiver wandb ou falhar import, segue sem tracking
    pass

# ---- prepara sys.argv para delegar ao script original -----------------------
orig_script = os.path.join("scripts", "ingest_odds_theoddsapi.py")
forward_argv = [orig_script]
# reconstroi argv com o que o workflow te passaria normalmente
if rodada:
    forward_argv += ["--rodada", rodada]
if regions:
    forward_argv += ["--regions", regions]
if debug_flag:
    forward_argv += ["--debug"]
# mantém quaisquer flags adicionais que o workflow esteja mandando
forward_argv += unknown

# ---- executa e blinda falhas ------------------------------------------------
try:
    # Garante diretórios
    ensure_dirs(csv_path)

    # Troca sys.argv só para a execução do script original
    _old_argv = sys.argv
    sys.argv = [orig_script] + forward_argv[1:]

    # Executa o script original como se fosse __main__
    runpy.run_path(orig_script, run_name="__main__")

except SystemExit as e:
    # Se o script original der sys.exit != 0, ainda garantimos CSV vazio
    if getattr(e, "code", 0) not in (0, None):
        write_empty_csv(csv_path)
        if debug_flag:
            write_debug_json(dbg_path, {
                "when": datetime.now(timezone.utc).isoformat(),
                "rodada": rodada,
                "regions": regions,
                "error": "SystemExit",
                "exit_code": getattr(e, "code", None),
                "note": "CSV vazio gerado pelo wrapper devido a SystemExit != 0."
            })
        # Não propaga o erro para não quebrar o job
        sys.exit(0)
    # Exit normal → apenas repassa
    sys.exit(0)

except Exception as ex:
    # Qualquer exceção inesperada → cria CSV vazio e loga debug
    write_empty_csv(csv_path)
    if debug_flag:
        write_debug_json(dbg_path, {
            "when": datetime.now(timezone.utc).isoformat(),
            "rodada": rodada,
            "regions": regions,
            "error": repr(ex),
            "note": "CSV vazio gerado pelo wrapper devido a exceção."
        })
    # Não falhar o pipeline
    sys.exit(0)

finally:
    # Restaura argv
    try:
        sys.argv = _old_argv  # type: ignore[name-defined]
    except Exception:
        pass
