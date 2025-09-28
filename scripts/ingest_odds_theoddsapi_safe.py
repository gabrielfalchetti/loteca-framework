#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wrapper 'à prova de falhas' para o ingest do TheOddsAPI.
- Remove kwargs desconhecidos do wandb.init (ex.: finish_previous, start_method)
- Invoca o script original via runpy, repassando os mesmos argumentos
- Em erro, cria CSV vazio com header e um debug.json com detalhes
"""

import argparse
import csv
import json
import os
import runpy
import sys
from datetime import datetime, timezone

THEODDS_CSV_COLUMNS = [
    "home", "away", "book", "odd_home", "odd_draw", "odd_away",
    "event_id", "provider"
]

def ensure_dir_for(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

def write_empty_csv(csv_path: str) -> None:
    ensure_dir_for(csv_path)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(THEODDS_CSV_COLUMNS)

def write_debug(debug_path: str, payload: dict) -> None:
    ensure_dir_for(debug_path)
    with open(debug_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ---- args mínimos (para sabermos a RODADA) ----------------------------------
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--rodada", default=os.environ.get("RODADA", "").strip())
parser.add_argument("--regions", default="uk,eu,us,au")
parser.add_argument("--debug", action="store_true")
known, unknown = parser.parse_known_args()

rodada = known.rodada or ""
regions = known.regions
debug_flag = known.debug

out_dir = os.path.join("data", "out", rodada) if rodada else os.path.join("data", "out", "")
csv_path = os.path.join(out_dir, "odds_theoddsapi.csv")
dbg_path = os.path.join(out_dir, "theoddsapi_wrapper_debug.json")

# ---- monkey-patch no wandb.init ---------------------------------------------
try:
    import wandb  # noqa
    _orig_init = wandb.init
    def _safe_init(*args, **kwargs):
        kwargs.pop("finish_previous", None)
        kwargs.pop("start_method", None)
        return _orig_init(*args, **kwargs)
    wandb.init = _safe_init  # type: ignore
except Exception:
    pass  # sem wandb ou falha no import → segue sem tracking

# ---- prepara chamada do script original -------------------------------------
orig_script = os.path.join("scripts", "ingest_odds_theoddsapi.py")
forward_argv = [orig_script]
if rodada:
    forward_argv += ["--rodada", rodada]
if regions:
    forward_argv += ["--regions", regions]
if debug_flag:
    forward_argv += ["--debug"]
forward_argv += unknown

# ---- executa blindado -------------------------------------------------------
ensure_dir_for(csv_path)
_old_argv = sys.argv
try:
    sys.argv = [orig_script] + forward_argv[1:]
    runpy.run_path(orig_script, run_name="__main__")
except SystemExit as e:
    if getattr(e, "code", 0) not in (0, None):
        write_empty_csv(csv_path)
        if debug_flag:
            write_debug(dbg_path, {
                "when": datetime.now(timezone.utc).isoformat(),
                "rodada": rodada,
                "regions": regions,
                "error": "SystemExit",
                "exit_code": getattr(e, "code", None),
                "note": "CSV vazio gerado pelo wrapper devido a SystemExit != 0."
            })
        sys.exit(0)
    sys.exit(0)
except Exception as ex:
    write_empty_csv(csv_path)
    if debug_flag:
        write_debug(dbg_path, {
            "when": datetime.now(timezone.utc).isoformat(),
            "rodada": rodada,
            "regions": regions,
            "error": repr(ex),
            "note": "CSV vazio gerado pelo wrapper devido a exceção."
        })
    sys.exit(0)
finally:
    sys.argv = _old_argv
