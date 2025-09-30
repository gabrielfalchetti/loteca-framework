#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consenso SAFE:
- Lê TheOddsAPI e API-Football (RapidAPI) se existirem.
- NUNCA quebra: se só uma fonte tem dados, fazemos passthrough.
- Se houver chave comum óbvia, conciliamos; senão, apenas unimos.
- Gera sempre data/out/<rodada>/odds_consensus.csv

Boas práticas aplicadas:
- Tipagem, logs consistentes, IO resiliente, funções pequenas e testáveis.
"""

from __future__ import annotations
import argparse
from typing import Dict, List, Tuple

from scripts.logging_setup import get_logger
from scripts.csv_utils import (
    read_csv_rows, write_csv_rows, count_csv_rows, lower_all
)

LOGGER = get_logger("consensus-safe")

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gera consenso de odds de forma segura.")
    p.add_argument("--rodada", required=True, help="Carimbo da rodada (ex.: 2025-09-27_1213)")
    return p.parse_args()

def paths(rodada: str) -> Tuple[str, str, str]:
    base = f"data/out/{rodada}"
    theodds = f"{base}/odds_theoddsapi.csv"
    apifoot = f"{base}/odds_apifootball.csv"
    out = f"{base}/odds_consensus.csv"
    return theodds, apifoot, out

# Heurística simples de chave
def extract_key(row: Dict[str, str]) -> str:
    """
    Tenta montar uma chave de match reutilizando colunas comuns.
    Preferência: match_id/fixture_id; fallback: home|away|date.
    """
    r = {k.lower(): (v or "").strip() for k, v in row.items()}
    for k in ("match_id", "fixture_id", "id", "game_id"):
        if r.get(k):
            return f"id:{r[k]}"

    home = r.get("home") or r.get("home_team") or r.get("team_home") or ""
    away = r.get("away") or r.get("away_team") or r.get("team_away") or ""
    date = r.get("kickoff") or r.get("commence_time") or r.get("date") or r.get("start_time") or ""
    norm = "|".join(s.strip().lower() for s in (home, away, date))
    if norm.strip("|"):
        return f"triple:{norm}"
    # Sem nada para chaves → linha única
    return f"rowhash:{hash(tuple(sorted(r.items())))}"

def merge_consensus(rows_a: List[Dict[str, str]], rows_b: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Estratégia:
      - Normaliza chaves para minúsculas (compatibilidade).
      - Indexa A por chave; para B, se chave já existe, preferimos A e
        acrescentamos colunas de B que não existem.
      - Se não existir, acrescentamos a linha de B.
    Resultado: união deduplicada e enriquecida.
    """
    A = lower_all(rows_a)
    B = lower_all(rows_b)

    idx: Dict[str, Dict[str, str]] = {}
    for r in A:
        key = extract_key(r)
        r2 = dict(r)
        r2.setdefault("source", "theoddsapi")
        idx[key] = r2

    for r in B:
        key = extract_key(r)
        if key in idx:
            # mescla colunas que A não tem
            for k, v in r.items():
                if k not in idx[key] or not idx[key][k]:
                    idx[key][k] = v
            # marca presença de ambos
            if "source" in idx[key]:
                if "apifootball" not in idx[key]["source"]:
                    idx[key]["source"] = (idx[key]["source"] + "+apifootball").strip("+")
            else:
                idx[key]["source"] = "apifootball"
        else:
            r2 = dict(r)
            r2.setdefault("source", "apifootball")
            idx[key] = r2

    return list(idx.values())

def main() -> int:
    ns = parse_args()
    theodds_path, apifoot_path, out_path = paths(ns.rodada)

    rows_theodds = read_csv_rows(theodds_path)
    rows_apifoot = read_csv_rows(apifoot_path)

    if not rows_theodds:
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {theodds_path}")
    if not rows_apifoot:
        # não é erro — apenas loga
        pass

    print(f"[consensus-safe] lido {theodds_path} -> {len(rows_theodds)} linhas")
    print(f"[consensus-safe] lido {apifoot_path} -> {len(rows_apifoot)} linhas")

    if not rows_theodds and not rows_apifoot:
        print("[consensus-safe] AVISO: nenhum provedor retornou odds. CSV vazio gerado.")
        written = write_csv_rows(out_path, [])
        print(f"[consensus-safe] OK -> {out_path} ({written} linhas)")
        return 0

    merged = merge_consensus(rows_theodds, rows_apifoot)
    written = write_csv_rows(out_path, merged)
    print(f"[consensus-safe] OK -> {out_path} ({written} linhas)")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
