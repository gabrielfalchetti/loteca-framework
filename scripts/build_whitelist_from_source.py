#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera a whitelist exclusivamente a partir de data/in/matches_source.csv,
sem consultar nenhuma API externa.

Entrada: data/in/matches_source.csv  (colunas: match_id,home,away[,date])
Saída:
  - {OUT_DIR}/matches_whitelist.csv   (para os próximos steps)
  - data/in/matches_whitelist.csv     (cópia de conveniência, opcional)
"""

import argparse
import csv
import os
import sys

REQUIRED_COLS = ["match_id", "home", "away"]

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def read_source(path):
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        raise FileNotFoundError(f"Arquivo obrigatório ausente/vazio: {path}")
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = [h.strip() for h in reader.fieldnames or []]
        for c in REQUIRED_COLS:
            if c not in header:
                raise ValueError(f"Coluna obrigatória ausente em {path}: '{c}'")
        rows = []
        for row in reader:
            if not row.get("match_id") or not row.get("home") or not row.get("away"):
                # ignora linhas incompletas
                continue
            # normaliza espaços
            rows.append({
                "match_id": row["match_id"].strip(),
                "home": row["home"].strip(),
                "away": row["away"].strip(),
            })
    if not rows:
        raise ValueError(f"Nenhum jogo válido em {path}")
    return rows

def write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REQUIRED_COLS)
        writer.writeheader()
        writer.writerows(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Pasta de saída (OUT_DIR)")
    ap.add_argument("--source", default="data/in/matches_source.csv",
                    help="CSV de entrada com seus jogos (default: data/in/matches_source.csv)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    try:
        rows = read_source(args.source)
    except Exception as exc:
        eprint(f"[whitelist-source][ERRO] {exc}")
        sys.exit(3)

    out_whitelist = os.path.join(args.rodada, "matches_whitelist.csv")
    try:
        write_csv(out_whitelist, rows)
        # cópia opcional para data/in (útil para inspeção)
        write_csv("data/in/matches_whitelist.csv", rows)
    except Exception as exc:
        eprint(f"[whitelist-source][ERRO] Falha ao salvar whitelist: {exc}")
        sys.exit(3)

    if args.debug:
        print(f"[whitelist-source] gerados {len(rows)} jogos -> {out_whitelist}")
        for r in rows[:20]:
            print(f" - {r['match_id']}: {r['home']} x {r['away']}")

if __name__ == "__main__":
    main()