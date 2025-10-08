#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coleta injuries do API-Football com resolução de time robusta (evita U17/U20/U23 etc.).
"""

from __future__ import annotations
import argparse
import os
import re
import requests
import pandas as pd
from pathlib import Path

BAD_TEAM_PAT = re.compile(r"\b(u\d{2}|sub-\d{2}|u-?\d{2})\b", re.I)  # evita categorias de base
SPC = re.compile(r"\s+")
NORM = lambda s: SPC.sub(" ", re.sub(r"[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000]", " ", s or "")).strip().lower()

ALIASES = {
    "america-mg": ["america mg", "america mineiro", "américa mineiro"],
    "vila nova": ["vila-nova", "vilanova"],
    "atletico-mg": ["atletico mg", "atletico mineiro"],
    "sport": ["sport recife", "sport club do recife"],
    "operario-pr": ["operario pr","operario"],
    "novorizontino": ["grêmio novorizontino","gremio novorizontino"],
    "avai": ["avaí"],
}

def resolve_team_id(name: str, candidates: list[dict]) -> dict | None:
    n = NORM(name)
    # 1) match exato (sem base)
    exact = [c for c in candidates if NORM(c.get("name","")) == n and not BAD_TEAM_PAT.search(NORM(c.get("name","")))]
    if exact: return exact[0]
    # 2) aliases
    for k,alts in ALIASES.items():
        if n == k or n in [NORM(a) for a in alts]:
            # escolhe candidato que contém a palavra-chave do alias e não é base
            for c in candidates:
                cn = NORM(c.get("name",""))
                if k in cn and not BAD_TEAM_PAT.search(cn):
                    return c
    # 3) contains e sem base
    contains = [c for c in candidates if n in NORM(c.get("name","")) and not BAD_TEAM_PAT.search(NORM(c.get("name","")))]
    if contains: return contains[0]
    # 4) fallback: primeiro não-base
    nobase = [c for c in candidates if not BAD_TEAM_PAT.search(NORM(c.get("name","")))]
    if nobase: return nobase[0]
    # 5) último recurso: primeiro da lista
    return candidates[0] if candidates else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Este script é mock-safe: se não houver chave, gerar vazio
    key = os.environ.get("X_RAPIDAPI_KEY", "")
    if not key:
        (out_dir/"injuries.csv").write_text("", encoding="utf-8")
        print("[injuries] sem chave; gerando vazio.")
        return

    # >>> Aqui você colocaria suas chamadas reais à API. Para fins de compatibilidade,
    # vamos assumir que você já tem um dataset 'injuries_raw.json' se quiser “replay”.
    # Abaixo, um esqueleto de merge; adapte conforme seu formato atual.
    # Exemplo minimal: criar um CSV vazio com header consistente.
    cols = ["team_id","team_name","player","type","since","expected_return"]
    pd.DataFrame(columns=cols).to_csv(out_dir/"injuries.csv", index=False)
    print(f"[injuries] OK -> {out_dir/'injuries.csv'} (compat header)")

if __name__ == "__main__":
    main()
