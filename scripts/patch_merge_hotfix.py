#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
patch_merge_hotfix.py
Corrige, in-place, o merge inseguro em scripts/ingest_odds.py que usa:
    matches.merge(out, left_on=["home_n","away_n"], right_on=["home","away"], how="left")
Substitui por:
    matches.merge(out, on=["home_n","away_n"], how="left")

Também corrige variantes comuns (espaços, aspas simples, quebra de linha, ordem dos argumentos).
É idempotente: se já estiver correto, não altera nada.
"""

import re
from pathlib import Path
import sys

TARGET = Path("scripts/ingest_odds.py")

if not TARGET.exists():
    print(f"[hotfix] Arquivo não encontrado: {TARGET}", file=sys.stderr)
    sys.exit(2)

src = TARGET.read_text(encoding="utf-8")

patterns = [
    # Variante padrão
    r'matches\.merge\(\s*out\s*,\s*left_on\s*=\s*\[\s*["\']home_n["\']\s*,\s*["\']away_n["\']\s*\]\s*,\s*right_on\s*=\s*\[\s*["\']home["\']\s*,\s*["\']away["\']\s*\]\s*,\s*how\s*=\s*["\']left["\']\s*\)',
    # Permutação dos argumentos (qualquer ordem), preservando out como 2º arg posicional
    r'matches\.merge\(\s*out\s*,(?:(?!\)).)*left_on\s*=\s*\[\s*["\']home_n["\']\s*,\s*["\']away_n["\']\s*\](?:(?!\)).)*right_on\s*=\s*\[\s*["\']home["\']\s*,\s*["\']away["\']\s*\](?:(?!\)).)*how\s*=\s*["\']left["\'](?:(?!\)).)*\)',
]

repl = r'matches.merge(out, on=["home_n","away_n"], how="left")'

count_total = 0
new_src = src
for pat in patterns:
    new_src, n = re.subn(pat, repl, new_src, flags=re.DOTALL)
    count_total += n

if count_total == 0:
    # Já pode estar corrigido ou com uma variação diferente; tenta detectar merge correto
    if re.search(r'matches\.merge\(\s*cons?\s*,\s*on\s*=\s*\[\s*["\']home_n["\']\s*,\s*["\']away_n["\']\s*\]\s*,\s*how\s*=\s*["\']left["\']\s*\)', src):
        print("[hotfix] Merge já está no formato seguro — nenhuma alteração.")
        sys.exit(0)
    # Última tentativa: troca somente o trecho 'left_on=[...], right_on=[...]' por 'on=[...]'
    new_src2, n2 = re.subn(
        r'left_on\s*=\s*\[\s*["\']home_n["\']\s*,\s*["\']away_n["\']\s*\]\s*,\s*right_on\s*=\s*\[\s*["\']home["\']\s*,\s*["\']away["\']\s*\]',
        r'on=["home_n","away_n"]',
        src,
        flags=re.DOTALL
    )
    if n2 > 0:
        new_src = new_src2
        count_total = n2

if count_total > 0:
    TARGET.write_text(new_src, encoding="utf-8")
    print(f"[hotfix] Merge inseguro corrigido ({count_total} ocorrência(s)).")
else:
    print("[hotfix] Nenhum padrão de merge inseguro encontrado — verifique manualmente.")
