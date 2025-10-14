#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
normalize_matches.py

Normaliza os nomes de times (acentos, hífens, variantes PT/EN) e aplica
aliases/canonização para clubes do Brasil e internacionais comuns.
Entrada: CSV com colunas: match_id, home, away
Saída:   CSV com colunas: match_id, home, away, home_orig, away_orig

Uso:
  python -m scripts.normalize_matches \
    --in_csv data/in/matches_source.csv \
    --out_csv data/out/<RUN_ID>/matches_norm.csv
"""

import argparse
import csv
import re
from typing import Dict

import pandas as pd
from unidecode import unidecode


# --------------------------
# utilidades de normalização
# --------------------------

SPACE_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^\w\s\-\/\(\)&]")  # permite -, /, (), &


def clean_basic(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    # normaliza aspas/traços “estranhos”
    s = s.replace("–", "-").replace("—", "-").replace("-", "-")
    s = s.replace("’", "'").replace("`", "'")
    # remove pontuações “ruins” mas preserva -, /, (), &
    s = PUNCT_RE.sub(" ", s)
    # normaliza espaços
    s = SPACE_RE.sub(" ", s).strip()
    return s


def to_key(s: str) -> str:
    """chave ASCII minúscula para buscas no dicionário de aliases."""
    s = clean_basic(s)
    s = unidecode(s).lower()
    # uniformiza separadores
    s = s.replace("/", " ").replace("\\", " ").replace(".", " ")
    s = SPACE_RE.sub(" ", s).strip()
    # remove sufixos genéricos que atrapalham
    # (mantemos siglas importantes como SP/PA quando fizer sentido)
    tokens = s.split()
    drop = {"fc", "ec", "ac", "sc", "afc", "cf", "club", "clube"}
    tokens = [t for t in tokens if t not in drop]
    s = " ".join(tokens)
    return s


def title_smart(s: str) -> str:
    """
    title-case “amigável” para nomes (mantém siglas em maiúsculas simples).
    """
    if not s:
        return s
    # Siglas conhecidas que devem permanecer maiúsculas
    uppers = {"SP", "RJ", "RS", "SC", "PR", "PA", "MG", "GO", "MT", "DF",
              "AC", "BA", "CE", "ES", "MA", "PB", "PE", "PI", "RN", "RO", "RR",
              "AP", "AL", "SE", "TO", "U23", "U20", "U19"}
    out_words = []
    for w in s.split():
        w_clean = re.sub(r"[^A-Za-z0-9]", "", w)
        if w_clean.upper() in uppers:
            out_words.append(w_clean.upper())
        else:
            out_words.append(w.capitalize())
    return " ".join(out_words)


# ---------------------------------
# dicionário de aliases/canonização
# ---------------------------------
# Mapeia várias formas -> nome canônico.
ALIASES: Dict[str, str] = {}

def add_alias(canonical: str, *variants: str):
    key_canon = to_key(canonical)
    # garante que o próprio canônico também leve a si mesmo
    ALIASES[key_canon] = canonical
    for v in variants:
        ALIASES[to_key(v)] = canonical


# ====== Times BR muito recorrentes / sensíveis ======
# Athletico-PR
add_alias(
    "Athletico Paranaense",
    "Athletico-PR", "Atletico-PR", "Atlético-PR", "Athletico PR",
    "Atletico Paranaense", "Atlético Paranaense", "Clube Athletico Paranaense"
)

# Atletico Goianiense
add_alias(
    "Atletico Goianiense",
    "Atlético-GO", "Atletico-GO", "Atletico GO", "Atletico Goiania",
    "Atlético Goianiense", "Atletico Goianiense EC"
)

# Botafogo SP
add_alias(
    "Botafogo SP",
    "Botafogo-SP", "Botafogo de Ribeirao Preto", "Botafogo Ribeirao Preto",
    "Botafogo (SP)", "Botafogo Sao Paulo"
)
# (Obs.: Botafogo RJ intencionalmente NÃO colocado aqui para evitar colisão.)

# Ferroviaria
add_alias(
    "Ferroviaria",
    "Ferroviária", "Ferroviaria SP", "Ferroviaria (SP)", "AA Ferroviaria"
)

# Remo (PA)
add_alias(
    "Remo (PA)",
    "Remo", "Clube do Remo", "Remo PA", "Remo-PA"
)

# Paysandu (PA)
add_alias(
    "Paysandu (PA)",
    "Paysandu", "Paysandu PA", "Paysandu-PA", "Paysandu Sport Club"
)

# Avai
add_alias(
    "Avai",
    "Avaí", "Avai FC"
)

# Chapecoense
add_alias(
    "Chapecoense",
    "Associacao Chapecoense de Futebol", "Chapecoense-SC", "Chapecoense SC"
)

# CRB
add_alias(
    "CRB",
    "Clube de Regatas Brasil", "CRB-AL", "CRB AL"
)

# Volta Redonda
add_alias(
    "Volta Redonda",
    "Volta Redonda FC", "Volta Redonda RJ"
)

# Remo/Paysandu com barra (caso alguém escreva "Paysandu/Remo (PA)")
add_alias("Remo (PA)", "Remo (PA)")
add_alias("Paysandu (PA)", "Paysandu (PA)")

# ====== Alguns internacionais/gerais úteis ======
add_alias("France", "Franca", "França")
add_alias("Germany", "Alemanha")
add_alias("Switzerland", "Suica", "Suiça", "Suiça", "Switzeland")
add_alias("Iceland", "Islandia", "Islândia")
add_alias("Wales", "Pais de Gales", "País de Gales")
add_alias("Belgium", "Belgica", "Bélgica")
add_alias("Sweden", "Suecia", "Suécia")
add_alias("North Macedonia", "Macedonia do Norte", "Macedônia do Norte")
add_alias("Kazakhstan", "Cazaquistao", "Cazaquistão")
add_alias("Ukraine", "Ucrania", "Ucrânia")
add_alias("Azerbaijan", "Azerbaijao", "Azerbaijão")
add_alias("Luxembourg", "Luxemburgo")
add_alias("Slovenia", "Eslovenia", "Eslovênia")
add_alias("Slovakia", "Eslovaquia", "Eslováquia")
add_alias("Switzerland", "Suica", "Suíça", "Suiça")
add_alias("Northern Ireland", "Irlanda do Norte")
add_alias("Kosovo", "Kosovo")


def canonicalize(name: str) -> str:
    """
    Retorna nome canônico se encontrar no dicionário;
    caso contrário, devolve uma forma “limpa” e legível.
    """
    key = to_key(name)
    if key in ALIASES:
        return ALIASES[key]
    # sem alias explícito: devolve uma versão “bonita” do nome limpo
    cleaned = clean_basic(name)
    # tenta manter UF entre parênteses, p.ex. "Remo (PA)"
    return title_smart(cleaned)


def normalize_row(home: str, away: str) -> (str, str):
    home_c = canonicalize(home)
    away_c = canonicalize(away)
    return home_c, away_c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True, help="CSV de entrada com match_id,home,away")
    ap.add_argument("--out_csv", required=True, help="CSV de saída normalizado")
    args = ap.parse_args()

    df = pd.read_csv(args.in_csv, dtype=str, keep_default_na=False).rename(
        columns={c: c.strip() for c in pd.read_csv(args.in_csv, nrows=0).columns}
    )

    # validações mínimas
    for c in ("match_id", "home", "away"):
        if c not in df.columns:
            raise SystemExit(f"[normalize] ERRO: coluna obrigatória ausente: {c}")

    df["home_orig"] = df["home"].astype(str)
    df["away_orig"] = df["away"].astype(str)

    homes, aways = [], []
    for h, a in zip(df["home_orig"], df["away_orig"]):
        h_c, a_c = normalize_row(h, a)
        homes.append(h_c)
        aways.append(a_c)

    out = pd.DataFrame(
        {
            "match_id": df["match_id"],
            "home": homes,
            "away": aways,
            "home_orig": df["home_orig"],
            "away_orig": df["away_orig"],
        }
    )

    # remove duplicatas exactas por (match_id, home, away) se houver
    out = out.drop_duplicates(subset=["match_id", "home", "away"]).reset_index(drop=True)

    # salva com CSV consistente (LF e sem índice)
    out.to_csv(args.out_csv, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[normalize] OK — gravado {len(out)} linhas em {args.out_csv}")


if __name__ == "__main__":
    main()