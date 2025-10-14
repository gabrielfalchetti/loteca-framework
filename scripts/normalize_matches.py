#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import re
from typing import Dict, Tuple

import pandas as pd
from unidecode import unidecode


def _slug(s: str) -> str:
    """
    Normaliza string para uma 'slug' comparável:
    - remove acentos
    - lowercase
    - troca separadores por espaço
    - remove tudo que não for [a-z0-9] ou espaço
    - compacta espaços
    """
    if s is None:
        return ""
    s = unidecode(str(s)).lower()
    s = (
        s.replace("(", " ")
         .replace(")", " ")
         .replace("/", " ")
         .replace("-", " ")
         .replace("_", " ")
         .replace(".", " ")
         .replace(",", " ")
         .replace("'", " ")
         .replace("’", " ")
         .replace("´", " ")
         .replace("`", " ")
         .replace("º", " ")
         .replace("°", " ")
         .replace("&", " and ")
    )
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _canon_case(s: str) -> str:
    """
    Title-case com alguns ajustes comuns para siglas.
    """
    if not s:
        return s
    # title default
    t = " ".join(w.capitalize() for w in s.split())
    # ajustes de sigla
    t = re.sub(r"\bFc\b", "FC", t)
    t = re.sub(r"\bSc\b", "SC", t)
    t = re.sub(r"\bSp\b", "SP", t)
    t = re.sub(r"\bPr\b", "PR", t)
    t = re.sub(r"\bGo\b", "GO", t)
    t = re.sub(r"\bPa\b", "PA", t)
    t = re.sub(r"\bMg\b", "MG", t)
    t = re.sub(r"\bRj\b", "RJ", t)
    t = re.sub(r"\bRs\b", "RS", t)
    return t


def _aliases_map() -> Dict[str, str]:
    """
    Mapa de aliases -> canônico.
    A chave deve vir no formato slug (ver _slug),
    o valor é a forma canônica que queremos exibir/trabalhar.
    """
    m: Dict[str, str] = {}

    def add(canon: str, *variants: str):
        canon_out = canon  # canônico de saída
        for v in variants:
            m[_slug(v)] = canon_out
        # também mapeia o próprio canônico em slug
        m[_slug(canon)] = canon_out

    # Times brasileiros citados + variações comuns
    add("Athletico-PR",
        "Athletico PR", "Atletico PR", "Atletico Paranaense", "Athletico Paranaense",
        "Atletico-PR", "AthleticoPR", "AtleticoPR"
    )

    add("Atlético-GO",
        "Atletico GO", "Atletico Goianiense", "Atletico-GO", "Atletico Goiania",
        "Atletico Go", "Atl Goianiense"
    )

    add("Botafogo-SP",
        "Botafogo SP", "Botafogo Ribeirao Preto", "Botafogo de Ribeirao Preto",
        "Botafogo (SP)", "Botafogo-SP", "Botafogo RP"
    )

    add("Ferroviária",
        "Ferroviaria", "Ferroviaria SP", "Associacao Ferroviaria de Esportes", "AFE"
    )

    add("Volta Redonda", "Volta Redonda FC", "Volta Redonda Futebol Clube", "Volta Redonda-RJ")

    add("Chapecoense",
        "Chapecoense AF", "Associacao Chapecoense de Futebol", "Chapecoense-SC", "Chape"
    )

    add("Avaí",
        "Avai", "Avai FC", "Avai-SC", "Avaí SC"
    )

    add("CRB",
        "Clube de Regatas Brasil", "CRB Alagoas", "CRB-AL"
    )

    add("Paysandu",
        "Paysandu SC", "Paysandu Sport Club", "Paysandu-PA"
    )

    add("Remo",
        "Clube do Remo", "Remo PA", "Remo (PA)", "Remo-PA"
    )

    # Alguns exemplos extras comuns
    add("São Paulo", "Sao Paulo", "Sao Paulo FC", "SPFC")
    add("Flamengo", "Clube de Regatas do Flamengo", "Flamengo RJ", "Flamengo-RJ")
    add("Fluminense", "Fluminense RJ", "Fluminense-RJ")
    add("Grêmio", "Gremio", "Gremio FBPA", "Gremio-RS")
    add("Internacional", "SC Internacional", "Internacional RS", "Internacional-RS")
    add("Atlético-MG", "Atletico MG", "Clube Atletico Mineiro", "Atletico-MG", "Atletico Mineiro")
    add("Corinthians", "SC Corinthians Paulista", "Corinthians SP", "Corinthians-SP")
    add("Palmeiras", "SE Palmeiras", "Palmeiras SP", "Palmeiras-SP")
    add("Santos", "Santos FC", "Santos SP", "Santos-SP")

    # Seleções (exemplos)
    add("Eslováquia", "Slovakia", "Eslovakia", "Slovak Republic")
    add("Luxemburgo", "Luxembourg")
    add("Eslovênia", "Slovenia", "Eslovenia")
    add("Suíça", "Switzerland", "Suica")
    add("Irlanda do Norte", "Northern Ireland")
    add("Alemanha", "Germany")
    add("Islândia", "Iceland", "Islandia")
    add("França", "France", "Franca")
    add("País de Gales", "Wales", "Pais de Gales")
    add("Bélgica", "Belgium", "Belgica")
    add("Suécia", "Sweden", "Suecia")
    add("Kosovo", "Kosova")
    add("Macedônia do Norte", "North Macedonia", "Macedonia do Norte", "Macedonia")
    add("Cazaquistão", "Kazakhstan", "Cazaquistao")
    add("Ucrânia", "Ukraine", "Ucrania")
    add("Azerbaijão", "Azerbaijan", "Azerbaijao")

    return m


ALIASES = _aliases_map()


def normalize_team(name: str) -> str:
    """
    Retorna forma canônica a partir do nome informado.
    Se não achar em ALIASES, devolve versão levemente normalizada e caseada.
    """
    if not name:
        return name
    key = _slug(name)
    if key in ALIASES:
        return ALIASES[key]
    # fallback: tenta melhorar pequenos detalhes
    # Mantém acentos em alguns conhecidos (título + siglas)
    base = _canon_case(name.strip())
    # Correções frequentes de hífen UF
    base = re.sub(r"\b([A-Za-z]+)\s+(SP|RJ|PR|RS|MG|GO|PA)\b", r"\1-\2", base)
    return base


def load_and_validate(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"::error::Arquivo de entrada não encontrado: {path}", file=sys.stderr)
        sys.exit(3)
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"::error::Falha ao ler CSV {path}: {e}", file=sys.stderr)
        sys.exit(3)

    # Normaliza nomes das colunas para garantir match_id, home, away (case-insensitive)
    cols = {c.lower().strip(): c for c in df.columns}
    # Possíveis alternativas
    col_match = cols.get("match_id") or cols.get("id") or cols.get("jogo") or cols.get("game_id")
    col_home = cols.get("home") or cols.get("team_home") or cols.get("mandante")
    col_away = cols.get("away") or cols.get("team_away") or cols.get("visitante")

    missing = []
    if not col_match: missing.append("match_id")
    if not col_home: missing.append("home")
    if not col_away: missing.append("away")
    if missing:
        print(f"::error::CSV precisa conter colunas: match_id, home, away (faltando: {', '.join(missing)})", file=sys.stderr)
        sys.exit(3)

    out = df.rename(columns={
        col_match: "match_id",
        col_home: "home",
        col_away: "away"
    })[["match_id", "home", "away"]].copy()

    # Sanitiza espaços
    for c in ["home", "away"]:
        out[c] = out[c].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

    return out


def main():
    ap = argparse.ArgumentParser(description="Normaliza nomes de times/seleções para matching com APIs.")
    ap.add_argument("--in_csv", required=True, help="Arquivo de entrada (ex: data/in/matches_source.csv)")
    ap.add_argument("--out_csv", required=True, help="Arquivo de saída normalizado (ex: data/out/.../matches_norm.csv)")
    args = ap.parse_args()

    df = load_and_validate(args.in_csv)

    # Cria colunas normalizadas sem alterar as originais
    df["home_norm"] = df["home"].apply(normalize_team)
    df["away_norm"] = df["away"].apply(normalize_team)

    # Log básico de alterações
    diffs = []
    for _, r in df.iterrows():
        if r["home"] != r["home_norm"] or r["away"] != r["away_norm"]:
            diffs.append(f"{r['match_id']}: '{r['home']}'→'{r['home_norm']}'  |  '{r['away']}'→'{r['away_norm']}'")
    if diffs:
        print("[normalize] Ajustes aplicados:")
        for line in diffs:
            print(f"[normalize] {line}")
    else:
        print("[normalize] Nenhum ajuste necessário; nomes já estavam canônicos.")

    # Garante diretório de saída
    os.makedirs(os.path.dirname(os.path.abspath(args.out_csv)), exist_ok=True)

    # Salva CSV
    df.to_csv(args.out_csv, index=False)
    print(f"[normalize] OK — gravado em {args.out_csv}  linhas={len(df)}")


if __name__ == "__main__":
    main()