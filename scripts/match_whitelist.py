#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
match_whitelist.py
------------------
Gera o arquivo data/out/<RODADA_ID>/matches_whitelist.csv a partir de data/in/matches_source.csv.

- Valida cabeçalho obrigatório: match_id,home,away,source,lat,lon
- Normaliza nomes dos times e cria uma chave estável `match_key` no formato:
    <home_normalizado>__vs__<away_normalizado>
- Copia latitude/longitude (quando existirem) para futuro uso por steps de clima
- Loga mensagens amigáveis e falha com código de saída != 0 em caso de erro

Uso:
    python -m scripts.match_whitelist --rodada <OUT_DIR>

Exemplo:
    python -m scripts.match_whitelist --rodada "data/out/1759844885"

Saída esperada:
    data/out/<RODADA_ID>/matches_whitelist.csv com colunas:
        match_id,team_home,team_away,match_key,lat,lon,source
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import unicodedata
from typing import Dict, Any, List

import pandas as pd


REQUIRED_COLS = ["match_id", "home", "away", "source", "lat", "lon"]
INPUT_FILE = os.path.join("data", "in", "matches_source.csv")
OUTPUT_BASENAME = "matches_whitelist.csv"


def log(msg: str) -> None:
    print(f"[whitelist] {msg}")


def fail(msg: str, code: int = 6) -> None:
    print(f"##[error]{msg}")
    sys.exit(code)


def _strip_lower_noaccents(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s_norm = unicodedata.normalize("NFKD", s)
    s_ascii = "".join(ch for ch in s_norm if not unicodedata.combining(ch))
    return s_ascii.lower()


def _slug_team(s: str) -> str:
    s0 = _strip_lower_noaccents(s)
    # mantém letras, números e separadores simples
    out = []
    for ch in s0:
        if ch.isalnum():
            out.append(ch)
        elif ch in [" ", "-", "_", "/"]:
            out.append("-")
        else:
            # ignora outros sinais
            out.append("")
    slug = "".join(out)
    # normaliza múltiplos hífens
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


def make_match_key(team_home: str, team_away: str) -> str:
    return f"{_slug_team(team_home)}__vs__{_slug_team(team_away)}"


def load_matches_source(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        fail(f"Entrada {path} não encontrada. Esperado cabeçalho: {', '.join(REQUIRED_COLS)}")

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception as e:
        fail(f"Falha ao ler {path}: {e}")

    # Normaliza cabeçalho (tira espaços/CR e força lower)
    df.columns = [c.strip() for c in df.columns]
    header = [c.lower() for c in df.columns]

    missing = [c for c in REQUIRED_COLS if c not in header]
    if missing:
        fail(f"Cabecalhos ausentes em {path}: {missing}. Use: {', '.join(REQUIRED_COLS)}")

    # Reordena/renomeia para nomes fixos
    rename_map = {c: c.lower() for c in df.columns}
    df = df.rename(columns=rename_map)

    # Sanitiza valores string (trim)
    for c in ["match_id", "home", "away", "source", "lat", "lon"]:
        df[c] = df[c].astype(str).map(lambda x: x.strip())

    # Remove linhas sem match_id ou times
    before = len(df)
    df = df[(df["match_id"] != "") & (df["home"] != "") & (df["away"] != "")]
    after = len(df)
    if after == 0:
        fail(f"Nenhum jogo válido em {path}.")
    if after < before:
        log(f"Descartadas {before - after} linhas inválidas (sem match_id/home/away).")

    return df


def build_whitelist(df_src: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for _, r in df_src.iterrows():
        match_id = r["match_id"]
        team_home = r["home"]
        team_away = r["away"]
        lat = r.get("lat", "")
        lon = r.get("lon", "")
        source = r.get("source", "")

        mk = make_match_key(team_home, team_away)

        rows.append(
            {
                "match_id": match_id,
                "team_home": team_home,
                "team_away": team_away,
                "match_key": mk,
                "lat": lat,
                "lon": lon,
                "source": source,
            }
        )

    df_out = pd.DataFrame(rows, columns=["match_id", "team_home", "team_away", "match_key", "lat", "lon", "source"])

    # Checa duplicatas por match_id e por match_key (apenas alerta, mas mantemos)
    dup_id = df_out["match_id"].duplicated(keep=False).sum()
    dup_key = df_out["match_key"].duplicated(keep=False).sum()
    if dup_id:
        log(f"AVISO: {dup_id} registros com match_id duplicado.")
    if dup_key:
        log(f"AVISO: {dup_key} registros com match_key duplicado.")

    return df_out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gera matches_whitelist.csv a partir de data/in/matches_source.csv")
    p.add_argument(
        "--rodada",
        required=True,
        help="Diretório de saída da rodada (ex.: data/out/1759844885). O arquivo matches_whitelist.csv será criado aqui.",
    )
    p.add_argument("--debug", action="store_true", help="Exibe logs mais verbosos")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    out_dir = args.rodada
    if not isinstance(out_dir, str) or out_dir.strip() == "":
        fail("Parâmetro --rodada inválido (diretório de saída vazio).")

    os.makedirs(out_dir, exist_ok=True)

    log("INICIANDO GERAÇÃO DO WHITELIST")
    log(f"Fonte de jogos : {INPUT_FILE}")
    log(f"Saída (rodada) : {out_dir}")

    df_src = load_matches_source(INPUT_FILE)
    log(f"Linhas de entrada: {len(df_src)}")

    df_white = build_whitelist(df_src)
    log(f"Linhas na whitelist: {len(df_white)}")

    out_path = os.path.join(out_dir, OUTPUT_BASENAME)
    try:
        df_white.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        fail(f"Falha ao salvar {out_path}: {e}")

    # Prévia de saída
    try:
        head_preview = df_white.head(10)
        with pd.option_context("display.max_columns", None, "display.width", 200):
            log("Prévia:")
            print(head_preview)
    except Exception:
        pass

    log(f"OK -> {out_path}")


if __name__ == "__main__":
    main()