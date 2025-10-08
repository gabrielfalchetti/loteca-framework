#!/usr/bin/env python3
"""
Gera data/out/<RODADA>/matches_whitelist.csv a partir de data/in/matches_source.csv

Saída (CSV):
- match_id
- team_home
- team_away
- match_key  (normalizado: "<home>__<away>" em lowercase, sem acentos, espaços->hífens)

Regras:
- Valida cabeçalho obrigatório: match_id,home,away,source,lat,lon
- Ignora linhas vazias
- Se existir data/in/team_aliases.csv (home_name,away_name,alias), aplica normalização de nomes
- Falha com exit code 6 se algo crítico faltar

Uso:
  python -m scripts.match_whitelist --rodada data/out/<id> [--debug]
"""

from __future__ import annotations
import argparse
import csv
import os
import sys
import unicodedata
from typing import Dict, Tuple
import pandas as pd


def log(msg: str) -> None:
    print(f"[whitelist] {msg}", flush=True)


def err(msg: str) -> None:
    print(f"##[error]{msg}", flush=True)


def normalize_name(s: str) -> str:
    """remove acento, troca espaços/underscores por hífen, lowercase, trim múltiplos hífens"""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("_", " ").strip().lower()
    parts = [p for p in s.split() if p]
    return "-".join(parts)


def match_key(home: str, away: str) -> str:
    return f"{normalize_name(home)}__{normalize_name(away)}"


def load_aliases(path: str) -> Dict[str, str]:
    """
    Lê CSV simples com duas ou três colunas.
    Formatos aceitos:
      - alias,canonical
      - alias,home_or_away,canonical   (home_or_away é ignorado aqui)
    Retorna dict {alias_normalizado_lower: canonical_original}
    """
    aliases: Dict[str, str] = {}
    if not os.path.isfile(path):
        return aliases

    try:
        df = pd.read_csv(path)
    except Exception as e:
        log(f"AVISO: não foi possível ler aliases '{path}': {e}")
        return aliases

    cols = [c.strip().lower() for c in df.columns.tolist()]
    if set(cols) >= {"alias", "canonical"}:
        alias_col = "alias"
        canonical_col = "canonical"
    elif len(cols) >= 2:
        alias_col = df.columns[0]
        canonical_col = df.columns[-1]
    else:
        log("AVISO: team_aliases.csv com formato inesperado — ignorando.")
        return aliases

    for _, r in df.iterrows():
        a = str(r[alias_col]).strip()
        c = str(r[canonical_col]).strip()
        if a and c and a.lower() != "nan" and c.lower() != "nan":
            aliases[normalize_name(a)] = c
    log(f"aliases carregados: {len(aliases)}")
    return aliases


def apply_alias(name: str, aliases: Dict[str, str]) -> str:
    key = normalize_name(name)
    return aliases.get(key, name)


def validate_header(df: pd.DataFrame) -> Tuple[bool, list]:
    need = ["match_id", "home", "away", "source", "lat", "lon"]
    cols = [c.strip().lower() for c in df.columns.tolist()]
    miss = [c for c in need if c not in cols]
    return (len(miss) == 0, miss)


def build_whitelist(in_file: str, out_dir: str, debug: bool = False) -> str:
    if not os.path.isfile(in_file):
        err(f"Arquivo de entrada ausente: {in_file}")
        sys.exit(6)

    df = pd.read_csv(in_file)
    ok, miss = validate_header(df)
    if not ok:
        err(f"Cabeçalhos ausentes em matches_source: {miss}")
        sys.exit(6)

    # normaliza nomes das colunas para acesso seguro
    df.columns = [c.strip().lower() for c in df.columns]

    # remove linhas completamente vazias em home/away
    df = df.dropna(subset=["home", "away"], how="any")
    df = df[(df["home"].astype(str).str.strip() != "") & (df["away"].astype(str).str.strip() != "")]
    if len(df) == 0:
        err("Nenhum jogo válido após limpeza.")
        sys.exit(6)

    # carrega aliases (opcional)
    aliases_path = os.path.join("data", "in", "team_aliases.csv")
    aliases = load_aliases(aliases_path)

    # aplica aliases
    homes = []
    aways = []
    keys = []
    ids = []

    for _, r in df.iterrows():
        mid = str(r["match_id"]).strip()
        h = str(r["home"]).strip()
        a = str(r["away"]).strip()

        h2 = apply_alias(h, aliases) if aliases else h
        a2 = apply_alias(a, aliases) if aliases else a

        homes.append(h2)
        aways.append(a2)
        keys.append(match_key(h2, a2))
        ids.append(mid)

    out_df = pd.DataFrame(
        {
            "match_id": ids,
            "team_home": homes,
            "team_away": aways,
            "match_key": keys,
        }
    )

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "matches_whitelist.csv")
    out_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    if debug:
        log("preview:")
        print(out_df.head(10).to_string(index=False))

    log(f"OK -> {out_path} (linhas={len(out_df)})")
    return out_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", "--out-dir", dest="out_dir", required=True, help="Diretório da rodada (ex.: data/out/1759...)")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    in_file = os.path.join("data", "in", "matches_source.csv")
    build_whitelist(in_file, args.out_dir, debug=args.debug)


if __name__ == "__main__":
    main()