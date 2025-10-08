#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera data/out/<RODADA_ID>/matches_whitelist.csv a partir de data/in/matches_source.csv.

Entrada obrigatória (data/in/matches_source.csv):
  match_id,home,away,source,lat,lon

Saída (OUT_DIR/matches_whitelist.csv):
  match_id,match_key,team_home,team_away,source

- Normaliza nomes (remove acentos, espaços -> hífen, caixa-baixa, mantém dígitos e letras).
- match_key: "<slug(home)>__vs__<slug(away)>"
- Faz logs verbosos quando DEBUG=true
"""

import argparse
import csv
import os
import sys
import unicodedata
from typing import Optional

import pandas as pd

def log(msg: str):
    print(f"[whitelist] {msg}", flush=True)

def err(msg: str):
    print(f"##[error][whitelist] {msg}", flush=True)
    sys.exit(6)

def getenv_bool(name: str, default: bool=False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1","true","yes","y","on"}

DEBUG = getenv_bool("DEBUG", False)

def debug(msg: str):
    if DEBUG:
        print(f"[whitelist][DEBUG] {msg}", flush=True)

def slugify_team(name: str) -> str:
    """
    Normaliza nome do time para slug (similar ao observado no pipeline):
    - remove acentos
    - to lower
    - substitui espaços e underscores por hífen
    - mantém letras, dígitos e hífens; remove demais caracteres
    - compacta hífens repetidos
    """
    if name is None:
        return ""
    s = str(name).strip()
    # remove acentos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    # separadores básicos -> hífen
    s = s.replace("_", "-").replace(" ", "-")
    # mantém letras/dígitos/hífen
    keep = []
    for ch in s:
        if ch.isalnum() or ch == "-":
            keep.append(ch)
    s = "".join(keep)
    # remove hífens duplicados
    while "--" in s:
        s = s.replace("--", "-")
    # remove hífens nas pontas
    s = s.strip("-")
    return s

def build_match_key(home: str, away: str) -> str:
    return f"{slugify_team(home)}__vs__{slugify_team(away)}"

def main():
    parser = argparse.ArgumentParser(description="Gera matches_whitelist.csv a partir de matches_source.csv")
    parser.add_argument("--rodada", required=True, help="Diretório de saída da rodada (ex.: data/out/17598...)")
    parser.add_argument("--in", dest="infile", default="data/in/matches_source.csv", help="CSV de entrada com os jogos")
    parser.add_argument("--aliases", dest="aliases", default=None, help="(opcional) CSV de aliases (colunas: canonical,alias)")
    parser.add_argument("--debug", action="store_true", help="Força debug verbose independente da env DEBUG")
    args = parser.parse_args()

    if args.debug:
        global DEBUG
        DEBUG = True

    out_dir = args.rodada
    infile = args.infile
    aliases_file: Optional[str] = args.aliases

    log("===================================================")
    log("GERANDO MATCHES WHITELIST")
    log(f"Rodada (OUT_DIR): {out_dir}")
    log(f"Entrada: {infile}")
    if aliases_file:
        log(f"Aliases: {aliases_file}")
    log("===================================================")

    # Validar existência de OUT_DIR
    if not os.path.isdir(out_dir):
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception as e:
            err(f"Falha ao criar OUT_DIR '{out_dir}': {e}")

    # Ler matches_source
    if not os.path.isfile(infile):
        err(f"Arquivo de entrada não encontrado: {infile}")

    try:
        df = pd.read_csv(infile, dtype=str).fillna("")
    except Exception as e:
        err(f"Falha ao ler {infile}: {e}")

    # Validar colunas
    required = ["match_id","home","away","source","lat","lon"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        err(f"Colunas obrigatórias ausentes em {infile}: {missing}")

    # Opcional: aliases
    alias_map = {}
    if aliases_file and os.path.isfile(aliases_file):
        try:
            da = pd.read_csv(aliases_file, dtype=str).fillna("")
            need = {"canonical","alias"}
            if not need.issubset(set(da.columns)):
                log(f"Aviso: arquivo de aliases não contém colunas {need}. Ignorando.")
            else:
                # prioriza mapeamento alias->canonical minúsculo normalizado (sem acento para comparação)
                def norm_key(s: str) -> str:
                    s2 = unicodedata.normalize("NFKD", s or "")
                    s2 = "".join(c for c in s2 if not unicodedata.combining(c)).lower().strip()
                    return s2
                for _, r in da.iterrows():
                    alias_map[norm_key(r["alias"])] = r["canonical"]
                debug(f"Aliases carregados: {len(alias_map)}")
        except Exception as e:
            log(f"Aviso: falha ao ler aliases '{aliases_file}': {e}")

    def maybe_alias(s: str) -> str:
        if not alias_map:
            return s
        # normaliza para chave
        s2 = unicodedata.normalize("NFKD", s or "")
        s2 = "".join(c for c in s2 if not unicodedata.combining(c)).lower().strip()
        return alias_map.get(s2, s)

    # Normalização e geração do match_key
    rows = []
    for _, r in df.iterrows():
        match_id = str(r.get("match_id","")).strip()
        home_raw = str(r.get("home","")).strip()
        away_raw = str(r.get("away","")).strip()
        source = str(r.get("source","")).strip()

        if not match_id or not home_raw or not away_raw:
            log(f"Aviso: linha ignorada (match_id/home/away vazios): {r.to_dict()}")
            continue

        home = maybe_alias(home_raw)
        away = maybe_alias(away_raw)
        mkey = build_match_key(home, away)

        rows.append({
            "match_id": match_id,
            "match_key": mkey,
            "team_home": home,
            "team_away": away,
            "source": source,
        })

    if not rows:
        err("Nenhuma linha válida após normalização. Verifique o arquivo de entrada e aliases.")

    out_file = os.path.join(out_dir, "matches_whitelist.csv")
    try:
        pd.DataFrame(rows, columns=["match_id","match_key","team_home","team_away","source"])\
          .to_csv(out_file, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        err(f"Falha ao salvar {out_file}: {e}")

    log(f"OK -> {out_file} (linhas={len(rows)})")

    # Preview
    try:
        prev = pd.read_csv(out_file, dtype=str).fillna("")
        debug("Preview (até 10 linhas):")
        debug(prev.head(10).to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()