# scripts/match_whitelist.py
# -*- coding: utf-8 -*-
"""
Gera data/out/<RID>/matches_whitelist.csv a partir de data/in/matches_source.csv,
aplicando aliases (se existir data/in/team_aliases.csv) e criando um match_key estável.

Saída: columns = [match_id, team_home, team_away, match_key]

Uso:
  python -m scripts.match_whitelist --rodada data/out/<RID> [--debug]

Requisitos: pandas
"""
from __future__ import annotations
import argparse
import csv
import os
import sys
import unicodedata
import pandas as pd
from datetime import datetime

REQUIRED_COLS = ["match_id", "home", "away", "source", "lat", "lon"]

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[whitelist][{level}] {msg}", flush=True)

def die(msg: str, code: int = 6) -> None:
    log(msg, "ERROR")
    sys.exit(code)

def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return s
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm_team(s: str) -> str:
    """
    Normaliza para chave estável (lowercase, sem acento, pontuação e espaços -> '-').
    """
    if s is None:
        return ""
    s = s.strip()
    s = strip_accents(s)
    s = s.lower()
    # troca separadores comuns por espaço
    for ch in [",", ";", "/", "\\", "|", "_"]:
        s = s.replace(ch, " ")
    # compacta espaços e troca por hífen
    s = "-".join([t for t in s.split() if t])
    return s

def load_alias_map(path_alias: str) -> dict[str, str]:
    """
    Lê data/in/team_aliases.csv (se existir) no formato:
      alias,canonical
    e retorna {alias_normalizado: canonical_exato}
    """
    alias_map: dict[str, str] = {}
    if not os.path.isfile(path_alias):
        log(f"Arquivo de aliases não encontrado (opcional): {path_alias}", "DEBUG")
        return alias_map

    try:
        df = pd.read_csv(path_alias)
    except Exception as e:
        log(f"Falha lendo aliases '{path_alias}': {e}", "WARN")
        return alias_map

    want = {"alias", "canonical"}
    missing = want - set(c.lower() for c in df.columns)
    if missing:
        log(f"Colunas ausentes em aliases ({path_alias}): {missing} — ignorando.", "WARN")
        return alias_map

    # garantir nomes coerentes
    cols_lower = {c.lower(): c for c in df.columns}
    alias_col = cols_lower["alias"]
    canon_col = cols_lower["canonical"]

    for _, row in df.iterrows():
        a = str(row.get(alias_col, "") or "").strip()
        c = str(row.get(canon_col, "") or "").strip()
        if a and c:
            alias_map[norm_team(a)] = c
    log(f"Aliases carregados: {len(alias_map)}", "DEBUG")
    return alias_map

def apply_alias(name: str, alias_map: dict[str, str]) -> str:
    key = norm_team(name)
    return alias_map.get(key, name)

def build_match_key(home: str, away: str) -> str:
    return f"{norm_team(home)}__vs__{norm_team(away)}"

def main():
    ap = argparse.ArgumentParser(description="Gera matches_whitelist.csv a partir de matches_source.csv")
    ap.add_argument("--rodada", required=True, help="Diretório de saída da rodada (ex.: data/out/17598...)")
    ap.add_argument("--debug", action="store_true", help="Ativa logs de depuração")
    args = ap.parse_args()

    out_dir = args.rodada
    src_matches = os.path.join("data", "in", "matches_source.csv")
    src_aliases = os.path.join("data", "in", "team_aliases.csv")
    out_file = os.path.join(out_dir, "matches_whitelist.csv")

    if args.debug:
        log(f"RODADA DIR  : {out_dir}", "DEBUG")
        log(f"MATCHES SRC : {src_matches}", "DEBUG")
        log(f"ALIASES SRC : {src_aliases}", "DEBUG")
        log(f"OUT FILE    : {out_file}", "DEBUG")

    if not os.path.isdir(out_dir):
        die(f"Diretório de rodada inexistente: {out_dir}")

    if not os.path.isfile(src_matches):
        die(f"Entrada não encontrada: {src_matches}")

    try:
        df = pd.read_csv(src_matches)
    except Exception as e:
        die(f"Falha lendo {src_matches}: {e}")

    # valida colunas
    cols_lower = {c.lower(): c for c in df.columns}
    missing = [c for c in REQUIRED_COLS if c not in cols_lower]
    if missing:
        die(f"matches_source.csv sem colunas obrigatórias: {missing}")

    # normaliza nomes das colunas (acesso pelos originais mapeados)
    c_id = cols_lower["match_id"]
    c_home = cols_lower["home"]
    c_away = cols_lower["away"]

    # aplica aliases (opcional)
    alias_map = load_alias_map(src_aliases)
    df[c_home] = df[c_home].astype(str).map(lambda s: apply_alias(s, alias_map))
    df[c_away] = df[c_away].astype(str).map(lambda s: apply_alias(s, alias_map))

    # monta whitelist enxuta
    out_rows = []
    for _, r in df.iterrows():
        match_id = r[c_id]
        home = (r[c_home] or "").strip()
        away = (r[c_away] or "").strip()
        if not home or not away:
            log(f"Ignorando linha sem times válidos: match_id={match_id}", "WARN")
            continue
        mk = build_match_key(home, away)
        out_rows.append(
            {
                "match_id": match_id,
                "team_home": home,
                "team_away": away,
                "match_key": mk,
            }
        )

    if not out_rows:
        die("Nenhum jogo válido encontrado em matches_source.csv")

    os.makedirs(out_dir, exist_ok=True)
    pd.DataFrame(out_rows, columns=["match_id", "team_home", "team_away", "match_key"])\
      .to_csv(out_file, index=False, quoting=csv.QUOTE_MINIMAL)

    log(f"OK -> {out_file} (linhas={len(out_rows)})")
    # preview
    try:
        prev = pd.read_csv(out_file).head(10)
        with pd.option_context('display.max_columns', None, 'display.width', 200):
            log("Preview whitelist:\n" + prev.to_string(index=False), "DEBUG")
    except Exception:
        pass

if __name__ == "__main__":
    main()