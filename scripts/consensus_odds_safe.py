#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds_consensus.csv em data/out/<RODADA_ID>.
Se matches_whitelist.csv não existir, cria a partir de data/in/matches_source.csv.
Aplica data/teams_aliases.csv (alias->canonical) em whitelist e odds antes do match.

Saídas:
  data/out/<RODADA_ID>/matches_whitelist.csv
    colunas: match_id,match_key,team_home,team_away,source
  data/out/<RODADA_ID>/odds_consensus.csv
    colunas: match_id,team_home,team_away,odds_home,odds_draw,odds_away,source
"""

import argparse
import csv
import os
import sys
import unicodedata
from typing import Dict, List

import pandas as pd

# ---------------------------
# Logging
# ---------------------------
def log(msg: str): print(f"[consensus] {msg}", flush=True)
def warn(msg: str): print(f"##[warning][consensus] {msg}", flush=True)
def fail(msg: str):
    print(f"##[error][consensus] {msg}", flush=True)
    sys.exit(6)

def getenv_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in {"1","true","yes","y","on"}

DEBUG = getenv_bool("DEBUG", False)
def debug(msg: str):
    if DEBUG:
        print(f"[consensus][DEBUG] {msg}", flush=True)

# ---------------------------
# Texto / normalização
# ---------------------------
def strip_accents(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in s if not unicodedata.combining(c))

def slugify_team(name: str) -> str:
    s = strip_accents(name).lower().strip()
    s = s.replace("_","-").replace(" ","-")
    keep = []
    for ch in s:
        if ch.isalnum() or ch == "-":
            keep.append(ch)
    s = "".join(keep)
    while "--" in s: s = s.replace("--","-")
    return s.strip("-")

def make_key(home: str, away: str) -> str:
    return f"{slugify_team(home)}__vs__{slugify_team(away)}"

# ---------------------------
# Aliases
# ---------------------------
def load_aliases(path: str="data/teams_aliases.csv") -> Dict[str, str]:
    """
    Carrega arquivo alias,canonical.
    Retorna dict em lower-case e sem acentos para o 'alias' (chave).
    """
    mapping: Dict[str, str] = {}
    if not os.path.isfile(path):
        warn(f"{path} inexistente — seguindo sem aliases (pode causar não-casamento).")
        return mapping
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception as e:
        warn(f"Falha ao ler {path}: {e}. Seguindo sem aliases.")
        return mapping

    miss = [c for c in ("alias","canonical") if c not in df.columns]
    if miss:
        warn(f"{path} sem colunas {miss}. Seguindo sem aliases.")
        return {}

    for _, r in df.iterrows():
        alias = strip_accents(r["alias"]).lower().strip()
        canonical = str(r["canonical"]).strip()
        if alias and canonical:
            mapping[alias] = canonical
    debug(f"Aliases carregados: {len(mapping)}")
    return mapping

def apply_alias(name: str, aliases: Dict[str, str]) -> str:
    key = strip_accents(name).lower().strip()
    return aliases.get(key, name.strip())

# ---------------------------
# Whitelist
# ---------------------------
REQ_SRC_COLS = ["match_id","home","away","source","lat","lon"]

def ensure_whitelist(out_dir: str, infile: str, aliases: Dict[str, str]) -> pd.DataFrame:
    wl_path = os.path.join(out_dir, "matches_whitelist.csv")
    if os.path.isfile(wl_path):
        try:
            df = pd.read_csv(wl_path, dtype=str).fillna("")
            need = {"match_id","match_key","team_home","team_away","source"}
            if not need.issubset(df.columns):
                raise ValueError("colunas incorretas")
            # aplica aliases (idempotente)
            df["team_home"] = df["team_home"].apply(lambda x: apply_alias(x, aliases))
            df["team_away"] = df["team_away"].apply(lambda x: apply_alias(x, aliases))
            df["match_key"] = df.apply(lambda r: make_key(r["team_home"], r["team_away"]), axis=1)
            return df
        except Exception as e:
            warn(f"Whitelist existente inválida ({e}); recriando de {infile}.")

    if not os.path.isfile(infile):
        fail(f"matches_whitelist.csv ausente e {infile} não existe.")

    try:
        src = pd.read_csv(infile, dtype=str).fillna("")
    except Exception as e:
        fail(f"Falha ao ler {infile}: {e}")

    miss = [c for c in REQ_SRC_COLS if c not in src.columns]
    if miss:
        fail(f"Colunas obrigatórias ausentes em {infile}: {miss} (esperado: {REQ_SRC_COLS})")

    rows = []
    for _, r in src.iterrows():
        mid = str(r["match_id"]).strip()
        h = apply_alias(str(r["home"]).strip(), aliases)
        a = apply_alias(str(r["away"]).strip(), aliases)
        s = str(r["source"]).strip()
        if not mid or not h or not a:
            warn(f"Linha inválida (match_id/home/away): {r.to_dict()}")
            continue
        rows.append({
            "match_id": mid,
            "match_key": make_key(h, a),
            "team_home": h,
            "team_away": a,
            "source": s
        })
    if not rows:
        fail("Nenhuma linha válida para construir whitelist.")

    df_wl = pd.DataFrame(rows, columns=["match_id","match_key","team_home","team_away","source"])
    try:
        df_wl.to_csv(wl_path, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        fail(f"Falha ao salvar {wl_path}: {e}")

    log(f"Whitelist OK -> {wl_path} (linhas={len(df_wl)})")
    return df_wl

# ---------------------------
# Odds
# ---------------------------
def _normalize_odds_df(df: pd.DataFrame, home_col: str, away_col: str, aliases: Dict[str, str]) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    def pick(*names):
        for n in names:
            if n in cols: return cols[n]
        return None

    oh = pick("odds_home","odd_home")
    od = pick("odds_draw","odd_draw")
    oa = pick("odds_away","odd_away")
    if not (oh and od and oa):
        raise ValueError("Colunas de odds não encontradas (esperado odds_* ou odd_*)")

    out = pd.DataFrame({
        "team_home": df[home_col].astype(str).fillna(""),
        "team_away": df[away_col].astype(str).fillna(""),
        "odds_home": pd.to_numeric(df[oh], errors="coerce"),
        "odds_draw": pd.to_numeric(df[od], errors="coerce"),
        "odds_away": pd.to_numeric(df[oa], errors="coerce"),
    })
    # aplica aliases ANTES de montar a chave
    out["team_home"] = out["team_home"].apply(lambda x: apply_alias(x, aliases))
    out["team_away"] = out["team_away"].apply(lambda x: apply_alias(x, aliases))
    out["match_key"] = out.apply(lambda r: make_key(r["team_home"], r["team_away"]), axis=1)
    out = out.dropna(subset=["odds_home","odds_draw","odds_away"])
    return out

def read_all_odds(out_dir: str, aliases: Dict[str, str]) -> List[pd.DataFrame]:
    dfs = []

    # theoddsapi
    f1 = os.path.join(out_dir, "odds_theoddsapi.csv")
    if os.path.isfile(f1):
        try:
            d1 = pd.read_csv(f1, dtype=str).fillna("")
            d1 = _normalize_odds_df(d1, home_col="home", away_col="away", aliases=aliases)
            dfs.append(d1)
            debug(f"Carregado odds from theoddsapi: {len(d1)} linhas")
        except Exception as e:
            warn(f"Falha ao ler/normalizar {f1}: {e}")

    # apifootball
    f2 = os.path.join(out_dir, "odds_apifootball.csv")
    if os.path.isfile(f2):
        try:
            d2 = pd.read_csv(f2, dtype=str).fillna("")
            home_col = "team_home" if "team_home" in d2.columns else ("home" if "home" in d2.columns else None)
            away_col = "team_away" if "team_away" in d2.columns else ("away" if "away" in d2.columns else None)
            if not home_col or not away_col:
                raise ValueError("Colunas de time (team_home/team_away ou home/away) não encontradas")
            d2 = _normalize_odds_df(d2, home_col=home_col, away_col=away_col, aliases=aliases)
            dfs.append(d2)
            debug(f"Carregado odds from apifootball: {len(d2)} linhas")
        except Exception as e:
            warn(f"Falha ao ler/normalizar {f2}: {e}")

    return dfs

# ---------------------------
# Consenso
# ---------------------------
def build_consensus(df_wl: pd.DataFrame, odds_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    if not odds_dfs:
        fail("Nenhuma fonte de odds disponível (nem odds_theoddsapi.csv nem odds_apifootball.csv).")

    df_all = pd.concat(odds_dfs, ignore_index=True) if len(odds_dfs) > 1 else odds_dfs[0].copy()
    if df_all.empty:
        fail("Arquivos de odds lidos mas vazios após normalização.")

    g = df_all.groupby("match_key", as_index=False).agg({
        "odds_home":"mean",
        "odds_draw":"mean",
        "odds_away":"mean"
    })

    out = g.merge(df_wl[["match_id","match_key","team_home","team_away"]], on="match_key", how="right")

    # Diagnóstico: quem não casou?
    not_matched = out[out[["odds_home","odds_draw","odds_away"]].isna().any(axis=1)].copy()
    if not not_matched.empty:
        warn("Alguns jogos da whitelist não casaram com odds. Mostrando até 10:")
        try:
            print(not_matched[["match_id","team_home","team_away","match_key"]].head(10).to_string(index=False), flush=True)
        except Exception:
            pass

    out = out.dropna(subset=["odds_home","odds_draw","odds_away"])
    if out.empty:
        fail("Nenhum jogo da whitelist casou com odds disponíveis. Verifique aliases/nome dos times.")

    out = out[["match_id","team_home","team_away","odds_home","odds_draw","odds_away"]].copy()
    out["source"] = "consensus"
    return out

# ---------------------------
# main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="RODADA_ID (ex.: 17598...)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        global DEBUG
        DEBUG = True

    rodada_id = str(args.rodada).strip()
    out_dir = os.path.join("data","out", rodada_id)
    os.makedirs(out_dir, exist_ok=True)

    log("===================================================")
    log("GERANDO ODDS CONSENSUS")
    log(f"RODADA_ID: {rodada_id}")
    log(f"OUT_DIR  : {out_dir}")
    log("===================================================")

    # 0) aliases
    aliases = load_aliases("data/teams_aliases.csv")

    # 1) whitelist
    df_wl = ensure_whitelist(out_dir, "data/in/matches_source.csv", aliases)

    # 2) odds
    odds_dfs = read_all_odds(out_dir, aliases)

    # 3) consenso
    df_cons = build_consensus(df_wl, odds_dfs)

    # 4) salvar
    out_file = os.path.join(out_dir, "odds_consensus.csv")
    df_cons.to_csv(out_file, index=False, quoting=csv.QUOTE_MINIMAL)
    log(f"OK -> {out_file}")

    if DEBUG:
        try:
            debug("Preview odds_consensus (até 10):")
            debug(pd.read_csv(out_file).head(10).to_string(index=False))
        except Exception:
            pass

if __name__ == "__main__":
    main()