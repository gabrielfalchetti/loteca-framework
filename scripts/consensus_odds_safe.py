#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds_consensus.csv em data/out/<RODADA_ID>, sem depender de passo externo.
Se matches_whitelist.csv não existir, o script o cria a partir de data/in/matches_source.csv.

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
from typing import List, Dict, Tuple, Optional

import pandas as pd

# ---------------------------
# Logging helpers
# ---------------------------
def log(msg: str): print(f"[consensus] {msg}", flush=True)
def warn(msg: str): print(f"##[warning][consensus] {msg}", flush=True)
def err(msg: str):
    print(f"##[error][consensus] {msg}", flush=True)
    sys.exit(6)

def getenv_bool(name: str, default: bool=False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1","true","yes","y","on"}

DEBUG = getenv_bool("DEBUG", False)
def debug(msg: str):
    if DEBUG:
        print(f"[consensus][DEBUG] {msg}", flush=True)

# ---------------------------
# Normalização de times e match_key
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

def match_key(home: str, away: str) -> str:
    return f"{slugify_team(home)}__vs__{slugify_team(away)}"

# ---------------------------
# Whitelist (gera on-demand se faltar)
# ---------------------------
REQ_SRC_COLS = ["match_id","home","away","source","lat","lon"]

def ensure_whitelist(out_dir: str, infile: str="data/in/matches_source.csv") -> pd.DataFrame:
    """Garante que OUT_DIR/matches_whitelist.csv exista; se não, cria a partir do matches_source."""
    wl_path = os.path.join(out_dir, "matches_whitelist.csv")
    if os.path.isfile(wl_path):
        debug(f"Whitelist já existe: {wl_path}")
        try:
            df = pd.read_csv(wl_path, dtype=str).fillna("")
            need = {"match_id","match_key","team_home","team_away","source"}
            if not need.issubset(df.columns):
                warn(f"{wl_path} não tem colunas {need}. Recriando a partir de {infile}.")
                raise ValueError("columns mismatch")
            return df
        except Exception as e:
            warn(f"Falha ao ler whitelist existente ({e}); tentando recriar de {infile}.")

    # Criar do zero
    if not os.path.isfile(infile):
        err(f"matches_whitelist.csv ausente e {infile} também não existe.")
    try:
        df_src = pd.read_csv(infile, dtype=str).fillna("")
    except Exception as e:
        err(f"Falha ao ler {infile}: {e}")

    miss = [c for c in REQ_SRC_COLS if c not in df_src.columns]
    if miss:
        err(f"Colunas obrigatórias ausentes em {infile}: {miss} (esperado: {REQ_SRC_COLS})")

    rows = []
    for _, r in df_src.iterrows():
        mid = str(r["match_id"]).strip()
        home = str(r["home"]).strip()
        away = str(r["away"]).strip()
        src  = str(r["source"]).strip()
        if not mid or not home or not away:
            warn(f"Linha ignorada em matches_source (faltando match_id/home/away): {r.to_dict()}")
            continue
        rows.append({
            "match_id": mid,
            "match_key": match_key(home, away),
            "team_home": home,
            "team_away": away,
            "source": src
        })

    if not rows:
        err("Nenhuma linha válida para construir whitelist.")

    df_wl = pd.DataFrame(rows, columns=["match_id","match_key","team_home","team_away","source"])
    try:
        df_wl.to_csv(wl_path, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        err(f"Falha ao salvar {wl_path}: {e}")

    log(f"Whitelist OK -> {wl_path} (linhas={len(df_wl)})")
    return df_wl

# ---------------------------
# Leitura e normalização das odds
# ---------------------------
def _normalize_odds_df(df: pd.DataFrame, home_col: str, away_col: str) -> pd.DataFrame:
    # tenta mapear colunas possíveis para odds_home/draw/away
    # aceita: odds_home/odds_draw/odds_away OR odd_home/odd_draw/odd_away
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
    out["match_key"] = out.apply(lambda r: match_key(r["team_home"], r["team_away"]), axis=1)
    out = out.dropna(subset=["odds_home","odds_draw","odds_away"])
    return out

def read_all_odds(out_dir: str) -> List[pd.DataFrame]:
    """Lê odds_theoddsapi.csv e odds_apifootball.csv (se existir) e devolve dataframes normalizados."""
    dfs = []

    # TheOddsAPI
    f1 = os.path.join(out_dir, "odds_theoddsapi.csv")
    if os.path.isfile(f1):
        try:
            d1 = pd.read_csv(f1, dtype=str).fillna("")
            # colunas esperadas no seu pipeline: home,away,odds_home,odds_draw,odds_away
            d1 = _normalize_odds_df(d1, home_col="home", away_col="away")
            dfs.append(d1)
            debug(f"Carregado odds from theoddsapi: {len(d1)} linhas")
        except Exception as e:
            warn(f"Falha ao ler/normalizar {f1}: {e}")

    # API-Football (se existir)
    f2 = os.path.join(out_dir, "odds_apifootball.csv")
    if os.path.isfile(f2):
        try:
            d2 = pd.read_csv(f2, dtype=str).fillna("")
            # aceita nomes alternativos para times
            home_col = "team_home" if "team_home" in d2.columns else ("home" if "home" in d2.columns else None)
            away_col = "team_away" if "team_away" in d2.columns else ("away" if "away" in d2.columns else None)
            if not home_col or not away_col:
                raise ValueError("Colunas de time (team_home/team_away ou home/away) não encontradas")
            d2 = _normalize_odds_df(d2, home_col=home_col, away_col=away_col)
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
        err("Nenhuma fonte de odds disponível (nem odds_theoddsapi.csv nem odds_apifootball.csv).")

    df_all = pd.concat(odds_dfs, ignore_index=True) if len(odds_dfs) > 1 else odds_dfs[0].copy()
    if df_all.empty:
        err("Arquivos de odds lidos mas vazios após normalização.")

    # média por match_key
    g = df_all.groupby("match_key", as_index=False).agg({
        "odds_home":"mean",
        "odds_draw":"mean",
        "odds_away":"mean"
    })

    # junta com whitelist para pegar match_id e nomes "originais"
    need = {"match_id","match_key","team_home","team_away"}
    if not need.issubset(df_wl.columns):
        err(f"Whitelist não possui colunas {need}")

    out = g.merge(df_wl[["match_id","match_key","team_home","team_away"]], on="match_key", how="right")
    # Mantém apenas os jogos listados na whitelist (o RIGHT garante isso); jogos sem odds ficam NaN
    out = out.dropna(subset=["odds_home","odds_draw","odds_away"])

    if out.empty:
        err("Nenhum jogo da whitelist casou com odds disponíveis. Verifique nomes/aliases.")

    out = out[["match_id","team_home","team_away","odds_home","odds_draw","odds_away"]].copy()
    out["source"] = "consensus"
    return out

# ---------------------------
# main
# ---------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="RODADA_ID (não o caminho). Ex.: 17598...")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    if args.debug:
        global DEBUG
        DEBUG = True

    rodada_id = str(args.rodada).strip()
    out_dir = os.path.join("data","out", rodada_id)

    if not os.path.isdir(out_dir):
        try: os.makedirs(out_dir, exist_ok=True)
        except Exception as e: err(f"Falha ao criar OUT_DIR '{out_dir}': {e}")

    log("===================================================")
    log("GERANDO ODDS CONSENSUS")
    log(f"RODADA_ID: {rodada_id}")
    log(f"OUT_DIR  : {out_dir}")
    log("===================================================")

    # 1) Garantir whitelist
    df_wl = ensure_whitelist(out_dir, infile="data/in/matches_source.csv")

    # 2) Ler odds
    odds_dfs = read_all_odds(out_dir)

    # 3) Consenso
    df_cons = build_consensus(df_wl, odds_dfs)

    # 4) Salvar
    out_file = os.path.join(out_dir, "odds_consensus.csv")
    try:
        df_cons.to_csv(out_file, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        err(f"Falha ao salvar {out_file}: {e}")

    log(f"OK -> {out_file}")
    # preview amigável
    try:
        debug("Preview (até 10 linhas):")
        debug(pd.read_csv(out_file).head(10).to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()