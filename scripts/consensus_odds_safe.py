#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/consensus_odds_safe.py  — versão STRICT

Função:
  Gera odds de consenso reais, cruzando TheOddsAPI + API-Football com a whitelist da rodada.

Política:
  🚫 NUNCA aceita vazios, ausentes ou odds não casadas.
  🚫 NUNCA cria dados fictícios.
  ✅ Exige casamento 100% entre whitelist e odds de fontes reais.

Saída obrigatória:
  data/out/<RODADA_ID>/odds_consensus.csv

Erro crítico:
  EXIT 99 se:
    - qualquer fonte (theoddsapi/apifootball) estiver vazia,
    - qualquer jogo da whitelist não tiver odds casadas.
"""

import argparse
import os
import sys
import re
import glob
import pandas as pd

EXIT_CRITICAL = 99
EXIT_OK = 0

# ========== Logging ==========
def log(msg): print(msg, flush=True)
def err(msg): print(f"::error::{msg}", flush=True)
def warn(msg): print(f"Warning: {msg}", flush=True)

# ========== Normalização ==========
_norm_space = re.compile(r"\s+")
_norm_punct = re.compile(r"[^\w\s]+")

def normalize(s: str) -> str:
    if not isinstance(s, str): return ""
    import unicodedata
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = _norm_punct.sub(" ", s.lower())
    s = _norm_space.sub(" ", s).strip()
    return s

# ========== Aliases ==========
def load_aliases():
    aliases = {}
    for path in sorted(glob.glob("data/aliases/*.csv")):
        df = pd.read_csv(path)
        if not {"from","to"}.issubset(df.columns.str.lower()):
            warn(f"[aliases] ignorado {path} (sem colunas from/to)")
            continue
        for _, r in df.iterrows():
            f = normalize(str(r.get("from","")))
            t = normalize(str(r.get("to","")))
            if f: aliases[f] = t
    log(f"[aliases] {len(aliases)} aliases carregados")
    return aliases

def apply_aliases(s, aliases):
    n = normalize(s)
    return aliases.get(n, n)

# ========== Carregamento de fontes ==========
def safe_read(path, req):
    if not os.path.isfile(path):
        err(f"[consensus] Arquivo ausente: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path)
    miss = [c for c in req if c not in df.columns]
    if miss:
        err(f"[consensus] Colunas faltando em {path}: {miss}")
        return pd.DataFrame()
    return df

def load_odds(out_dir):
    req = ["home","away","odds_home","odds_draw","odds_away"]
    the_path = os.path.join(out_dir,"odds_theoddsapi.csv")
    api_path = os.path.join(out_dir,"odds_apifootball.csv")
    d1 = safe_read(the_path, req)
    d2 = safe_read(api_path, req)
    if d1.empty or d2.empty:
        err(f"[CRITICAL] Fontes de odds incompletas. theodds={len(d1)} apifoot={len(d2)}")
        sys.exit(EXIT_CRITICAL)
    d1["source"]="theoddsapi"
    d2["source"]="apifootball"
    return pd.concat([d1,d2],ignore_index=True)

# ========== Execução principal ==========
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True)
    args = parser.parse_args()
    out_dir = args.rodada if os.path.isdir(args.rodada) else os.path.join("data","out",str(args.rodada))
    os.makedirs(out_dir, exist_ok=True)

    log("[consensus] ===================================================")
    log(f"[consensus] STRICT MODE — RODADA: {out_dir}")
    log("[consensus] ===================================================")

    wl_path = os.path.join(out_dir,"matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        err("[CRITICAL] matches_whitelist.csv ausente")
        sys.exit(EXIT_CRITICAL)
    wl = pd.read_csv(wl_path)
    if wl.empty:
        err("[CRITICAL] whitelist vazia")
        sys.exit(EXIT_CRITICAL)

    aliases = load_aliases()
    wl["nh"] = wl["team_home"].map(lambda x: apply_aliases(x, aliases))
    wl["na"] = wl["team_away"].map(lambda x: apply_aliases(x, aliases))

    odds = load_odds(out_dir)
    odds["nh"] = odds["home"].map(lambda x: apply_aliases(x, aliases))
    odds["na"] = odds["away"].map(lambda x: apply_aliases(x, aliases))

    grp = (
        odds.groupby(["nh","na"],dropna=False)[["odds_home","odds_draw","odds_away"]]
        .mean().reset_index()
    )
    merged = wl.merge(grp,on=["nh","na"],how="left",suffixes=("_wl","_odds"))
    miss = merged[merged["odds_home"].isna()]
    if not miss.empty:
        err("[CRITICAL] Existem jogos sem odds casadas! Abortando.")
        print("==== JOGOS SEM ODDS ====")
        print(miss[["match_id","team_home","team_away"]].to_string(index=False))
        print("=========================")
        sys.exit(EXIT_CRITICAL)

    out = os.path.join(out_dir,"odds_consensus.csv")
    merged[["match_id","team_home","team_away","odds_home","odds_draw","odds_away"]].assign(source="consensus").to_csv(out,index=False)
    log(f"[consensus] ✅ OK ({len(merged)} jogos processados) -> {out}")
    sys.exit(EXIT_OK)

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        err(f"[CRITICAL] Falha inesperada: {e}")
        sys.exit(EXIT_CRITICAL)