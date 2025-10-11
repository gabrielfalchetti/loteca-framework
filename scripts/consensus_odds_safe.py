#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds de consenso a partir dos arquivos:
  - <rodada>/odds_theoddsapi.csv
  - <rodada>/odds_apifootball.csv

Saída:
  - <rodada>/odds_consensus.csv

Colunas:
  team_home, team_away, odds_home, odds_draw, odds_away

Regras:
 - Junta por nomes normalizados (tolerante a acentos, sufixos tipo "FC", siglas de estado etc.).
 - Se houver múltiplas fontes, usa MEDIANA por mercado (mais robusta).
 - Em modo --strict, falha se QUALQUER jogo da whitelist não tiver odds em nenhuma fonte.
"""

import os
import re
import sys
import json
import math
import argparse
from unicodedata import normalize as _ucnorm

import pandas as pd

REQ_COLS = ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
WL_MAP = {"match_id": "match_id", "home": "team_home", "away": "team_away"}

STOPWORD_TOKENS = {
    "aa","ec","ac","sc","fc","afc","cf","ca","cd","ud",
    "sp","pr","rj","rs","mg","go","mt","ms","pa","pe","pb","rn","ce","ba","al","se","pi","ma","df","es","sc",
}

def log(level, msg):
    print(f"[consensus] {msg}" if level == "INFO" else f"[consensus][{level}] {msg}", flush=True)

def _deaccent(s: str) -> str:
    return _ucnorm("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")

def norm_key(name: str) -> str:
    s = _deaccent(name).lower()
    s = s.replace("&", " e ")
    s = re.sub(r"[/()\-_.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_key_tokens(name: str) -> str:
    """Normalização *por tokens* removendo stopwords comuns (AA, FC, siglas de estado etc.)."""
    toks = [t for t in re.split(r"\s+", norm_key(name)) if t and t not in STOPWORD_TOKENS]
    return " ".join(toks)

def read_csv_safe(path: str, expected_cols=None):
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        # uniformizar nomes esperados se presentes
        if expected_cols:
            for c in expected_cols:
                if c not in df.columns:
                    # tentar mapear variantes comuns
                    alt = None
                    lower_cols = {col.lower(): col for col in df.columns}
                    if c in lower_cols:
                        alt = lower_cols[c]
                    if alt and alt != c:
                        df[c] = df[alt]
        return df
    except Exception as e:
        log("WARN", f"Falha lendo {path}: {e}")
        return pd.DataFrame()

def secure_float(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def median_ignore_none(vals):
    f = [secure_float(v) for v in vals if secure_float(v) is not None]
    if not f:
        return None
    f.sort()
    n = len(f)
    mid = n // 2
    if n % 2 == 1:
        return f[mid]
    return (f[mid - 1] + f[mid]) / 2.0

def build_index(df: pd.DataFrame, home_col="home", away_col="away"):
    """
    Recebe DF com colunas: [match_id?, home, away, odds_home, odds_draw, odds_away]
    Retorna dicionário indexado por (norm_tokens(home)|norm_tokens(away)) -> lista de odds (para mediana).
    """
    idx = {}
    if df.empty:
        return idx
    # renomear possíveis variantes
    rename_map = {}
    if "team_home" in df.columns: rename_map["team_home"] = "home"
    if "team_away" in df.columns: rename_map["team_away"] = "away"
    df = df.rename(columns=rename_map)

    for _, r in df.iterrows():
        h = str(r.get("home", "") or "")
        a = str(r.get("away", "") or "")
        if not h or not a:
            continue
        k = f"{norm_key_tokens(h)}|{norm_key_tokens(a)}"
        oh = secure_float(r.get("odds_home"))
        od = secure_float(r.get("odds_draw"))
        oa = secure_float(r.get("odds_away"))
        if any(v is None for v in (oh, od, oa)):
            continue
        idx.setdefault(k, []).append((oh, od, oa))
    return idx

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--strict", action="store_true", help="Falha se algum jogo ficar sem odds")
    ap.add_argument("--aliases", default=os.getenv("ALIASES_JSON", "data/aliases.json"))
    return ap.parse_args()

def load_whitelist(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        log("CRITICAL", f"Whitelist não encontrada: {path}")
        sys.exit(6)
    df = pd.read_csv(path)
    # manter colunas esperadas
    df = df.rename(columns={"home": "team_home", "away": "team_away"})
    return df[["match_id", "team_home", "team_away"]].copy()

def main():
    args = parse_args()
    rodada = args.rodada
    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    log("INFO", "==================================================")
    log("INFO", f"STRICT MODE — RODADA: {rodada}" if args.strict else f"RODADA: {rodada}")
    log("INFO", "==================================================")

    wl = load_whitelist(wl_path)
    log("INFO", f"whitelist: {wl_path}  linhas={len(wl)}  mapping={{'match_id':'match_id','home':'home','away':'away'}}")

    # ler fontes
    p_theodds = os.path.join(rodada, "odds_theoddsapi.csv")
    p_apifoot = os.path.join(rodada, "odds_apifootball.csv")
    df_theodds = read_csv_safe(p_theodds)
    df_apifoot = read_csv_safe(p_apifoot)

    log("INFO", f"fontes: theodds={0 if df_theodds is None else len(df_theodds)}  apifoot={0 if df_apifoot is None else len(df_apifoot)}")

    idx_the = build_index(df_theodds)
    idx_api = build_index(df_apifoot)

    out_rows = []
    missing = []

    for _, r in wl.iterrows():
        mid = r["match_id"]
        th = str(r["team_home"])
        ta = str(r["team_away"])
        key = f"{norm_key_tokens(th)}|{norm_key_tokens(ta)}"

        cands = []
        if key in idx_the:
            cands.extend(idx_the[key])
        if key in idx_api:
            cands.extend(idx_api[key])

        if not cands:
            missing.append((mid, th, ta))
            continue

        oh = median_ignore_none([x[0] for x in cands])
        od = median_ignore_none([x[1] for x in cands])
        oa = median_ignore_none([x[2] for x in cands])

        if oh is None or od is None or oa is None:
            missing.append((mid, th, ta))
            continue

        out_rows.append({
            "team_home": th,
            "team_away": ta,
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
        })

    out_path = os.path.join(rodada, "odds_consensus.csv")
    pd.DataFrame(out_rows, columns=REQ_COLS).to_csv(out_path, index=False)

    if args.strict and missing:
        print("##[error][CRITICAL] Jogos sem odds após consenso (modo estrito ligado).")
        print("match_id   team_home team_away")
        for mid, th, ta in missing:
            print(f"{mid:>7} {th}   {ta}")
        sys.exit(6)

    if not out_rows:
        print("##[error][CRITICAL] Consenso vazio (nenhuma linha gerada).")
        sys.exit(6)

    log("INFO", f"consenso gerado: {out_path}  linhas={len(out_rows)}  missing={len(missing)}")
    if missing:
        log("WARN", f"Jogos sem odds: {len(missing)} -> {[m[0] for m in missing]}")
    return 0

if __name__ == "__main__":
    sys.exit(main())