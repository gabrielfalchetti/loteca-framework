#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds de consenso a partir dos provedores disponíveis.

✔ Funciona com UM provedor só.
✔ Detecta automaticamente nomes de colunas de odds (home/draw/away), mesmo que mudem:
   - "odds_home", "home_odds", "h2h_home", "moneyline_home", "home", etc.
✔ Converte formatos:
   - Probabilidades (0<p<1, soma ≈1) -> odds decimais (1/p)
   - American (+120/-150) -> odds decimais
   - Vírgula decimal e '%' -> normaliza
✔ Diagnóstico: conta os motivos de invalidação por linha.

Entrada (se existirem):
  data/out/<RODADA>/odds_theoddsapi.csv
  data/out/<RODADA>/odds_apifootball.csv

Saída:
  data/out/<RODADA>/odds_consensus.csv

Uma linha é considerada válida se tiver ≥2 odds numéricas > 1.0.
"""

import argparse
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

import pandas as pd


REQUIRED_BASE = ["match_key", "team_home", "team_away"]
PREFS_HOME = ["odds_home", "home_odds", "h2h_home", "moneyline_home", "home_ml", "home"]
PREFS_DRAW = ["odds_draw", "draw_odds", "h2h_draw", "moneyline_draw", "x", "empate", "draw"]
PREFS_AWAY = ["odds_away", "away_odds", "h2h_away", "moneyline_away", "away_ml", "away"]

def log(msg: str) -> None:
    print(f"[consensus-safe] {msg}")

def exists(p: str) -> bool:
    return os.path.isfile(p) and os.path.getsize(p) > 0

def cleanse_num_series(s: pd.Series) -> pd.Series:
    # troca vírgula por ponto; remove %
    s = s.astype(str).str.replace(",", ".", regex=False).str.replace("%", "", regex=False)
    return pd.to_numeric(s, errors="coerce")

def detect_col(cols: List[str], prefs: List[str]) -> Optional[str]:
    cols_l = [c.lower().strip() for c in cols]
    # preferidos exatos
    for p in prefs:
        if p in cols_l:
            return cols[cols_l.index(p)]
    # heurística por regex
    regexes = [
        r"\bhome\b|\bcasa\b",
        r"\bdraw\b|\bx\b|\bempate\b",
        r"\baway\b|\bfora\b|\bvisitor\b",
    ]
    # usa o conjunto pedido
    targets = "|".join(re.escape(p) for p in prefs)
    pat = re.compile(targets, re.I)
    for c in cols:
        if pat.search(c):
            return c
    # fallback: heurística curta
    for c in cols:
        cl = c.lower()
        if prefs is PREFS_HOME and ("home" in cl or "casa" in cl or cl.endswith("_h")):
            return c
        if prefs is PREFS_DRAW and ("draw" in cl or "empate" in cl or cl in ("x",)):
            return c
        if prefs is PREFS_AWAY and ("away" in cl or "fora" in cl or cl.endswith("_a")):
            return c
    return None

def map_columns(df: pd.DataFrame) -> Tuple[str, str, str, str, str, str]:
    cols = [c.strip() for c in df.columns]
    # obrigatórias base (permite variações simples)
    def pick_base(name: str, alt: List[str]) -> str:
        candidates = [name] + alt
        for c in candidates:
            if c in df.columns:
                return c
        # heurística: match_key pode faltar — criaremos depois
        return ""
    c_match = pick_base("match_key", ["match id", "matchid", "key", "id"])
    c_home = pick_base("team_home", ["home_team", "time_casa", "mandante", "home"])
    c_away = pick_base("team_away", ["away_team", "time_fora", "visitante", "away"])
    # odds
    oh = detect_col(cols, PREFS_HOME)
    od = detect_col(cols, PREFS_DRAW)
    oa = detect_col(cols, PREFS_AWAY)
    return c_match, c_home, c_away, oh or "", od or "", oa or ""

def mk_join_key(row, c_match: str, c_home: str, c_away: str) -> str:
    def g(c):
        return "" if not c else str(row.get(c, "")).strip().lower()
    mk = g(c_match)
    if mk:
        return mk
    h = g(c_home)
    a = g(c_away)
    return f"{h}__vs__{a}"

def american_to_decimal(v: float) -> float:
    if pd.isna(v):
        return pd.NA
    if v >= 100:
        return 1.0 + (v / 100.0)
    if v <= -100:
        return 1.0 + (100.0 / abs(v))
    return v  # não parece american; devolve como veio

def normalize_row(row: pd.Series, oh: str, od: str, oa: str) -> pd.Series:
    # forçar numérico com limpeza
    for c in (oh, od, oa):
        if c:
            row[c] = cleanse_num_series(pd.Series([row[c]])).iloc[0]

    vals = [row.get(oh), row.get(od), row.get(oa)]
    nums = [v for v in vals if pd.notna(v)]

    # Probabilidades?
    if len(nums) >= 2 and all(0 < v < 1 for v in nums):
        s = sum(nums)
        if 0.90 <= s <= 1.10:  # relaxado
            for c in (oh, od, oa):
                v = row.get(c)
                row[c] = (1.0 / v) if (c and pd.notna(v) and v > 0) else pd.NA
            return row

    # American?
    if len(nums) >= 2 and sum(1 for v in nums if abs(v) >= 100) >= 2:
        for c in (oh, od, oa):
            v = row.get(c)
            row[c] = american_to_decimal(v) if (c and pd.notna(v)) else pd.NA
        return row

    # Já decimal ou incerto
    return row

def read_provider(path: str, provider: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    c_match, c_home, c_away, oh, od, oa = map_columns(df)

    # criar join_key
    df["__join_key"] = df.apply(lambda r: mk_join_key(r, c_match, c_home, c_away), axis=1)
    # copiar base visível
    out = pd.DataFrame({
        "match_key": df[c_match] if c_match else df["__join_key"],
        "team_home": df[c_home] if c_home else "",
        "team_away": df[c_away] if c_away else "",
    })
    out["__join_key"] = df["__join_key"]
    out["__prov"] = provider

    # odds brutas
    for nick, col in (("odds_home", oh), ("odds_draw", od), ("odds_away", oa)):
        out[nick] = cleanse_num_series(df[col]) if col else pd.NA

    # normalização por linha
    tmp = out.copy()
    tmp = tmp.rename(columns={"odds_home": "oh", "odds_draw": "od", "odds_away": "oa"})
    tmp[["oh", "od", "oa"]] = df.apply(lambda r: normalize_row(
        r.rename(index=str), oh, od, oa)[[oh, od, oa]].rename(index={oh:"oh", od:"od", oa:"oa"}) if True else r, axis=1)
    # acima pode gerar NaNs se col não existir; então preenche a partir de out quando vazio
    for a, b in (("odds_home", "oh"), ("odds_draw", "od"), ("odds_away", "oa")):
        out[a] = pd.to_numeric(tmp[b], errors="coerce").fillna(out[a])

    # marcar validade e motivo
    def validity_reason(r) -> Tuple[bool, str]:
        vals = [r["odds_home"], r["odds_draw"], r["odds_away"]]
        cnt_gt1 = sum(1 for v in vals if pd.notna(v) and v > 1.0)
        if cnt_gt1 >= 2:
            return True, ""
        if all(pd.isna(v) for v in vals):
            return False, "sem_odds"
        if any(pd.notna(v) and v <= 0 for v in vals):
            return False, "odds_nao_positivas"
        if sum(1 for v in vals if pd.notna(v)) < 2:
            return False, "menos_de_duas_odds"
        return False, "odds_<=1"
    val = out.apply(validity_reason, axis=1, result_type="expand")
    out["__valid"] = val[0]
    out["__reason"] = val[1]

    return out

def consensus(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    base = pd.concat(dfs, ignore_index=True)

    # escolher meta por join_key (primeiro provedor vence p/ nomes)
    meta = (base.sort_values(["__join_key", "__prov"])
                 .drop_duplicates("__join_key", keep="first")
                 [["__join_key", "match_key", "team_home", "team_away"]])

    agg = base.groupby("__join_key")[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True).reset_index()
    out = meta.merge(agg, on="__join_key", how="left")

    def is_valid(r):
        vals = [r["odds_home"], r["odds_draw"], r["odds_away"]]
        return sum(1 for v in vals if pd.notna(v) and v > 1.0) >= 2

    out["__valid"] = out.apply(is_valid, axis=1)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    paths = [
        ("theoddsapi", os.path.join(out_dir, "odds_theoddsapi.csv")),
        ("apifootball", os.path.join(out_dir, "odds_apifootball.csv")),
    ]

    dfs: List[pd.DataFrame] = []
    any_found = False
    for prov, p in paths:
        if exists(p):
            any_found = True
            try:
                df = read_provider(p, prov)
                valid = int(df["__valid"].sum())
                log(f"lido {os.path.basename(p)} -> {len(df)} linhas; válidas: {valid}")
                # diagnóstico resumido
                if valid < len(df):
                    diag = df.loc[~df["__valid"], "__reason"].value_counts().to_dict()
                    if diag:
                        log(f"motivos inválidos {prov}: {diag}")
                dfs.append(df)
            except Exception as e:
                log(f"AVISO: erro lendo {p}: {e}")

    if not any_found:
        log("AVISO: nenhum CSV de odds encontrado em data/out/<RODADA>.")
        log("consenso bruto: 0 linhas; válidas (>=2 odds > 1.0): 0")
        log("ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    cons = consensus(dfs)
    total = len(cons)
    valid = int(cons["__valid"].sum())
    log(f"consenso bruto: {total} linhas; válidas (>=2 odds > 1.0): {valid}")

    if valid == 0:
        # imprime amostra para facilitar debug no log
        sample = cons.head(5).to_dict(orient="records")
        log(f"AMOSTRA (top 5): {sample}")
        log("ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    cons = cons.loc[cons["__valid"], ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]]
    out_path = os.path.join(out_dir, "odds_consensus.csv")
    cons.to_csv(out_path, index=False)
    log(f"OK -> {out_path} ({len(cons)} linhas)")

if __name__ == "__main__":
    main()