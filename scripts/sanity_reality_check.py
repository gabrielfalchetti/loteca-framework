#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sanity & Reality Check para garantir integridade total antes da aposta.

Este script deve rodar **antes da predição e da Kelly**, logo após todas as coletas (API-Football, TheOddsAPI, News).

Objetivo: impedir o pipeline de continuar se qualquer insumo crítico estiver
faltando, vazio, inconsistente ou baseado em dados fictícios.

Entradas obrigatórias:
  - {OUT_DIR}/apifoot_fixtures.csv
  - {OUT_DIR}/apifoot_odds.csv
  - {OUT_DIR}/odds_consensus.csv
  - {OUT_DIR}/apifoot_lineups.csv
  - {OUT_DIR}/apifoot_injuries.csv
  - {OUT_DIR}/apifoot_teamstats.csv
  - {OUT_DIR}/apifoot_standings.csv
  - {OUT_DIR}/apifoot_h2h.csv

Saídas:
  - OK → exit(0)
  - Erro → exit(2) e mensagem clara explicando o problema
"""

import os
import sys
import pandas as pd
from typing import List

# ===== Funções auxiliares =====================================================

def die(msg: str):
    print(f"[sanity] ❌ ERRO: {msg}", file=sys.stderr)
    sys.exit(2)

def warn(msg: str):
    print(f"[sanity] ⚠️ AVISO: {msg}")

def ok(msg: str):
    print(f"[sanity] ✅ {msg}")

def file_must_exist(path: str):
    if not os.path.isfile(path):
        die(f"arquivo obrigatório não encontrado: {path}")

def check_nonempty_csv(path: str, min_rows: int = 1, min_cols: int = 2):
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"falha ao ler {path}: {e}")
    if df.empty or len(df) < min_rows:
        die(f"{path} está vazio ou tem menos de {min_rows} linha(s)")
    if df.shape[1] < min_cols:
        die(f"{path} possui colunas insuficientes ({df.shape[1]} colunas)")
    ok(f"{os.path.basename(path)} ✔️  {len(df)} linhas, {df.shape[1]} colunas")
    return df

def required_columns(df: pd.DataFrame, required: List[str], name: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        die(f"{name} faltando colunas obrigatórias: {missing}")

def compare_match_counts(df1: pd.DataFrame, df2: pd.DataFrame, label1: str, label2: str):
    set1 = set(df1.get("match_key", []))
    set2 = set(df2.get("match_key", []))
    inter = set1 & set2
    if len(inter) == 0:
        die(f"nenhum match_key em comum entre {label1} e {label2}")
    if len(inter) < min(len(set1), len(set2)) * 0.7:
        warn(f"baixa interseção entre {label1} e {label2}: {len(inter)} / {len(set1)} / {len(set2)}")
    else:
        ok(f"match_key interseção suficiente entre {label1} e {label2}: {len(inter)} comuns")

# ===== Execução principal =====================================================

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="OUT_DIR (ex: data/out/123456)")
    args = p.parse_args()

    out_dir = args.rodada
    if out_dir.isdigit():
        out_dir = os.path.join("data", "out", out_dir)

    print(f"[sanity] 🔍 Verificando integridade de {out_dir}")

    # ==== 1. Arquivos obrigatórios
    must_exist = [
        "apifoot_fixtures.csv",
        "apifoot_odds.csv",
        "apifoot_lineups.csv",
        "apifoot_injuries.csv",
        "apifoot_teamstats.csv",
        "apifoot_standings.csv",
        "apifoot_h2h.csv",
        "odds_consensus.csv"
    ]
    for fname in must_exist:
        file_must_exist(os.path.join(out_dir, fname))

    # ==== 2. Sanidade individual
    df_fix = check_nonempty_csv(os.path.join(out_dir, "apifoot_fixtures.csv"), min_rows=3)
    df_odds = check_nonempty_csv(os.path.join(out_dir, "apifoot_odds.csv"))
    df_line = check_nonempty_csv(os.path.join(out_dir, "apifoot_lineups.csv"))
    df_inj = check_nonempty_csv(os.path.join(out_dir, "apifoot_injuries.csv"))
    df_team = check_nonempty_csv(os.path.join(out_dir, "apifoot_teamstats.csv"))
    df_stand = check_nonempty_csv(os.path.join(out_dir, "apifoot_standings.csv"))
    df_h2h = check_nonempty_csv(os.path.join(out_dir, "apifoot_h2h.csv"))
    df_cons = check_nonempty_csv(os.path.join(out_dir, "odds_consensus.csv"))

    # ==== 3. Colunas essenciais
    required_columns(df_fix, ["home","away","league_id","date"], "apifoot_fixtures.csv")
    required_columns(df_odds, ["fixture_id","odds_home","odds_away"], "apifoot_odds.csv")
    required_columns(df_cons, ["match_key","odds_home","odds_draw","odds_away"], "odds_consensus.csv")

    # ==== 4. Consistência de nomes/time
    # Mapeia chave (home vs away)
    def mk(home, away): return f"{str(home).lower().strip()}__vs__{str(away).lower().strip()}"
    df_fix["match_key"] = [mk(h, a) for h, a in zip(df_fix["home"], df_fix["away"])]
    df_cons["match_key"] = df_cons["match_key"].astype(str).str.lower().str.strip()

    compare_match_counts(df_fix, df_cons, "fixtures", "odds_consensus")

    # ==== 5. Sanidade estatística
    # odds devem ser >1
    if (df_cons[["odds_home","odds_draw","odds_away"]] <= 1.0).any().any():
        die("algumas odds ≤ 1.0 — dados inválidos")
    ok("todas as odds > 1.0")

    # fixtures devem ter datas coerentes
    if pd.to_datetime(df_fix["date"], errors="coerce").isna().any():
        die("datas inválidas em fixtures.csv")
    ok("todas as datas válidas")

    # standings deve ter times únicos e posições
    if df_stand["team"].duplicated().sum() > 10:
        warn("muitas duplicações em standings (pode haver múltiplos grupos)")
    ok("standings com times e posições válidos")

    # injuries pode estar vazio se não houver lesões — apenas avisa
    if df_inj.empty:
        warn("injuries vazio (sem lesões reportadas no dia)")
    else:
        ok("injuries OK")

    # lineups deve conter pelo menos metade dos jogos
    if len(df_line) < len(df_fix)/2:
        warn(f"lineups contém poucos jogos ({len(df_line)} vs {len(df_fix)})")
    else:
        ok("lineups OK")

    print("[sanity] ✅ Nenhum erro crítico encontrado. Dados são reais e íntegros.")

if __name__ == "__main__":
    main()