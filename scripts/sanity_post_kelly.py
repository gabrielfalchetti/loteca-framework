#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sanity pós-Kelly: garante a consistência dos artefatos finais
antes da publicação/uso do cartão da Loteca.

Verificações principais:
  1) Presença e integridade de:
     - {OUT_DIR}/odds_consensus.csv
     - {OUT_DIR}/predictions_market.csv  (ou variantes xg_*, stacked, etc — opcional)
     - {OUT_DIR}/kelly_stakes.csv
     - {OUT_DIR}/loteca_cartao.txt
  2) Número de jogos coerente (default 14) comparando com fixtures e/ou matches_source.
  3) Colunas obrigatórias e valores válidos (odds > 1, probabilidades entre 0..1, stakes >= 0).
  4) Somatório de stakes <= BANKROLL (ambiente).
  5) Cada jogo presente em consensus deve estar representado no cartão.
  6) Cartão com número de linhas igual ao total de jogos elegíveis.
  7) Escreve relatório em {OUT_DIR}/post_sanity_report.txt

Exit codes:
  - 0: OK
  - 2: erro crítico (pipeline deve parar)
"""

import os
import sys
import glob
import pandas as pd
from typing import List, Optional

# ========= Helpers ============================================================

def die(msg: str):
    print(f"[post-sanity] ❌ ERRO: {msg}", file=sys.stderr)
    sys.exit(2)

def warn(msg: str):
    print(f"[post-sanity] ⚠️ AVISO: {msg}")

def ok(msg: str):
    print(f"[post-sanity] ✅ {msg}")

def file_must_exist(path: str):
    if not os.path.isfile(path):
        die(f"arquivo obrigatório não encontrado: {path}")

def read_csv_safe(path: str, min_rows: int = 1, min_cols: int = 2) -> pd.DataFrame:
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

def has_columns(df: pd.DataFrame, required: List[str], label: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        die(f"{label} faltando colunas obrigatórias: {missing}")

def norm_key(s: str) -> str:
    return str(s).lower().strip()

def guess_expected_games(out_dir: str, fallback: int) -> int:
    """
    Tenta descobrir a quantidade esperada de jogos:
      - OUT_DIR/apifoot_fixtures.csv (se existir)
      - data/in/matches_source.csv (estrutura simplificada)
      - fallback -> 14
    """
    fx = os.path.join(out_dir, "apifoot_fixtures.csv")
    if os.path.isfile(fx):
        try:
            df = pd.read_csv(fx)
            if not df.empty and {"home","away"}.issubset(df.columns):
                n = len(df)
                ok(f"jogos esperados pela fixtures: {n}")
                return n
        except Exception as e:
            warn(f"não consegui inferir por fixtures: {e}")

    ms = os.path.join("data", "in", "matches_source.csv")
    if os.path.isfile(ms):
        try:
            df = pd.read_csv(ms)
            if not df.empty and {"home","away"}.issubset(df.columns):
                n = len(df)
                ok(f"jogos esperados pelo matches_source: {n}")
                return n
        except Exception as e:
            warn(f"não consegui inferir por matches_source: {e}")

    warn(f"não foi possível inferir nº de jogos; usando fallback={fallback}")
    return fallback

def read_predictions_any(out_dir: str) -> Optional[pd.DataFrame]:
    """
    Aceita qualquer um dos arquivos de predição, se existir:
      predictions_market.csv
      predictions_xg_uni.csv
      predictions_xg_bi.csv
      predictions_calibrated.csv
      predictions_stacked.csv
    Se nenhum existir, retorna None (pois Kelly pode ter caído para odds de mercado).
    """
    candidates = [
        "predictions_market.csv",
        "predictions_xg_uni.csv",
        "predictions_xg_bi.csv",
        "predictions_calibrated.csv",
        "predictions_stacked.csv",
    ]
    for c in candidates:
        p = os.path.join(out_dir, c)
        if os.path.isfile(p):
            df = read_csv_safe(p)
            df["match_key"] = df["match_key"].astype(str).str.lower().str.strip()
            ok(f"usando {c} como fonte de predição")
            return df
    warn("nenhum arquivo de predição encontrado — assumindo Kelly baseado em odds de mercado.")
    return None

def read_card_lines(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [ln.rstrip("\n") for ln in f.readlines()]
    except Exception as e:
        die(f"falha ao ler {path}: {e}")
    # remove linhas em branco
    lines = [ln for ln in lines if ln.strip() != ""]
    return lines

# ========= Execução ===========================================================

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="OUT_DIR (ex: data/out/123456 ou apenas 123456)")
    p.add_argument("--expected-games", type=int, default=14, help="nº esperado de jogos (fallback)")
    args = p.parse_args()

    out_dir = args.rodada
    if out_dir.isdigit():
        out_dir = os.path.join("data", "out", out_dir)

    print(f"[post-sanity] 🔎 Validando artefatos em {out_dir}")

    # 1) Arquivos obrigatórios
    consensus_path = os.path.join(out_dir, "odds_consensus.csv")
    kelly_path     = os.path.join(out_dir, "kelly_stakes.csv")
    card_path      = os.path.join(out_dir, "loteca_cartao.txt")

    file_must_exist(consensus_path)
    file_must_exist(kelly_path)
    file_must_exist(card_path)

    df_cons = read_csv_safe(consensus_path)
    has_columns(df_cons, ["match_key","odds_home","odds_draw","odds_away"], "odds_consensus.csv")
    df_cons["match_key"] = df_cons["match_key"].astype(str).str.lower().str.strip()

    # odds válidas
    if (df_cons[["odds_home","odds_draw","odds_away"]] <= 1.0).any().any():
        die("há odds ≤ 1.0 em odds_consensus.csv")

    # 2) Predições (opcionais)
    df_pred = read_predictions_any(out_dir)
    if df_pred is not None:
        has_columns(df_pred, ["match_key","prob_home","prob_draw","prob_away"], "predictions_*")
        for c in ["prob_home","prob_draw","prob_away"]:
            if ((df_pred[c] < 0) | (df_pred[c] > 1)).any():
                die(f"probabilidades fora de [0,1] em {c}")
        # alinhamento com consensus
        inter = set(df_pred["match_key"]) & set(df_cons["match_key"])
        if len(inter) == 0:
            die("nenhuma interseção entre predictions_* e odds_consensus")
        ok(f"predições alinhadas com consensus: {len(inter)} jogos")

    # 3) Kelly
    df_kelly = read_csv_safe(kelly_path)
    has_columns(df_kelly, ["match_key","stake"], "kelly_stakes.csv")
    df_kelly["match_key"] = df_kelly["match_key"].astype(str).str.lower().str.strip()

    if (df_kelly["stake"] < 0).any():
        die("stake negativa detectada em kelly_stakes.csv")

    bankroll = os.environ.get("BANKROLL", "1000")
    try:
        bankroll = float(bankroll)
    except Exception:
        die(f"BANKROLL inválido no ambiente: {bankroll}")

    total_stake = float(df_kelly["stake"].sum())
    if total_stake - bankroll > 1e-6:
        die(f"somatório de stakes ({total_stake:.2f}) excede BANKROLL ({bankroll:.2f})")
    ok(f"somatório de stakes OK: {total_stake:.2f} ≤ bankroll {bankroll:.2f}")

    # 4) Coerência de jogos
    # Tenta inferir nº esperado
    expected_games = guess_expected_games(out_dir, args.expected_games)

    # Jogos “válidos” = aqueles com odds em consensus
    games_cons = sorted(set(df_cons["match_key"]))
    n_cons = len(games_cons)
    if n_cons == 0:
        die("nenhum jogo em odds_consensus.csv")
    ok(f"jogos no consensus: {n_cons}")

    # 5) Cartão
    card_lines = read_card_lines(card_path)

    # Heurística: número de linhas deve ser igual ao nº de jogos esperados
    # (caso a Loteca do dia tenha menos, o sanity de fixtures já teria sinalizado)
    if len(card_lines) != expected_games:
        die(f"cartão tem {len(card_lines)} linha(s), mas o esperado é {expected_games}")

    ok(f"cartão com {len(card_lines)} linha(s) — OK")

    # 6) Cobertura: todo jogo do consensus deve estar representado no cartão
    # Não sabemos o formato exato do cartão (1)/X/2 ou HOME/DRAW/AWAY),
    # então checamos que o nome dos times (home x away) aparece na linha.
    # Para isso, tentamos reconstruir chaves a partir de consensus (se tiver colunas de times)
    covered = 0
    if {"team_home","team_away"}.issubset(df_cons.columns):
        ref_keys = []
        for _, r in df_cons.iterrows():
            th = norm_key(r["team_home"]) if not pd.isna(r.get("team_home")) else ""
            ta = norm_key(r["team_away"]) if not pd.isna(r.get("team_away")) else ""
            ref_keys.append((th, ta))
        for th, ta in ref_keys:
            found = any((th in norm_key(ln) and ta in norm_key(ln)) for ln in card_lines)
            if found:
                covered += 1
        if covered < min(expected_games, n_cons):
            warn(f"nem todos os jogos do consensus parecem estar descritos textualmente no cartão ({covered}/{min(expected_games, n_cons)})")
        else:
            ok("todas as partidas do consensus parecem contempladas no cartão")
    else:
        warn("team_home/team_away ausentes no consensus — cobertura textual do cartão não pôde ser verificada")

    # 7) Relatório
    report_path = os.path.join(out_dir, "post_sanity_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== POST SANITY REPORT ===\n")
        f.write(f"OUT_DIR = {out_dir}\n")
        f.write(f"expected_games = {expected_games}\n")
        f.write(f"consensus_games = {n_cons}\n")
        f.write(f"card_lines = {len(card_lines)}\n")
        f.write(f"bankroll = {bankroll:.2f}\n")
        f.write(f"total_stake = {total_stake:.2f}\n")
        f.write(f"predictions_used = {df_pred is not None}\n")
        f.write(f"covered_games_by_card_heuristic = {covered}\n")
    ok(f"relatório escrito em {report_path}")

    print("[post-sanity] ✅ Nenhum erro crítico encontrado. Artefatos finais coerentes.")

if __name__ == "__main__":
    main()