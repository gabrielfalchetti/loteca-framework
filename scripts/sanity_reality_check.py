#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sanity check geral para garantir que estamos trabalhando com JOGOS e ODDS reais.
- Verifica arquivo de entrada matches_source.csv (14 jogos, colunas, datas)
- Confere se há odds válidas (>= 2 preços > 1.0) por jogo em pelo menos UMA fonte
- Confere se odds_consensus.csv existe (ou ao menos odds de uma fonte)
- Gera relatório JSON e falha (exit 2) se houver erro crítico

Saídas:
  data/out/<rodada>/reality_report.json
  data/out/<rodada>/reality_report.txt (resumo legível)

Uso:
  python scripts/sanity_reality_check.py --rodada 2025-09-27_1213 --strict
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import numpy as np

REQUIRED_MATCH_COLS = ["match_key", "team_home", "team_away", "league", "match_date_iso"]
ODDS_COLS = ["match_key", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]

def now_utc():
    return datetime.now(timezone.utc)

def load_csv(path):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        return f"ERROR:{e}"

def is_valid_odds_row(row) -> bool:
    vals = [row.get("odds_home"), row.get("odds_draw"), row.get("odds_away")]
    cnt = 0
    for v in vals:
        try:
            if float(v) > 1.0:
                cnt += 1
        except:
            pass
    return cnt >= 2

def ensure_odds_columns(df: pd.DataFrame) -> pd.DataFrame:
    # tenta mapear algumas variações comuns
    mapping_try = [
        {"match_key":"match_key","team_home":"team_home","team_away":"team_away","odds_home":"odds_home","odds_draw":"odds_draw","odds_away":"odds_away"},
        {"match_key":"__join_key","team_home":"team_home","team_away":"team_away","odds_home":"odds_home","odds_draw":"odds_draw","odds_away":"odds_away"},
        {"match_key":"match_key","team_home":"home_team","team_away":"away_team","odds_home":"home_odds","odds_draw":"draw_odds","odds_away":"away_odds"},
    ]
    for mp in mapping_try:
        if set(mp.values()).issubset(df.columns):
            out = df[list(mp.values())].copy()
            out.columns = list(mp.keys())
            return out
    return df  # deixa como está; o validador vai apontar ausência

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--strict", action="store_true", help="Falhar caso encontre qualquer jogo sem odds válidas")
    args = ap.parse_args()

    rodada = args.rodada
    in_dir = os.path.join("data", "in", rodada)
    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)

    report = {"rodada": rodada, "ts": now_utc().isoformat(), "checks": [], "summary": {}}
    errors = []
    warnings = []

    # 1) matches_source.csv
    matches_path = os.path.join(in_dir, "matches_source.csv")
    matches_df = load_csv(matches_path)
    if matches_df is None:
        errors.append(f"Arquivo ausente: {matches_path}")
    elif isinstance(matches_df, str) and matches_df.startswith("ERROR:"):
        errors.append(f"Falha ao ler {matches_path}: {matches_df}")
    else:
        missing = [c for c in REQUIRED_MATCH_COLS if c not in matches_df.columns]
        if missing:
            errors.append(f"Colunas ausentes em matches_source.csv: {missing}")
        if len(matches_df) != 14:
            warnings.append(f"Esperado 14 jogos; encontrado {len(matches_df)}")
        # datas
        bad_dates = []
        for i, r in matches_df.iterrows():
            try:
                dt = datetime.fromisoformat(str(r["match_date_iso"]).replace("Z","+00:00"))
                if dt.tzinfo is None:
                    warnings.append(f"match_date_iso sem timezone (linha {i}): {r['match_date_iso']}")
            except Exception:
                bad_dates.append((i, r.get("match_date_iso")))
        if bad_dates:
            errors.append(f"Datas inválidas em matches_source.csv: {bad_dates}")

    # 2) odds (consenso e fontes)
    odds_consensus = os.path.join(out_dir, "odds_consensus.csv")
    odds_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    odds_apifoot = os.path.join(out_dir, "odds_apifootball.csv")

    df_consensus = load_csv(odds_consensus)
    df_theodds = load_csv(odds_theodds)
    df_apifoot = load_csv(odds_apifoot)

    sources = []
    for name, df in [("consensus", df_consensus), ("theoddsapi", df_theodds), ("apifootball", df_apifoot)]:
        if df is None:
            sources.append({"name": name, "exists": False, "rows": 0, "valid_rows": 0})
            continue
        if isinstance(df, str) and df.startswith("ERROR:"):
            warnings.append(f"Falha ao ler odds ({name}): {df}")
            sources.append({"name": name, "exists": True, "rows": 0, "valid_rows": 0, "error": df})
            continue
        df = ensure_odds_columns(df)
        valid = 0
        if set(ODDS_COLS).issubset(df.columns):
            valid = int(df.apply(is_valid_odds_row, axis=1).sum())
        else:
            warnings.append(f"Colunas ausentes em odds ({name}); esperadas: {ODDS_COLS}")
        sources.append({"name": name, "exists": True, "rows": len(df), "valid_rows": valid})

    report["checks"].append({"matches_source": matches_path, "ok": len(errors)==0})
    report["checks"].append({"odds_sources": sources})

    # 3) cruza: cada jogo do matches_source tem pelo menos UMA linha com odds válidas?
    no_odds = []
    coverage = 0
    if isinstance(matches_df, pd.DataFrame):
        for _, m in matches_df.iterrows():
            mk = str(m["match_key"])
            has_valid = False
            for name, p in [("consensus", odds_consensus), ("theoddsapi", odds_theodds), ("apifootball", odds_apifoot)]:
                df = load_csv(p)
                if isinstance(df, pd.DataFrame):
                    df = ensure_odds_columns(df)
                    if set(ODDS_COLS).issubset(df.columns):
                        hit = df[(df["match_key"] == mk) & (df.apply(is_valid_odds_row, axis=1))]
                        if len(hit) > 0:
                            has_valid = True
                            break
            if has_valid:
                coverage += 1
            else:
                no_odds.append(mk)

    report["summary"]["games_in_card"] = int(len(matches_df)) if isinstance(matches_df, pd.DataFrame) else 0
    report["summary"]["games_with_valid_odds"] = coverage
    report["summary"]["games_without_valid_odds"] = no_odds
    report["summary"]["errors"] = errors
    report["summary"]["warnings"] = warnings

    # salva relatório
    json_path = os.path.join(out_dir, "reality_report.json")
    txt_path = os.path.join(out_dir, "reality_report.txt")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"RODADA: {rodada}\n")
        f.write(f"OK matches_source: {'SIM' if len(errors)==0 else 'NÃO'}\n")
        f.write(f"Jogos no cartão: {report['summary']['games_in_card']}\n")
        f.write(f"Cobertura de odds válidas: {coverage}/{report['summary']['games_in_card']}\n")
        if warnings:
            f.write("WARNINGS:\n")
            for w in warnings:
                f.write(f"  - {w}\n")
        if errors:
            f.write("ERRORS:\n")
            for e in errors:
                f.write(f"  - {e}\n")
        if no_odds:
            f.write("SEM ODDS VÁLIDAS PARA:\n")
            for mk in no_odds:
                f.write(f"  - {mk}\n")

    # Política de falha:
    # - Sempre falha se houver errors de estrutura
    # - Se --strict, falha se cobertura < total de jogos (queremos odds para todos)
    exit_code = 0
    if errors:
        exit_code = 2
    elif args.strict and (coverage < report["summary"]["games_in_card"]):
        exit_code = 3

    print(f"[reality] OK -> {json_path}")
    print(f"[reality] RESUMO -> {txt_path}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()