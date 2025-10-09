#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/consensus_odds_safe.py

Gera odds de consenso (abertura) a partir das fontes disponíveis
no diretório da rodada, com política "APIs obrigatórias" por padrão.

Saídas:
  <OUT_DIR>/odds_consensus.csv  com colunas:
    match_id,team_home,team_away,odds_home,odds_draw,odds_away,sources_count

Regras:
- Sem dados fictícios.
- STRICT (padrão): todo jogo da whitelist precisa ter odds em pelo menos 1 fonte.
- Em falha, retornos com exit codes diferentes de zero (workflow interrompe).
"""

from __future__ import annotations
import os
import sys
import argparse
import pandas as pd
from typing import Dict, Tuple, List

# ------------------------ utilidades ------------------------

WL_CANDIDATES = ("matches_whitelist.csv", "matches_source.csv", "matches.csv")

CANON = {
    "match_id": {"match_id", "id", "game_id", "jogo_id"},
    "home": {"home", "mandante", "home_team", "team_home", "casa"},
    "away": {"away", "visitante", "away_team", "team_away", "fora"},
}

def _normalize_whitelist_cols(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    cols = {c.lower().strip(): c for c in df.columns}
    inv: Dict[str, str] = {}
    for canon, variants in CANON.items():
        for v in variants:
            if v in cols:
                inv[canon] = cols[v]
                break
    # renomeia se achou; mantém originais se já canônicas
    mapping = {}
    if "match_id" in inv: mapping[inv["match_id"]] = "match_id"
    if "home" in inv: mapping[inv["home"]] = "home"
    if "away" in inv: mapping[inv["away"]] = "away"
    df2 = df.rename(columns=mapping)
    return df2, mapping

def _load_whitelist(rodada_dir: str) -> Tuple[pd.DataFrame, str]:
    for fname in WL_CANDIDATES:
        path = os.path.join(rodada_dir, fname)
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
            except Exception as e:
                print(f"::error::Falha ao ler {path}: {e}", file=sys.stderr)
                sys.exit(6)
            if df.shape[0] == 0:
                continue
            df, mapping = _normalize_whitelist_cols(df)
            req = {"match_id", "home", "away"}
            if not req.issubset(set(df.columns)):
                print(f"::error::Colunas da whitelist não normalizadas para match_id,home,away. "
                      f"Arquivo: {path}. Encontradas: {list(df.columns)}. Mapping: {mapping}", file=sys.stderr)
                sys.exit(6)
            df["match_id"] = df["match_id"].astype(str).str.strip()
            df["home"] = df["home"].astype(str).str.strip()
            df["away"] = df["away"].astype(str).str.strip()
            df = df[(df["match_id"]!="") & (df["home"]!="") & (df["away"]!="")]
            if df.empty:
                print(f"::error::Whitelist vazia após limpeza ({path}).", file=sys.stderr)
                sys.exit(6)
            print(f"[consensus] whitelist: {path}  linhas={len(df)}  mapping={mapping}")
            return df[["match_id","home","away"]].copy(), path
    print(f"::error::Nenhum arquivo de partidas encontrado (procurado: {', '.join(WL_CANDIDATES)}).", file=sys.stderr)
    sys.exit(6)

def _load_odds_csv(path: str, source_label: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["match_id","home","away","odds_home","odds_draw","odds_away","source"])
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"::error::Falha ao ler {path}: {e}", file=sys.stderr)
        return pd.DataFrame(columns=["match_id","home","away","odds_home","odds_draw","odds_away","source"])

    # normalizações comuns
    rename_map = {}
    lc = {c.lower(): c for c in df.columns}
    # nomes de times
    for can, alts in (("home", {"home","home_team","team_home","mandante","casa"}),
                      ("away", {"away","away_team","team_away","visitante","fora"})):
        for a in alts:
            if a in lc: 
                rename_map[lc[a]] = can
                break
    # odds
    for can, alts in (("odds_home", {"odds_home","home_odds","odd_home","home","1"}),
                      ("odds_draw", {"odds_draw","draw_odds","odd_draw","draw","x"}),
                      ("odds_away", {"odds_away","away_odds","odd_away","away","2"})):
        for a in alts:
            if a in lc:
                rename_map[lc[a]] = can
                break
    # match_id
    for a in ("match_id","id","game_id","jogo_id"):
        if a in lc:
            rename_map[lc[a]] = "match_id"
            break

    df = df.rename(columns=rename_map)

    req = {"match_id","home","away","odds_home","odds_draw","odds_away"}
    missing = [c for c in req if c not in df.columns]
    if missing:
        print(f"Warning: {source_label} colunas faltantes em {path}; ignorando. Faltantes: {missing}")
        return pd.DataFrame(columns=list(req)+["source"])

    # limpeza básica
    df = df[list(req)].copy()
    df["match_id"] = df["match_id"].astype(str).str.strip()
    for c in ("home","away"):
        df[c] = df[c].astype(str).str.strip()
    for c in ("odds_home","odds_draw","odds_away"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["match_id","home","away","odds_home","odds_draw","odds_away"])
    df = df[(df["odds_home"]>1.0) & (df["odds_draw"]>1.0) & (df["odds_away"]>1.0)]
    if df.empty:
        return pd.DataFrame(columns=list(req)+["source"])

    df["source"] = source_label
    return df

def _mean_consensus(stacked: pd.DataFrame) -> pd.DataFrame:
    # média simples das odds por match_id (mantém times pela primeira ocorrência)
    # Poderia fazer média em probabilidades; para simplicidade e reprodutibilidade, média aritmética.
    grp = stacked.groupby(["match_id","home","away"], as_index=False).agg(
        odds_home=("odds_home","mean"),
        odds_draw=("odds_draw","mean"),
        odds_away=("odds_away","mean"),
        sources_count=("source","nunique"),
    )
    # renomear para o schema pedido pelo teste
    grp = grp.rename(columns={"home":"team_home","away":"team_away"})
    return grp[["match_id","team_home","team_away","odds_home","odds_draw","odds_away","sources_count"]]

# ------------------------ main ------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    # STRICT por padrão; permitir desligar com --no-strict
    strict_group = ap.add_mutually_exclusive_group()
    strict_group.add_argument("--strict", dest="strict", action="store_true", help="Exigir odds para todos os jogos (padrão).")
    strict_group.add_argument("--no-strict", dest="strict", action="store_false", help="Não exigir odds para todos os jogos.")
    ap.set_defaults(strict=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada_dir = args.rodada
    wl, wl_path = _load_whitelist(rodada_dir)

    # fontes conhecidas
    p_theodds = os.path.join(rodada_dir, "odds_theoddsapi.csv")
    p_apifoot = os.path.join(rodada_dir, "odds_apifootball.csv")

    df_theodds = _load_odds_csv(p_theodds, "theoddsapi")
    df_apifoot = _load_odds_csv(p_apifoot, "apifootball")

    n1, n2 = len(df_theodds), len(df_apifoot)
    print(f"[consensus] fontes: theodds={n1}  apifoot={n2}")

    stacked = pd.concat([df_theodds, df_apifoot], ignore_index=True)
    if stacked.empty:
        print("::error::Nenhuma odd válida encontrada para gerar consenso.", file=sys.stderr)
        return 6

    # mantenha somente jogos da whitelist
    wl_ids = set(wl["match_id"].astype(str))
    stacked = stacked[stacked["match_id"].astype(str).isin(wl_ids)].copy()

    if stacked.empty:
        print("::error::As fontes não possuem odds para os jogos da whitelist.", file=sys.stderr)
        return 6

    # gera consenso
    out = _mean_consensus(stacked)

    # STRICT: todos os jogos precisam aparecer
    missing_ids = sorted(list(wl_ids - set(out["match_id"].astype(str))))
    if missing_ids:
        msg = f"[CRITICAL] STRICT ativo — jogos sem odds em nenhuma fonte: {missing_ids}"
        print(f"::error::{msg}", file=sys.stderr)
        return 99

    out_file = os.path.join(rodada_dir, "odds_consensus.csv")
    out.to_csv(out_file, index=False)
    if args.debug:
        print(f"[consensus][DEBUG] gravado {out_file}  linhas={len(out)}")
        print(out.head(10).to_string(index=False))

    # validação mínima para o job que checa cabeçalho
    required_output_cols = ["team_home","team_away","odds_home","odds_draw","odds_away"]
    missing = [c for c in required_output_cols if c not in out.columns]
    if missing:
        print(f"::error::Colunas ausentes no consenso: {missing}", file=sys.stderr)
        return 6

    print(f"[consensus] OK -> {out_file}")
    return 0

if __name__ == "__main__":
    sys.exit(main())