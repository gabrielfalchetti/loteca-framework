#!/usr/bin/env python3
# scripts/consensus_odds_safe.py
from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import List, Dict

import pandas as pd

REQUIRED_WL = {"match_id", "home", "away"}
OUT_COLS = [
    "match_id",         # mantemos para rastreabilidade interna
    "team_home",        # exigido pelo seu step de validação
    "team_away",        # exigido pelo seu step de validação
    "odds_home",
    "odds_draw",
    "odds_away",
    "sources_count"     # auditoria do consenso
]

def log(msg: str, flush: bool = True):
    print(msg, flush=flush)

def dbg(enabled: bool, msg: str):
    if enabled:
        print(f"[consensus][DEBUG] {msg}", flush=True)

def warn(msg: str):
    print(f"Warning: {msg}", flush=True)

def read_csv_safe(path: str, debug: bool=False) -> pd.DataFrame:
    """
    Lê CSV; se não existir ou estiver vazio, retorna DataFrame vazio com 0 linhas.
    """
    if not os.path.exists(path):
        dbg(debug, f"{os.path.basename(path)} não existe")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception as e:
        warn(f"Falha ao ler {path}: {e}")
        return pd.DataFrame()
    # Se for NaN-only, normaliza para 0 linhas
    if df.shape[0] == 0:
        dbg(debug, f"{os.path.basename(path)} lido (somente cabeçalho ou 0 linhas)")
    return df

def normalize_cols(df: pd.DataFrame, mapping_opts: List[List[str]]) -> Dict[str, str]:
    """
    Dado um DF e opções de nomes por campo, cria mapeamento canônico.
    mapping_opts = [
       ["home","team_home"], ["away","team_away"], ["odds_home"], ["odds_draw"], ["odds_away"], ["match_id"]
    ]
    """
    cols_lower = {c.lower(): c for c in df.columns}
    out_map: Dict[str, str] = {}
    for opts in mapping_opts:
        canonical = opts[0]
        found = None
        for opt in opts:
            if opt in cols_lower:
                found = cols_lower[opt]
                break
        if found:
            out_map[canonical] = found
    return out_map

def load_whitelist(rodada_dir: str, debug: bool=False) -> pd.DataFrame:
    candidates = [
        os.path.join(rodada_dir, "matches_whitelist.csv"),
        os.path.join(rodada_dir, "matches_source.csv"),
        "data/in/matches_whitelist.csv",
        "data/in/matches_source.csv",
    ]
    path = None
    for p in candidates:
        if os.path.exists(p) and os.path.getsize(p) > 0:
            path = p
            break
    if not path:
        raise FileNotFoundError("Whitelist não encontrada em locais padrão.")

    df = pd.read_csv(path)
    # normalizar cabeçalhos p/ case-insensitive
    cols = {c.lower(): c for c in df.columns}
    missing = REQUIRED_WL - set(cols.keys())
    if missing:
        raise ValueError(f"Whitelist sem colunas {sorted(list(REQUIRED_WL))}; encontrado={list(df.columns)}")

    ren = {cols["match_id"]: "match_id", cols["home"]: "home", cols["away"]: "away"}
    df = df.rename(columns=ren)
    df["match_id"] = df["match_id"].astype(str)

    log(f"[consensus] whitelist: {path}  linhas={len(df)}  mapping={{{'match_id':'match_id','home':'home','away':'away'}}}")
    return df[["match_id","home","away"]].copy()

def clean_odds_df(df: pd.DataFrame, source_name: str, debug: bool=False) -> pd.DataFrame:
    """
    Harmoniza colunas para o padrão (match_id, home, away, odds_home, odds_draw, odds_away, source).
    Ignora DF sem colunas suficientes.
    """
    if df.empty:
        return pd.DataFrame(columns=["match_id","home","away","odds_home","odds_draw","odds_away","source"])

    # mapear colunas possíveis
    colmap = normalize_cols(df, [
        ["match_id"],
        ["home","team_home"],
        ["away","team_away"],
        ["odds_home","price_home","home_odds"],
        ["odds_draw","price_draw","draw_odds"],
        ["odds_away","price_away","away_odds"],
    ])

    required_min = {"home","away"}  # match_id pode vir ausente em odds; a gente cruza por nomes com whitelist
    faltantes = required_min - set(colmap.keys())
    if faltantes:
        warn(f"{source_name} colunas faltantes em {source_name}: {sorted(list(faltantes))}; ignorando fonte.")
        return pd.DataFrame(columns=["match_id","home","away","odds_home","odds_draw","odds_away","source"])

    # renomear para canônicos que tivermos
    df2 = df.rename(columns={v: k for k, v in colmap.items()})

    # assegurar todas as colunas padrão presentes
    for c in ["match_id","home","away","odds_home","odds_draw","odds_away"]:
        if c not in df2.columns:
            df2[c] = pd.NA

    # normalizações
    df2["home"] = df2["home"].astype(str)
    df2["away"] = df2["away"].astype(str)
    df2["match_id"] = df2["match_id"].astype(str).fillna("")

    # manter somente colunas padrão + source
    df2 = df2[["match_id","home","away","odds_home","odds_draw","odds_away"]].copy()
    df2["source"] = source_name
    # remover linhas totalmente vazias de odds
    df2 = df2.dropna(subset=["odds_home","odds_away"], how="all")
    return df2

def consensus_by_median(df_sources: pd.DataFrame, debug: bool=False) -> pd.DataFrame:
    """
    Calcula mediana por (match_id,home,away) nas colunas de odds.
    """
    if df_sources.empty:
        return pd.DataFrame(columns=OUT_COLS)

    grp = df_sources.groupby(["match_id","home","away"], dropna=False)
    agg = grp.agg(
        odds_home=("odds_home","median"),
        odds_draw=("odds_draw","median"),
        odds_away=("odds_away","median"),
        sources_count=("source","nunique"),
    ).reset_index()

    # renomear home/away para team_home/team_away (como seu step espera)
    agg = agg.rename(columns={"home":"team_home","away":"team_away"})
    # ordenar colunas
    agg = agg[OUT_COLS]
    return agg

def main():
    ap = argparse.ArgumentParser(description="Gera odds_consensus.csv unificando fontes de odds.")
    ap.add_argument("--rodada", required=True, help="Diretório da rodada/trabalho (OUT_DIR).")
    ap.add_argument("--strict", dest="strict", action="store_true", help="Exige odds válidas para TODOS os jogos da whitelist.")
    ap.add_argument("--no-strict", dest="strict", action="store_false", help="Não exige cobertura total.")
    ap.add_argument("--debug", action="store_true", help="Logs detalhados.")
    ap.set_defaults(strict=False)
    args = ap.parse_args()

    rodada_dir = args.rodada
    theodds_path = os.path.join(rodada_dir, "odds_theoddsapi.csv")
    apifoot_path = os.path.join(rodada_dir, "odds_apifootball.csv")
    out_path = os.path.join(rodada_dir, "odds_consensus.csv")

    log("="*50)
    log(f"[consensus] {'STRICT MODE — ' if args.strict else ''}RODADA: {rodada_dir}")
    log("="*50)

    try:
        wl = load_whitelist(rodada_dir, debug=args.debug)
    except Exception as e:
        print(f"##[error][consensus] Whitelist inválida: {e}")
        sys.exit(6)

    # carregar fontes
    df_theodds_raw = read_csv_safe(theodds_path, debug=args.debug)
    df_apifoot_raw = read_csv_safe(apifoot_path, debug=args.debug)

    df_theodds = clean_odds_df(df_theodds_raw, "theoddsapi", debug=args.debug)
    df_apifoot = clean_odds_df(df_apifoot_raw, "apifootball", debug=args.debug)

    # filtrar pelas partidas da whitelist — por (home, away) e/ou por match_id se houver
    wl_key = wl[["match_id","home","away"]].copy()
    wl_key["home_l"] = wl_key["home"].str.lower()
    wl_key["away_l"] = wl_key["away"].str.lower()

    def filter_by_wl(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        tmp = df.copy()
        tmp["home_l"] = tmp["home"].str.lower()
        tmp["away_l"] = tmp["away"].str.lower()
        # join por nomes; se match_id vier preenchido na fonte, também podemos aproveitar merge por match_id
        merged = tmp.merge(
            wl_key[["match_id","home_l","away_l"]],
            on=["home_l","away_l"],
            how="inner",
            suffixes=("","_wl")
        )
        # se a fonte trouxe match_id (às vezes vazio), prioriza o da whitelist
        merged["match_id"] = merged["match_id_wl"]
        merged = merged.drop(columns=["home_l","away_l","match_id_wl"])
        return merged

    df_theodds_f = filter_by_wl(df_theodds)
    df_apifoot_f = filter_by_wl(df_apifoot)

    if args.debug:
        dbg(True, f"theodds -> linhas válidas após WL: {len(df_theodds_f)}")
        dbg(True, f"apifoot  -> linhas válidas após WL: {len(df_apifoot_f)}")

    fontes_count = (0 if df_theodds_f.empty else 1) + (0 if df_apifoot_f.empty else 1)
    log(f"[consensus] fontes: theodds={'0' if df_theodds_f.empty else len(df_theodds_f)}  apifoot={'0' if df_apifoot_f.empty else len(df_apifoot_f)}")

    df_all = pd.concat([df_theodds_f, df_apifoot_f], ignore_index=True)
    if df_all.empty:
        print("##[error]Nenhuma odd válida encontrada para gerar consenso.")
        # mensagem diagnóstica mais clara sobre colunas faltantes
        def missing_for(df_raw: pd.DataFrame, name: str) -> List[str]:
            required = ["home","away","odds_home","odds_away"]
            lower = [c.lower() for c in df_raw.columns]
            miss = [c for c in required if c not in lower and ("team_"+c if c in ["home","away"] else c) not in lower]
            return miss
        miss_theodds = missing_for(df_theodds_raw, "theoddsapi")
        miss_apifoot = missing_for(df_apifoot_raw, "apifootball")
        if miss_theodds:
            warn(f"theoddsapi colunas faltantes em {theodds_path}; ignorando. Faltantes: {miss_theodds}")
        if miss_apifoot:
            warn(f"apifootball colunas faltantes em {apifoot_path}; ignorando. Faltantes: {miss_apifoot}")
        sys.exit(6)

    df_cons = consensus_by_median(df_all, debug=args.debug)

    # STRICT: exigir cobertura para todos os jogos da WL
    if args.strict:
        wl_ids = set(wl["match_id"].astype(str))
        covered_ids = set(df_cons["match_id"].astype(str))
        missing_ids = sorted(wl_ids - covered_ids)
        if missing_ids:
            # montar relatório dos jogos faltantes
            miss_rows = wl[wl["match_id"].astype(str).isin(missing_ids)].copy()
            miss_rows = miss_rows.rename(columns={"home":"team_home","away":"team_away"})
            miss_preview = miss_rows[["match_id","team_home","team_away"]].to_string(index=False)
            print("##[error][CRITICAL] Jogos sem odds após consenso (modo estrito ligado).")
            print(miss_preview)
            sys.exit(6)

    # salvar
    os.makedirs(rodada_dir, exist_ok=True)
    df_cons = df_cons[OUT_COLS]
    df_cons.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # preview
    head_n = min(20, len(df_cons))
    log(f"[consensus] OK -> {out_path} (linhas={len(df_cons)})")
    if head_n > 0:
        log("===== Preview odds_consensus =====")
        log(df_cons.head(head_n).to_string(index=False))

if __name__ == "__main__":
    main()