#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera consenso de odds a partir de múltiplas fontes (TheOddsAPI e API-Football, se existirem).
Regras:
- NÃO inventa dados.
- Se nenhuma partida da whitelist casar com odds reais => sai com código 6 e diagnóstico.
- Normalização agressiva de nomes p/ casar com mais segurança (lower, rm acentos, pontuação).
- Aliases opcionais: se existir data/aliases/*.csv, aplica mapeamentos.
Saída: data/out/<RID>/odds_consensus.csv com colunas:
  match_id,team_home,team_away,odds_home,odds_draw,odds_away,source
"""

from __future__ import annotations
import sys
import os
import csv
import re
import unicodedata
from collections import defaultdict
from typing import Dict, Tuple, List

import pandas as pd

EXIT_CODE = 6

def log(msg: str):
    print(f"[consensus] {msg}")

def debug(msg: str):
    print(f"[consensus][DEBUG] {msg}")

def err(msg: str):
    print(f"::error::{msg}", file=sys.stderr)

def warn(msg: str):
    print(f"Warning: {msg}")

def normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_aliases() -> Dict[str, str]:
    """
    Carrega aliases opcionais de data/aliases/*.csv com formato:
    from,to
    Ex.: "atletico mg","atletico mineiro"
    """
    out: Dict[str, str] = {}
    base = "data/aliases"
    if not os.path.isdir(base):
        return out
    for name in os.listdir(base):
        if not name.endswith(".csv"):
            continue
        path = os.path.join(base, name)
        try:
            df = pd.read_csv(path)
            for _, row in df.iterrows():
                f = normalize(row.get("from", ""))
                t = normalize(row.get("to", ""))
                if f and t:
                    out[f] = t
        except Exception as e:
            warn(f"Falha ao carregar aliases {path}: {e}")
    if out:
        debug(f"Aliases carregados: {len(out)}")
    return out

def apply_alias(s: str, aliases: Dict[str, str]) -> str:
    ns = normalize(s)
    return aliases.get(ns, ns)

def read_whitelist(out_dir: str) -> pd.DataFrame:
    path = os.path.join(out_dir, "matches_whitelist.csv")
    if not os.path.isfile(path):
        err(f"Arquivo whitelist ausente: {path}")
        sys.exit(EXIT_CODE)
    df = pd.read_csv(path)
    need = {"match_id", "match_key", "team_home", "team_away"}
    if not need.issubset(df.columns):
        err(f"Colunas ausentes em whitelist ({path}). Esperado: {sorted(need)}")
        sys.exit(EXIT_CODE)
    return df

def read_theodds(out_dir: str) -> pd.DataFrame:
    path = os.path.join(out_dir, "odds_theoddsapi.csv")
    if not os.path.isfile(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    # colunas: match_id,home,away,region,sport,odds_home,odds_draw,odds_away,last_update,source
    cols = {"home","away","odds_home","odds_draw","odds_away"}
    if not cols.issubset(df.columns):
        warn(f"theoddsapi colunas faltantes em {path}; ignorando.")
        return pd.DataFrame()
    df["src"] = "theoddsapi"
    return df

def read_apifootball(out_dir: str) -> pd.DataFrame:
    path = os.path.join(out_dir, "odds_apifootball.csv")
    if not os.path.isfile(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    # aceitar as mesmas colunas
    cols = {"home","away","odds_home","odds_draw","odds_away"}
    if not cols.issubset(df.columns):
        warn(f"apifootball colunas faltantes em {path}; ignorando.")
        return pd.DataFrame()
    df["src"] = "apifootball"
    return df

def consensus_for_match(rows: List[pd.Series]) -> Tuple[float,float,float]:
    # média simples entre fontes
    oh = [r["odds_home"] for r in rows if pd.notna(r["odds_home"])]
    od = [r["odds_draw"] for r in rows if pd.notna(r["odds_draw"])]
    oa = [r["odds_away"] for r in rows if pd.notna(r["odds_away"])]
    if not oh or not od or not oa:
        return (float("nan"), float("nan"), float("nan"))
    return (sum(oh)/len(oh), sum(od)/len(od), sum(oa)/len(oa))

def main():
    if len(sys.argv) < 3 or sys.argv[1] not in ("--rodada",):
        print("Uso: python -m scripts.consensus_odds_safe --rodada <OUT_DIR>")
        sys.exit(EXIT_CODE)
    out_dir = sys.argv[2]
    log("===================================================")
    log("GERANDO ODDS CONSENSUS")
    log(f"RODADA_DIR: {out_dir}")
    log("===================================================")

    aliases = load_aliases()
    wl = read_whitelist(out_dir).copy()
    wl["nh"] = wl["team_home"].apply(lambda s: apply_alias(s, aliases))
    wl["na"] = wl["team_away"].apply(lambda s: apply_alias(s, aliases))

    # fontes
    tdf = read_theodds(out_dir)
    adf = read_apifootball(out_dir)
    src_count = len(tdf) + len(adf)

    debug(f"Carregado odds from theoddsapi: {len(tdf)} linhas")
    debug(f"Carregado odds from apifootball: {len(adf)} linhas")

    if src_count == 0:
        err("Nenhuma fonte de odds disponível (theoddsapi/apifootball).")
        sys.exit(EXIT_CODE)

    all_odds = pd.concat([tdf, adf], ignore_index=True) if len(adf) else tdf
    if all_odds.empty:
        err("As fontes de odds foram carregadas mas estão vazias.")
        sys.exit(EXIT_CODE)

    # normalizar
    all_odds["nh"] = all_odds["home"].astype(str).apply(normalize)
    all_odds["na"] = all_odds["away"].astype(str).apply(normalize)

    # join por (nh,na)
    m = wl.merge(
        all_odds,
        on=["nh","na"],
        how="left",
        suffixes=("","")
    )

    # filtrar somente linhas casadas
    matched = m.dropna(subset=["odds_home","odds_draw","odds_away"])
    if matched.empty:
        warn("[consensus] Nenhum jogo da whitelist casou com odds.")
        # Diagnóstico útil
        print("==== DIAGNÓSTICO DE MATCHING ====")
        print("WHITELIST (até 20):")
        print(wl[["match_id","team_home","team_away"]].head(20).to_string(index=False))
        print("\nTIMES ENCONTRADOS NAS FONTES (amostra até 20 pares únicos):")
        uniq = all_odds[["home","away"]].drop_duplicates().head(20)
        if uniq.empty:
            print("(vazio)")
        else:
            print(uniq.to_string(index=False))
        print("=================================")
        # escrever arquivo vazio e falhar explicitamente
        out = os.path.join(out_dir, "odds_consensus.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            f.write("match_id,team_home,team_away,odds_home,odds_draw,odds_away,source\n")
        err(f"odds_consensus.csv está vazio em {out}.")
        sys.exit(EXIT_CODE)

    # agregar por jogo original (match_id da whitelist)
    rows = []
    for mk, g in matched.groupby("match_id"):
        # manter nomes “limpos” da whitelist
        wh = wl.loc[wl["match_id"] == mk].iloc[0]
        oh, od, oa = consensus_for_match(list(g.itertuples(index=False)))
        if pd.isna(oh) or pd.isna(od) or pd.isna(oa):
            continue
        rows.append({
            "match_id": mk,
            "team_home": wh["team_home"],
            "team_away": wh["team_away"],
            "odds_home": float(oh),
            "odds_draw": float(od),
            "odds_away": float(oa),
            "source": "consensus"
        })

    out = os.path.join(out_dir, "odds_consensus.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["match_id","team_home","team_away","odds_home","odds_draw","odds_away","source"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    if not rows:
        err(f"odds_consensus.csv está vazio em {out}.")
        sys.exit(EXIT_CODE)

    log(f"OK -> {out} (jogos={len(rows)})")
    debug("Preview odds_consensus (até 10):")
    try:
        print(pd.DataFrame(rows).head(10).to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()