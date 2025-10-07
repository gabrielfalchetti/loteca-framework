#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consenso de odds entre múltiplas fontes (TheOddsAPI, API-Football) com:
- normalização automática de esquema (home/away e odds 1X2)
- junção opcional com data/in/matches_source.csv via match_key
- geração de odds_consensus.csv em data/out/<RODADA_ID>/
Falha forte se nenhuma fonte válida for encontrada ou se não gerar saída.

Uso:
  python -m scripts.consensus_odds_safe --rodada <RODADA_ID>
  # onde <RODADA_ID> é só o ID numérico usado para compor data/out/<ID>/...
"""

import argparse
import os
import sys
import json
import unicodedata
from typing import List, Tuple
import pandas as pd

DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

# ---------- utils ----------

def log(msg: str):
    print(f"[consensus-safe] {msg}")

def die(code: int, msg: str):
    log(f"ERRO: {msg}")
    sys.exit(code)

def _strip_accents_lower(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower()

def make_key(home: str, away: str) -> str:
    return f"{_strip_accents_lower(home)}__vs__{_strip_accents_lower(away)}"

def load_csv_safe(path: str) -> pd.DataFrame:
    try:
        if not os.path.exists(path):
            log(f"AVISO: arquivo não encontrado: {path}")
            return pd.DataFrame()
        # pandas levanta se vazio/sem colunas
        df = pd.read_csv(path)
        if df.shape[0] == 0 or df.shape[1] == 0:
            log(f"AVISO: arquivo vazio: {path}")
            return pd.DataFrame()
        return df
    except Exception as e:
        log(f"ERRO ao ler {path}: {e}")
        return pd.DataFrame()

# ---------- normalização de esquemas ----------

HOME_ALIASES = ["home", "team_home", "home_team", "mandante"]
AWAY_ALIASES = ["away", "team_away", "away_team", "visitante"]

# odds 1X2
OH_ALIASES   = ["odds_home", "home_odds", "odd_home", "o1", "price_home", "h2h_home"]
OD_ALIASES   = ["odds_draw", "draw_odds", "odd_draw", "ox", "price_draw", "h2h_draw"]
OA_ALIASES   = ["odds_away", "away_odds", "odd_away", "o2", "price_away", "h2h_away"]

def first_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        lc = cand.lower()
        if lc in cols:
            return cols[lc]
    # tenta por contains leve
    for c in df.columns:
        lc = c.lower()
        for cand in candidates:
            if cand.lower() in lc:
                return c
    return ""

def normalize_schema(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Retorna DF com colunas padronizadas: home, away, odd_home, odd_draw, odd_away, source"""

    if df.empty:
        return df

    ch = first_col(df, HOME_ALIASES)
    ca = first_col(df, AWAY_ALIASES)

    # alguns dumps trazem 'home_team'/'away_team' como dicts/ids; tentamos fallback
    if not ch or not ca:
        # heurística: procura colunas com textos de time
        text_like = [c for c in df.columns if df[c].dtype == object]
        if len(text_like) >= 2:
            ch, ca = text_like[0], text_like[1]

    if not ch or not ca:
        log(f"AVISO: não encontrei colunas de times (home/away) em {source_name}. Colunas: {list(df.columns)}")
        return pd.DataFrame()

    oh = first_col(df, OH_ALIASES)
    od = first_col(df, OD_ALIASES)
    oa = first_col(df, OA_ALIASES)

    # às vezes a fonte traz odds 1X2 em largura diferente; tentamos detectar por nomes genéricos
    if not (oh and od and oa):
        # tenta detectar por 3 colunas numéricas que parecem odds (>=1.01)
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        candidates = []
        for c in numeric_cols:
            s = df[c].dropna()
            if not s.empty and (s >= 1.01).mean() > 0.7 and (s <= 1000).mean() > 0.95:
                candidates.append(c)
        if len(candidates) >= 3:
            oh, od, oa = candidates[:3]

    out = pd.DataFrame()
    out["home"] = df[ch].astype(str)
    out["away"] = df[ca].astype(str)

    # zera odds inválidas
    def safe_num(s):
        try:
            x = float(s)
            return x if x > 1.0001 else float("nan")
        except:
            return float("nan")

    if oh in df.columns:
        out["odd_home"] = df[oh].apply(safe_num)
    else:
        out["odd_home"] = float("nan")

    if od in df.columns:
        out["odd_draw"] = df[od].apply(safe_num)
    else:
        out["odd_draw"] = float("nan")

    if oa in df.columns:
        out["odd_away"] = df[oa].apply(safe_num)
    else:
        out["odd_away"] = float("nan")

    out["source"] = source_name
    out["match_key"] = out.apply(lambda r: make_key(r["home"], r["away"]), axis=1)

    # filtra linhas que têm pelo menos uma odd válida
    mask_valid = out[["odd_home", "odd_draw", "odd_away"]].notna().any(axis=1)
    out = out[mask_valid].copy()
    out.reset_index(drop=True, inplace=True)
    return out

# ---------- consenso ----------

def consensus(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()
    base = pd.concat(dfs, ignore_index=True)
    if base.empty:
        return base

    # agrega por match_key
    grp = base.groupby("match_key", as_index=False).agg({
        "home": "first",
        "away": "first",
        "odd_home": "mean",
        "odd_draw": "mean",
        "odd_away": "mean"
    })

    # prob. implícitas (normalizadas para overround)
    for col in ["odd_home", "odd_draw", "odd_away"]:
        grp[f"imp_{col[4:]}"] = 1.0 / grp[col]

    # normaliza overround
    s = grp[["imp_home", "imp_draw", "imp_away"]].sum(axis=1)
    for k in ["imp_home", "imp_draw", "imp_away"]:
        grp[k] = grp[k] / s

    # ordena por chave para determinismo
    grp = grp.sort_values(["home", "away"]).reset_index(drop=True)
    return grp

# ---------- matches_source join ----------

def try_join_matches(cons: pd.DataFrame) -> pd.DataFrame:
    """Se existir data/in/matches_source.csv, tenta casar e carregar match_id/source."""
    path = os.path.join("data", "in", "matches_source.csv")
    if not os.path.exists(path):
        log("AVISO: data/in/matches_source.csv não encontrado para join (seguindo sem match_id).")
        return cons

    try:
        ms = pd.read_csv(path)
    except Exception as e:
        log(f"AVISO: falha ao ler matches_source.csv: {e} (seguindo sem join).")
        return cons

    # exige colunas mínimas
    needed = {"match_id", "home", "away"}
    if not needed.issubset(set(c.lower() for c in ms.columns)):
        # tenta normalizar cabeçalhos
        cols_map = {c.lower(): c for c in ms.columns}
        miss = needed - set(cols_map.keys())
        if miss:
            log(f"AVISO: matches_source.csv sem colunas {miss} (seguindo sem join).")
            return cons

    # harmoniza nomes reais preservando maiúsculas do arquivo
    cols_map = {c.lower(): c for c in ms.columns}
    mh = cols_map["home"]; ma = cols_map["away"]; mid = cols_map["match_id"]
    tmp = ms[[mid, mh, ma]].copy()
    tmp["match_key"] = tmp.apply(lambda r: make_key(r[mh], r[ma]), axis=1)

    out = cons.merge(tmp[[mid, "match_key"]], on="match_key", how="left")
    # reordena (se existir match_id)
    if mid in out.columns:
        out = out[[mid, "home", "away", "odd_home", "odd_draw", "odd_away",
                   "imp_home", "imp_draw", "imp_away", "match_key"]]
        out = out.rename(columns={mid: "match_id"})
    return out

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID numérico da rodada (pasta em data/out/<ID>/)")
    args = ap.parse_args()

    rid = str(args.rodada).strip()
    out_dir = os.path.join("data", "out", rid)
    if not os.path.isdir(out_dir):
        die(2, f"diretório de saída não existe: {out_dir}")

    # entradas esperadas
    odds_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    odds_apifoot = os.path.join(out_dir, "odds_apifootball.csv")

    df1 = load_csv_safe(odds_theodds)
    if not df1.empty:
        df1n = normalize_schema(df1, "theoddsapi")
    else:
        df1n = pd.DataFrame()

    df2 = load_csv_safe(odds_apifoot)
    if not df2.empty:
        df2n = normalize_schema(df2, "apifootball")
    else:
        df2n = pd.DataFrame()

    dfs = [d for d in [df1n, df2n] if not d.empty]
    if not dfs:
        die(1, "nenhuma fonte de odds disponível.")

    cons = consensus(dfs)
    if cons.empty:
        die(1, "consenso gerou DF vazio.")

    cons = try_join_matches(cons)

    out_path = os.path.join(out_dir, "odds_consensus.csv")
    cons.to_csv(out_path, index=False)
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        die(1, "odds_consensus.csv não gerado.")

    # também gera um pequeno JSON resumo
    resume = {
        "total_matches": int(cons.shape[0]),
        "source_files": {
            "theoddsapi": os.path.exists(odds_theodds),
            "apifootball": os.path.exists(odds_apifoot)
        }
    }
    with open(os.path.join(out_dir, "odds_consensus_meta.json"), "w", encoding="utf-8") as f:
        json.dump(resume, f, ensure_ascii=False, indent=2)

    log(f"OK -> {out_path} ({cons.shape[0]} jogos)")

if __name__ == "__main__":
    main()