#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lê odds coletadas (TheOddsAPI; RapidAPI opcional) e produz
data/out/<RODADA>/odds_consensus.csv exigindo pelo menos 2 odds válidas (>1.0).

Args:
  --rodada RODADA
  --debug
"""

import os, sys, argparse, math
import pandas as pd

def log(msg): print(f"[consensus-safe] {msg}")
def ddbg(debug, msg): 
    if debug: print(f"[consensus-safe][DEBUG] {msg}")

def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    repl = {"": float("nan"), "[]": float("nan"), "None": float("nan"), "null": float("nan")}
    return df.replace(repl)

def load_csv(path, debug=False):
    if not os.path.isfile(path):
        log(f"AVISO: arquivo não encontrado: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        return sanitize(df)
    except Exception as e:
        log(f"AVISO: falha ao ler {path}: {e}")
        return pd.DataFrame()

def american_to_decimal(x):
    if x is None or (isinstance(x, float) and math.isnan(x)): return float("nan")
    try:
        v = float(x)
    except Exception:
        return float("nan")
    if 1.01 <= v <= 100.0:  # já decimal
        return v
    if v > 0: return 1.0 + v/100.0
    if v < 0: return 1.0 + 100.0/abs(v)
    return float("nan")

def to_float(c):
    def f(v):
        if v in ("", "[]", "None", "null"): return float("nan")
        try: return float(v)
        except: return float("nan")
    return c.map(f)

def ensure_mapping(df: pd.DataFrame, name: str, debug: bool) -> pd.DataFrame:
    # garante colunas core
    need = ["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        ddbg(debug, f"{name} faltando colunas {missing}, tentando mapear por nomes próximos…")
        # tentativas simples
        cols = {c.lower(): c for c in df.columns}
        m = dict()
        m["team_home"]  = cols.get("team_home")  or cols.get("home_team")  or cols.get("home") or "team_home"
        m["team_away"]  = cols.get("team_away")  or cols.get("away_team")  or cols.get("away") or "team_away"
        m["match_key"]  = cols.get("match_key")  or "match_key"
        m["odds_home"]  = cols.get("odds_home")  or cols.get("home_odds")  or cols.get("price_home") or "odds_home"
        m["odds_draw"]  = cols.get("odds_draw")  or cols.get("draw_odds")  or cols.get("price_draw") or "odds_draw"
        m["odds_away"]  = cols.get("odds_away")  or cols.get("away_odds")  or cols.get("price_away") or "odds_away"
        df = df.rename(columns={v:k for k,v in m.items() if v in df.columns})
    # cast numérico + conversão americana→decimal quando necessário
    for oc in ("odds_home","odds_draw","odds_away"):
        s = to_float(df[oc]) if oc in df.columns else pd.Series(dtype=float)
        df[oc] = s.map(american_to_decimal)
    return df

def valid_row(row) -> bool:
    cnt = 0
    for oc in ("odds_home","odds_draw","odds_away"):
        v = row.get(oc, float("nan"))
        if isinstance(v,(int,float)) and v > 1.0 and math.isfinite(v):
            cnt += 1
    return cnt >= 2

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    debug = bool(args.debug or os.getenv("DEBUG","").lower()=="true")

    out_dir = os.path.join("data","out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    # fontes
    theodds_csv = os.path.join(out_dir, "odds_theoddsapi.csv")
    rapid_csv   = os.path.join(out_dir, "odds_apifootball.csv")  # opcional

    srcs = []
    theodds = load_csv(theodds_csv, debug)
    if not theodds.empty:
        theodds = ensure_mapping(theodds, "theoddsapi", debug)
        srcs.append(theodds)
    else:
        log("lido odds_theoddsapi.csv -> 0 linhas; válidas: 0")

    rapid = load_csv(rapid_csv, debug)
    if not rapid.empty:
        rapid = ensure_mapping(rapid, "apifootball", debug)
        srcs.append(rapid)

    if not srcs:
        log("consenso bruto: 0 linhas; válidas (>=2 odds > 1.0): 0")
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    # concatena e escolhe melhor preço por partida/outcome
    all_df = pd.concat(srcs, ignore_index=True)
    # dedupe por match_key normalizado
    all_df["match_key"] = all_df["match_key"].astype(str)
    # agrupa
    def best(series):
        vals = [v for v in series if isinstance(v,(int,float)) and v>1.0 and math.isfinite(v)]
        return max(vals) if vals else float("nan")
    grouped = all_df.groupby(["match_key","team_home","team_away"], as_index=False).agg({
        "odds_home": best,
        "odds_draw": best,
        "odds_away": best,
    })

    # filtra válidas (>=2 odds > 1.0)
    mask_valid = grouped.apply(valid_row, axis=1)
    valid_df = grouped[mask_valid].reset_index(drop=True)

    log(f"lido odds_theoddsapi.csv -> {len(theodds)} linhas; válidas: {len(valid_df)}")
    if valid_df.empty:
        log(f"consenso bruto: {len(grouped)} (soma linhas válidas dos provedores); finais (>=2 odds > 1.0): 0")
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    out_csv = os.path.join(out_dir, "odds_consensus.csv")
    valid_df.to_csv(out_csv, index=False)
    log(f"OK -> {out_csv} ({len(valid_df)} linhas) | mapping theoddsapi: team_home='team_home', team_away='team_away', match_key='match_key', odds_home='odds_home', odds_draw='odds_draw', odds_away='odds_away'")

if __name__ == "__main__":
    main()