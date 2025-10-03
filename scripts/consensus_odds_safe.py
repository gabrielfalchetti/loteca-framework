#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera data/out/<rodada>/odds_consensus.csv A PARTIR DO QUE EXISTIR.
Prioridade:
  1) data/out/<rodada>/odds_theoddsapi.csv
  2) data/in/<rodada>/odds_theoddsapi.csv (copia para /out)
Aceita odds como:
  - decimal (2.05)
  - american (+110, -150)
  - string com listas/arrays: "[2.1, 2.2, 2.05]" -> extrai melhor, média etc.
Critério de validade: pelo menos 2 outcomes com odds > 1.0
Saída: colunas normalizadas
  team_home, team_away, match_key, odds_home, odds_draw, odds_away
"""

import argparse, os, sys, re, math, json, ast, shutil
from typing import List, Tuple, Optional
import numpy as np
import pandas as pd


# ---------- util ----------
def ensure_out_dir(rodada: str) -> str:
    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def copy_if_exists(src: str, dst: str) -> Optional[str]:
    if os.path.isfile(src):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
        return dst
    return None

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    def norm(c: str) -> str:
        c = str(c).strip().lower()
        c = re.sub(r"[ \t\.\-/]+", "_", c)
        c = re.sub(r"[\(\)\[\]\{\}]+", "", c)
        return c
    out = df.copy()
    out.columns = [norm(c) for c in df.columns]
    return out

def american_to_decimal(x: float) -> float:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return np.nan
    v = float(x)
    return 1.0 + (v/100.0) if v > 0 else 1.0 + (100.0/abs(v))

def to_num_or_nan(s: str) -> float:
    """Converte token para float decimal odds; aceita american (+120)."""
    if s is None:
        return np.nan
    t = str(s).strip()
    if t == "" or t.lower() in {"nan", "none", "null"}:
        return np.nan
    # american?
    if re.fullmatch(r"[+\-]?\d+(\.\d+)?", t):
        # pode ser decimal (2.05) ou american (+120)
        try:
            fv = float(t.replace(",", "."))
        except Exception:
            return np.nan
        # heuris.: odds decimais típicas entre (1, 100); american em módulo >= 100 (geralmente)
        if abs(fv) >= 100 or t.startswith(("+","-")):
            return american_to_decimal(fv)
        return fv
    # decimal com vírgula
    try:
        return float(t.replace(",", "."))
    except Exception:
        return np.nan

def parse_listish(cell) -> List[float]:
    """
    Aceita:
      - lista real [2.1, 2.2]
      - string "[2.1,2.2]" / "2.1;2.2" / "2.1|2.2" / "2.1, 2.2"
      - mistura de american e decimal
    """
    if cell is None:
        return []
    if isinstance(cell, (list, tuple)):
        tokens = cell
    else:
        txt = str(cell).strip()
        if txt == "" or txt in {"[]", "nan", "None", "null"}:
            return []
        # tenta literal_eval
        try:
            val = ast.literal_eval(txt)
            if isinstance(val, (list, tuple)):
                tokens = val
            else:
                tokens = [val]
        except Exception:
            # tenta split manual
            sep = ";" if ";" in txt else ("|" if "|" in txt else ",")
            tokens = [t.strip() for t in txt.split(sep)]
    out = []
    for tok in tokens:
        out.append(to_num_or_nan(tok))
    return [v for v in out if v and v > 1.0]

def pick(df: pd.DataFrame, cands: List[str]) -> str:
    for c in cands:
        if c in df.columns:
            return c
    # procura por "contém"
    for want in cands:
        for col in df.columns:
            if want in col:
                return col
    return ""

TH_ALS = ["team_home","home_team","mandante","time_casa","time_home","equipa_casa"]
TA_ALS = ["team_away","away_team","visitante","time_fora","time_away","equipa_fora"]
MK_ALS = ["match_key","game_key","fixture_key","key","match","partida","id_partida"]

HOME_ALS = ["odds_home","home_odds","price_home","home_price","home_decimal","price1","h2h_home","m1","h","home"]
DRAW_ALS = ["odds_draw","draw_odds","price_draw","draw_price","draw_decimal","pricex","h2h_draw","mx","x","tie","draw"]
AWAY_ALS = ["odds_away","away_odds","price_away","away_price","away_decimal","price2","h2h_away","m2","a","away"]

# qualquer coluna que "pareça conter listas de odds"
def find_listish_columns(df: pd.DataFrame, side: str) -> List[str]:
    keys = {
        "home": ["home", "team1", "m1", "price1", "h2h_home"],
        "draw": ["draw", "empate", "x", "mx", "pricex", "tie"],
        "away": ["away", "team2", "m2", "price2", "h2h_away"],
    }[side]
    cols = []
    for c in df.columns:
        s = str(c)
        if any(k in s for k in keys) and ("odds" in s or "price" in s or "decimal" in s or "prices" in s or "book" in s):
            cols.append(c)
    return cols

def best_and_mean_from_columns(df: pd.DataFrame, cols: List[str]) -> Tuple[pd.Series, pd.Series]:
    """Extrai melhor (max) e média das odds, combinando colunas simples e listas."""
    best = pd.Series(np.nan, index=df.index, dtype="float64")
    mean = pd.Series(np.nan, index=df.index, dtype="float64")

    for c in cols:
        series = df[c]
        if series.dtype == object:
            # tenta listas
            arr = series.apply(parse_listish)
            # melhor daquela coluna
            local_best = arr.apply(lambda xs: max(xs) if xs else np.nan)
            # média daquela coluna
            local_mean = arr.apply(lambda xs: float(np.mean(xs)) if xs else np.nan)
        else:
            local_best = pd.to_numeric(series, errors="coerce")
            local_mean = pd.to_numeric(series, errors="coerce")

        # trata american que veio como número textual
        local_best = local_best.apply(to_num_or_nan)
        local_mean = local_mean.apply(to_num_or_nan)

        # junta no global
        best = np.fmax(best, local_best)  # max ignorando nan
        mean = np.where(np.isnan(mean), local_mean, np.where(np.isnan(local_mean), mean, (mean + local_mean)/2.0))
        mean = pd.Series(mean, index=df.index, dtype="float64")

    # filtra odds inválidas
    best = best.mask(best <= 1.0)
    mean = mean.mask(mean <= 1.0)
    return best, mean

def build_match_key(df: pd.DataFrame, th: str, ta: str) -> pd.Series:
    return (
        df[th].astype(str).str.strip().str.lower()
        + "__vs__" +
        df[ta].astype(str).str.strip().str.lower()
    )

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = ensure_out_dir(args.rodada)

    # origem: theoddsapi
    p_out = os.path.join(out_dir, "odds_theoddsapi.csv")
    if os.path.isfile(p_out):
        base = p_out
    else:
        p_in = os.path.join("data","in",args.rodada,"odds_theoddsapi.csv")
        base = copy_if_exists(p_in, p_out) or p_in

    if not base or not os.path.isfile(base):
        print("[consensus-safe] ERRO: nenhuma fonte de odds encontrada (theoddsapi).")
        sys.exit(10)

    raw = pd.read_csv(base)
    df = clean_columns(raw)

    th = pick(df, TH_ALS)
    ta = pick(df, TA_ALS)
    mk = pick(df, MK_ALS)
    if not mk and th and ta:
        df["match_key"] = build_match_key(df, th, ta)
        mk = "match_key"

    if not (th and ta and mk and mk in df.columns):
        print("[consensus-safe] ERRO: não foi possível identificar team_home/team_away/match_key.")
        sys.exit(10)

    # tenta encontrar colunas de odds por lado (simples e listas)
    ch = pick(df, HOME_ALS)
    cd = pick(df, DRAW_ALS)
    ca = pick(df, AWAY_ALS)

    home_cols = list({c for c in ([ch] if ch else []) + find_listish_columns(df, "home") if c})
    draw_cols = list({c for c in ([cd] if cd else []) + find_listish_columns(df, "draw") if c})
    away_cols = list({c for c in ([ca] if ca else []) + find_listish_columns(df, "away") if c})

    # extrai melhor e média por lado
    best_h, mean_h = best_and_mean_from_columns(df, home_cols) if home_cols else (pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index))
    best_d, mean_d = best_and_mean_from_columns(df, draw_cols) if draw_cols else (pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index))
    best_a, mean_a = best_and_mean_from_columns(df, away_cols) if away_cols else (pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index))

    # monta tabela
    out = pd.DataFrame({
        "team_home": df[th].astype(str),
        "team_away": df[ta].astype(str),
        "match_key": df[mk].astype(str),
        "odds_home": best_h.astype("float64"),
        "odds_draw": best_d.astype("float64"),
        "odds_away": best_a.astype("float64"),
        # guardamos a média em colunas auxiliares (usadas pela prob_from_odds_consensus)
        "_mean_home": mean_h.astype("float64"),
        "_mean_draw": mean_d.astype("float64"),
        "_mean_away": mean_a.astype("float64"),
    })

    # valida: pelo menos 2 outcomes com odds > 1.0
    mask_valid = (out[["odds_home","odds_draw","odds_away"]] > 1.0).sum(axis=1) >= 2
    invalid_reasons = {
        "menos_de_duas_odds": int((~mask_valid).sum())
    }

    if args.debug:
        print(f"[consensus-safe] lido odds_theoddsapi.csv -> {len(out)} linhas; válidas: {int(mask_valid.sum())}")
        print(f"[consensus-safe] motivos inválidos theoddsapi: {invalid_reasons}")
        sample = out.head(5).to_dict(orient="records")
        print(f"[consensus-safe] AMOSTRA (top 5): {sample}")

    out_valid = out[mask_valid].copy()
    if out_valid.empty:
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        print(f"[consensus-safe] consenso bruto: {len(out)} linhas; válidas (>=2 odds > 1.0): 0")
        sys.exit(10)

    # salva versão enxuta (sem as médias auxiliares na saída principal)
    out_core = out_valid[["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]].copy()
    out_core.to_csv(os.path.join(out_dir, "odds_consensus.csv"), index=False)

    # também salva uma cópia estendida (com médias) para prob_from_odds_consensus usar se quiser
    out_valid.to_csv(os.path.join(out_dir, "odds_consensus_ext.csv"), index=False)

    print(f"[consensus-safe] OK -> {os.path.join(out_dir,'odds_consensus.csv')} ({len(out_core)} linhas)")

if __name__ == "__main__":
    main()