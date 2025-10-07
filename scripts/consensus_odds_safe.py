#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
consensus_odds_safe.py
----------------------
Gera odds_consensus.csv em esquema padronizado para o pipeline:

Saída: data/out/<RODADA>/odds_consensus.csv com colunas:
  - match_id
  - team_home
  - team_away
  - odds_home
  - odds_draw
  - odds_away
  - source

Entrada (opcional, usa as que existirem):
  - data/out/<RODADA>/odds_theoddsapi.csv
  - data/out/<RODADA>/odds_apifootball.csv

Uso:
  python -m scripts.consensus_odds_safe --rodada <ID_ou_PATH> [--debug]
"""

import os
import re
import argparse
import math
import numpy as np
import pandas as pd

# --------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Consenso de odds com padronização de esquema")
    p.add_argument("--rodada", required=True, help="ID da rodada (ex: 1829...) OU caminho data/out/<ID>")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()

# --------- Utils ----------
def _is_id_like(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{6,}", str(s)))

def _out_dir(rodada: str) -> str:
    return rodada if rodada.startswith("data/") else os.path.join("data", "out", str(rodada))

def _log(debug, *msg):
    if debug:
        print("[consensus]", *msg)

def _first_present(*paths):
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan

def _harmonic_mean(arr):
    arr = np.array([_to_float(a) for a in arr if _to_float(a) > 0], dtype=float)
    if arr.size == 0 or np.any(arr <= 0) or np.any(~np.isfinite(arr)):
        return np.nan
    return len(arr) / np.sum(1.0 / arr)

def _std_columns(df: pd.DataFrame, debug=False, source_name="") -> pd.DataFrame:
    """
    Normaliza qualquer esquema de odds para:
      team_home, team_away, odds_home, odds_draw, odds_away, source
    Regras de mapeamento tentam cobrir variações comuns.
    """
    cols_lower = {c.lower(): c for c in df.columns}

    def pick(names):
        for n in names:
            if n in cols_lower:
                return cols_lower[n]
        return None

    # times
    c_home = pick(["team_home", "home", "home_team", "time_mandante", "mandante"])
    c_away = pick(["team_away", "away", "away_team", "time_visitante", "visitante"])

    # odds
    c_oh = pick(["odds_home", "home_odds", "price_home", "h2h_home", "homeprice", "homeprice_decimal"])
    c_od = pick(["odds_draw", "draw_odds", "price_draw", "h2h_draw", "drawprice", "drawprice_decimal", "empate_odds"])
    c_oa = pick(["odds_away", "away_odds", "price_away", "h2h_away", "awayprice", "awayprice_decimal"])

    # alguns arquivos trazem apenas 'odds' em nested markets; tentar colunas genéricas
    # se não achou, tentar nomes curtos
    if c_oh is None: c_oh = pick(["home_price", "homeodd", "odd_home"])
    if c_od is None: c_od = pick(["draw_price", "drawodd", "odd_draw", "x_price", "x_odds"])
    if c_oa is None: c_oa = pick(["away_price", "awayodd", "odd_away"])

    # se ainda faltar algo essencial, abortar com erro informativo
    missing = []
    if c_home is None: missing.append("team_home (home/home_team)")
    if c_away is None: missing.append("team_away (away/away_team)")
    if c_oh is None:   missing.append("odds_home")
    if c_od is None:   missing.append("odds_draw")
    if c_oa is None:   missing.append("odds_away")
    if missing:
        raise ValueError(f"[consensus] fonte '{source_name}' sem colunas necessárias: {missing}")

    out = pd.DataFrame({
        "team_home": df[c_home].astype(str).str.strip(),
        "team_away": df[c_away].astype(str).str.strip(),
        "odds_home": pd.to_numeric(df[c_oh], errors="coerce"),
        "odds_draw": pd.to_numeric(df[c_od], errors="coerce"),
        "odds_away": pd.to_numeric(df[c_oa], errors="coerce"),
    })
    out["source"] = source_name or "unknown"
    # descartando linhas inválidas
    out = out.dropna(subset=["team_home", "team_away", "odds_home", "odds_draw", "odds_away"])
    return out

def _read_flex_csv(path, source_name, debug=False):
    if not path or not os.path.exists(path):
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])
    try:
        raw = pd.read_csv(path)
        if raw.empty:
            _log(debug, f"arquivo vazio: {path}")
            return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])
        std = _std_columns(raw, debug=debug, source_name=source_name)
        _log(debug, f"{source_name} -> linhas válidas: {len(std)}")
        return std
    except Exception as e:
        _log(debug, f"falha lendo {source_name}: {e}")
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])

def _consolidate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida múltiplas linhas do mesmo jogo/fonte. Estratégia:
      - agrupa por (team_home, team_away, source)
      - odds = média harmônica (mais adequada para preços)
      - depois agrupa por (team_home, team_away) cruzando fontes -> média harmônica novamente
    """
    if df.empty:
        return df

    lvl1 = (df
            .groupby(["team_home","team_away","source"], as_index=False)
            .agg({
                "odds_home": _harmonic_mean,
                "odds_draw": _harmonic_mean,
                "odds_away": _harmonic_mean
            }))

    # agora agregando across fontes (mantendo 'source' como 'consensus' na saída final)
    lvl2 = (lvl1
            .groupby(["team_home","team_away"], as_index=False)
            .agg({
                "odds_home": _harmonic_mean,
                "odds_draw": _harmonic_mean,
                "odds_away": _harmonic_mean
            }))
    lvl2["source"] = "consensus"
    return lvl2[["team_home","team_away","odds_home","odds_draw","odds_away","source"]]

# --------- Main ----------
def main():
    args = parse_args()
    out_dir = _out_dir(args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    path_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    path_apifoot = os.path.join(out_dir, "odds_apifootball.csv")

    _log(args.debug, "rodada:", out_dir)
    _log(args.debug, "buscando fontes:", path_theodds, path_apifoot)

    df_t = _read_flex_csv(path_theodds, "theoddsapi", debug=args.debug)
    df_a = _read_flex_csv(path_apifoot, "apifootball", debug=args.debug)

    # se nenhuma fonte disponível, erro controlado:
    if df_t.empty and df_a.empty:
        print("[consensus] AVISO: arquivo não encontrado: ", path_apifoot)
        raise SystemExit("[consensus] ERRO: nenhuma fonte de odds disponível.")

    df_all = pd.concat([df_t, df_a], ignore_index=True)
    df_all = df_all.dropna(subset=["team_home","team_away","odds_home","odds_draw","odds_away"])

    # consolidar
    df_cons = _consolidate(df_all)
    if df_cons.empty:
        raise SystemExit("[consensus] ERRO: após consolidação, não há odds válidas.")

    # construir match_id padronizado
    df_cons["match_id"] = (df_cons["team_home"].str.strip() + "__" + df_cons["team_away"].str.strip())

    # ordena para ficar previsível
    df_cons = df_cons[["match_id","team_home","team_away","odds_home","odds_draw","odds_away","source"]]
    df_cons = df_cons.sort_values(by=["team_home","team_away"]).reset_index(drop=True)

    out_path = os.path.join(out_dir, "odds_consensus.csv")
    df_cons.to_csv(out_path, index=False)
    print(f"[consensus] OK -> {out_path}")
    if args.debug:
        print(df_cons.head(20))

if __name__ == "__main__":
    main()