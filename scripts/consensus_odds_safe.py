#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, math, csv
import pandas as pd
import numpy as np

"""
Consenso de odds (tolerante a faltar um provedor):
- Lê data/out/<RODADA>/odds_theoddsapi.csv (OBRIGATÓRIO ter pelo menos 1 linha válida)
- Opcionalmente lê data/out/<RODADA>/odds_apifootball.csv (se existir)
- Faz o "consenso" por média simples das odds disponíveis (ou "pass-through" se só houver 1 fonte)
- Mantém apenas linhas com pelo menos 2 odds válidas (>1.0) em HOME/DRAW/AWAY
- Salva em data/out/<RODADA>/odds_consensus.csv
"""

REQ_COLS = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]

def valid_odd(x):
    return isinstance(x, (int,float,np.floating)) and x > 1.0 and np.isfinite(x)

def load_csv(path, label):
    if not os.path.exists(path):
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {path}")
        return None
    df = pd.read_csv(path)
    # garante colunas
    miss = [c for c in REQ_COLS if c not in df.columns]
    if miss:
        raise SystemExit(f"[consensus-safe] ERRO: colunas ausentes em {label}: {miss}")
    # normaliza tipos
    for c in ["odds_home","odds_draw","odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def combine_sources(dfs):
    # Junta por match_key mantendo nomes/ordem
    base = None
    for label, df in dfs.items():
        if df is None or df.empty: 
            continue
        sub = df[REQ_COLS].copy()
        sub = sub.add_suffix(f"__{label}")
        sub = sub.rename(columns={f"match_key__{label}":"match_key",
                                  f"team_home__{label}":"team_home__"+label,
                                  f"team_away__{label}":"team_away__"+label})
        if base is None:
            base = sub
        else:
            base = pd.merge(base, sub, on="match_key", how="outer")
    if base is None:
        return pd.DataFrame(columns=REQ_COLS)
    # Reconstroi team_home/away (prioridade theoddsapi depois apifootball)
    def first_nonnull(cols, row):
        for c in cols:
            v = row.get(c, None)
            if isinstance(v, str) and v.strip():
                return v
        return None
    out_rows = []
    for _, row in base.iterrows():
        team_home = first_nonnull([c for c in base.columns if c.startswith("team_home__")], row) or ""
        team_away = first_nonnull([c for c in base.columns if c.startswith("team_away__")], row) or ""
        # colete odds disponíveis
        o_home, o_draw, o_away = [], [], []
        for label in dfs.keys():
            h = row.get(f"odds_home__{label}", np.nan)
            d = row.get(f"odds_draw__{label}", np.nan)
            a = row.get(f"odds_away__{label}", np.nan)
            if valid_odd(h): o_home.append(h)
            if valid_odd(d): o_draw.append(d)
            if valid_odd(a): o_away.append(a)
        # média simples das disponíveis
        cons_home = float(np.mean(o_home)) if len(o_home)>0 else np.nan
        cons_draw = float(np.mean(o_draw)) if len(o_draw)>0 else np.nan
        cons_away = float(np.mean(o_away)) if len(o_away)>0 else np.nan

        # critério de validade: pelo menos 2 odds válidas no total
        valid_count = sum([valid_odd(cons_home), valid_odd(cons_draw), valid_odd(cons_away)])
        if valid_count >= 2:
            out_rows.append({
                "match_key": row["match_key"],
                "team_home": team_home,
                "team_away": team_away,
                "odds_home": cons_home,
                "odds_draw": cons_draw,
                "odds_away": cons_away,
            })
    return pd.DataFrame(out_rows, columns=REQ_COLS)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    p_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    p_apifoot  = os.path.join(out_dir, "odds_apifootball.csv")

    df_the = load_csv(p_theodds, "theoddsapi")
    df_rap = load_csv(p_apifoot, "apifootball")

    if df_the is not None and not df_the.empty:
        print(f"[consensus-safe] lido odds_theoddsapi.csv -> {len(df_the)} linhas")
        inv_reasons = {"menos_de_duas_odds":0}
        # estatística rápida (opcional)
        for _, r in df_the.iterrows():
            v = sum(valid_odd(r[c]) for c in ["odds_home","odds_draw","odds_away"])
            if v < 2: inv_reasons["menos_de_duas_odds"] += 1
        if inv_reasons["menos_de_duas_odds"]>0:
            print(f"[consensus-safe] motivos inválidos theoddsapi: {inv_reasons}")

    dfs = {}
    if df_the is not None and not df_the.empty:
        dfs["theoddsapi"] = df_the
    if df_rap is not None and not df_rap.empty:
        dfs["apifootball"] = df_rap

    df_out = combine_sources(dfs)
    if df_out.empty:
        total = sum((0 if d is None else len(d)) for d in dfs.values())
        print(f"[consensus-safe] consenso bruto: 0 (soma linhas válidas dos provedores); finais (>=2 odds > 1.0): 0")
        raise SystemExit("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")

    save_path = os.path.join(out_dir, "odds_consensus.csv")
    df_out.to_csv(save_path, index=False, float_format="%.6f")
    print(f"[consensus-safe] OK -> {save_path} ({len(df_out)} linhas) | mapping theoddsapi: team_home='team_home', team_away='team_away', match_key='match_key', odds_home='odds_home', odds_draw='odds_draw', odds_away='odds_away'")

if __name__ == "__main__":
    main()
