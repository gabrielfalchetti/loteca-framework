#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
compute_xg.py
-------------
Gera um CSV simples de "xg" por partida a partir de features_base.csv.

Compatibilidades e decisões:
- Lê data/out/{rodada}/features_base.csv (gerado pelo join_features.py).
- Aceita odds nos formatos (k1, kx, k2) OU (home_price, draw_price, away_price).
- Se houver p1/pX/p2 já calculados no features_base.csv, usa-os.
- Caso não haja probabilidades, tenta derivar de odds decimais (1/odd) e normaliza (desvig).
- Converte prob. 1X2 em uma proxy de 'xg' com total_gols ~ 2.40 (ajustável via --total-goals).
  * xg_home = total * (p1 + 0.5*pX)
  * xg_away = total * (p2 + 0.5*pX)
  * xg_draw = pX (mantido como coluna para compatibilidade a jusante)
- Exporta: match_id, home_team, away_team, xg_home, xg_draw, xg_away

Observação: isto é uma aproximação rápida para não quebrar o pipeline.
Se quiser um modelo Poisson/Skellam calibrado, podemos plugar depois.
"""

import os
import argparse
import numpy as np
import pandas as pd


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID da rodada (ex.: 2025-09-27_1213)")
    ap.add_argument("--out", default=None, help="Caminho de saída do xg.csv")
    ap.add_argument("--total-goals", type=float, default=2.40, help="Média de gols por jogo usada na proxy de xG")
    return ap.parse_args()


def to_prob_from_odds(k):
    if k is None:
        return np.nan
    try:
        k = float(k)
        return 1.0 / k if k > 0 else np.nan
    except Exception:
        return np.nan


def desvig(p1, pX, p2):
    vec = np.array([p1, pX, p2], dtype=float)
    if np.isnan(vec).any():
        return p1, pX, p2
    s = vec.sum()
    if s <= 0:
        return p1, pX, p2
    return tuple(vec / s)


def main():
    args = parse_args()
    rodada = args.rodada
    outdir = f"data/out/{rodada}"
    os.makedirs(outdir, exist_ok=True)

    feats_path = f"{outdir}/features_base.csv"
    if not os.path.exists(feats_path):
        raise FileNotFoundError(f"[xg] Não encontrei {feats_path}. Rode join_features antes.")

    df = pd.read_csv(feats_path)

    # Garante colunas de times
    if "home_team" not in df.columns or "away_team" not in df.columns:
        # fallback para nomes antigos (se existirem)
        if "home" in df.columns and "away" in df.columns:
            df["home_team"] = df["home"]
            df["away_team"] = df["away"]
        else:
            # cria colunas vazias para não quebrar
            df["home_team"] = df.get("home", "")
            df["away_team"] = df.get("away", "")

    # Extrai probabilidades:
    # 1) Se já existem p1/pX/p2 calculadas no features_base, usa.
    have_probs = all(c in df.columns for c in ["p1", "pX", "p2"])
    if not have_probs:
        # 2) Caso contrário, tenta derivar de odds:
        #    Primeiro tenta novo esquema (home_price/draw_price/away_price), senão k1/kx/k2.
        if set(["home_price", "draw_price", "away_price"]).issubset(df.columns):
            p1_raw = df["home_price"].apply(to_prob_from_odds)
            pX_raw = df["draw_price"].apply(to_prob_from_odds)
            p2_raw = df["away_price"].apply(to_prob_from_odds)
        else:
            # esquema antigo k1/kx/k2
            for c in ["k1", "kx", "k2"]:
                if c not in df.columns:
                    df[c] = np.nan
            p1_raw = df["k1"].apply(to_prob_from_odds)
            pX_raw = df["kx"].apply(to_prob_from_odds)
            p2_raw = df["k2"].apply(to_prob_from_odds)

        # remove vigorish (normaliza)
        p1_list, pX_list, p2_list = [], [], []
        for a, b, c in zip(p1_raw, pX_raw, p2_raw):
            p1n, pXn, p2n = desvig(a, b, c)
            p1_list.append(p1n)
            pX_list.append(pXn)
            p2_list.append(p2n)
        df["p1"], df["pX"], df["p2"] = p1_list, pX_list, p2_list

    # Calcula xg usando a proxy com média de gols
    total = float(args.total_goals)
    def row_to_xg(r):
        p1 = r.get("p1", np.nan)
        pX = r.get("pX", np.nan)
        p2 = r.get("p2", np.nan)
        if any(pd.isna([p1, pX, p2])):
            return np.nan, np.nan, np.nan
        xg_home = total * (p1 + 0.5 * pX)
        xg_away = total * (p2 + 0.5 * pX)
        xg_draw = pX  # mantemos como referência/compatibilidade
        return xg_home, xg_draw, xg_away

    xg_home, xg_draw, xg_away = [], [], []
    for _, r in df.iterrows():
        h, d, a = row_to_xg(r)
        xg_home.append(h)
        xg_draw.append(d)
        xg_away.append(a)

    out = pd.DataFrame({
        "match_id": df.get("match_id", pd.Series(dtype=str)),
        "home_team": df["home_team"],
        "away_team": df["away_team"],
        "xg_home": xg_home,
        "xg_draw": xg_draw,
        "xg_away": xg_away,
    })

    out_path = args.out or f"{outdir}/xg.csv"
    out.to_csv(out_path, index=False)
    print(f"[xg] OK -> {out_path} ({len(out)} linhas)")


if __name__ == "__main__":
    main()
