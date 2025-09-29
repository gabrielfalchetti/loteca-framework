#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera λ_home/λ_away e probabilidades 1/X/2 via Poisson independente (Skellam por soma truncada).
Entrada: data/out/<rodada>/xg.csv  (ou fallback para features_base.csv)
Saída:   data/out/<rodada>/preds_bivar.csv
"""

from __future__ import annotations
import argparse
import os
import sys
import math
import pandas as pd
from typing import Tuple

def _poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)

def _grid_probs(lh: float, la: float, max_goals: int = None) -> Tuple[float, float, float]:
    """
    Calcula P(home), P(draw), P(away) somando as massas de probabilidade
    em uma grade (gols_home x gols_away).
    """
    if max_goals is None:
        # grade adaptativa: segura e barata
        max_goals = int(min(18, max(10, math.ceil(lh + la + 8))))
    ph = [_poisson_pmf(i, lh) for i in range(max_goals + 1)]
    pa = [_poisson_pmf(j, la) for j in range(max_goals + 1)]

    p_home = 0.0
    p_draw = 0.0
    p_away = 0.0
    # soma dupla
    for i, p_i in enumerate(ph):
        for j, p_j in enumerate(pa):
            pij = p_i * p_j
            if i > j:
                p_home += pij
            elif i == j:
                p_draw += pij
            else:
                p_away += pij

    # pequena correção de normalização por truncamento (distribuir resto proporcionalmente)
    s = p_home + p_draw + p_away
    if s > 0:
        p_home /= s
        p_draw /= s
        p_away /= s

    return p_home, p_draw, p_away

def _find_cols(df: pd.DataFrame) -> Tuple[str, str, str]:
    """
    Tenta identificar colunas padrão: match_id, xg_home, xg_away (ou variantes).
    Levanta erro claro se não encontrar.
    """
    # match_id
    mid = None
    for c in df.columns:
        lc = c.lower()
        if lc in ("match_id", "id_partida", "id", "partida_id"):
            mid = c
            break

    # xg home / away
    cand_home = [c for c in df.columns if c.lower() in ("xg_home","xh","home_xg","xgh","lambda_home","lambda_h","lh")]
    cand_away = [c for c in df.columns if c.lower() in ("xg_away","xa","away_xg","xga","lambda_away","lambda_a","la")]

    col_h = cand_home[0] if cand_home else None
    col_a = cand_away[0] if cand_away else None

    if mid is None:
        raise ValueError("Não encontrei coluna de match_id no arquivo de entrada.")
    if col_h is None or col_a is None:
        raise ValueError("Não encontrei colunas de xG (home/away). Esperado algo como xg_home/xg_away.")

    return mid, col_h, col_a

def _read_best_source(base_dir: str) -> pd.DataFrame:
    """
    Prioriza xg.csv; se não existir, tenta features_base.csv com colunas xg_*.
    """
    xg_path = os.path.join(base_dir, "xg.csv")
    fb_path = os.path.join(base_dir, "features_base.csv")

    if os.path.exists(xg_path):
        df = pd.read_csv(xg_path)
        df["_source"] = "xg"
        return df
    if os.path.exists(fb_path):
        df = pd.read_csv(fb_path)
        df["_source"] = "features_base"
        # tentar deduzir/normalizar nomes
        # não alteramos aqui; a detecção ocorre em _find_cols
        return df

    raise FileNotFoundError("Nenhum arquivo de entrada encontrado. Esperado xg.csv ou features_base.csv.")

def main():
    ap = argparse.ArgumentParser(description="Poisson bivariado (indep.) -> preds_bivar.csv")
    ap.add_argument("--rodada", required=True, help="Identificador da rodada (ex.: 2025-09-27_1213)")
    ap.add_argument("--out", default=None, help="Caminho de saída para preds_bivar.csv")
    ap.add_argument("--max_goals", type=int, default=None, help="Grade máxima de gols (padrão adaptativo)")
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    out_path = args.out or os.path.join(out_dir, "preds_bivar.csv")

    # lê melhor fonte
    src = _read_best_source(out_dir)
    mid, ch, ca = _find_cols(src)

    # opcional: tentar trazer nomes dos times (para debug/legibilidade)
    home_col = away_col = None
    try:
        matches = pd.read_csv(os.path.join(out_dir, "matches.csv"))
        # heurística de colunas
        m_mid = [c for c in matches.columns if c.lower() == "match_id"]
        m_home = [c for c in matches.columns if c.lower() in ("home","mandante","time_casa")]
        m_away = [c for c in matches.columns if c.lower() in ("away","visitante","time_fora")]
        if m_mid and m_home and m_away:
            matches = matches[[m_mid[0], m_home[0], m_away[0]]].rename(
                columns={m_mid[0]: "match_id", m_home[0]:"home", m_away[0]:"away"}
            )
            src = src.merge(matches, left_on=mid, right_on="match_id", how="left")
            home_col, away_col = "home", "away"
    except Exception:
        pass  # segue sem nomes

    rows = []
    for _, r in src.iterrows():
        lh = float(max(0.0, r[ch]))
        la = float(max(0.0, r[ca]))
        p1, px, p2 = _grid_probs(lh, la, args.max_goals)

        row = {
            "match_id": r[mid],
            "lambda_home": lh,
            "lambda_away": la,
            "p1": p1,
            "px": px,
            "p2": p2,
        }
        if home_col and away_col:
            row["home"] = r.get(home_col, None)
            row["away"] = r.get(away_col, None)
        rows.append(row)

    out = pd.DataFrame(rows)
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[poisson_bivar] OK -> {out_path} ({len(out)} linhas)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[poisson_bivar] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
