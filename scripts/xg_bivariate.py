#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xg_bivariate.py
---------------
Gera probabilidades 1X2 para cada partida da rodada usando o estado dinâmico
salvo em <OUT_DIR>/state_params.json.

Entrada (detecta automaticamente):
  - <OUT_DIR>/odds_consensus.csv (preferido)  [colunas: team_home, team_away, ...]
    *fallback*:
  - <OUT_DIR>/matches_whitelist.csv          [colunas típicas: match_id, home, away]

Chamada (compatível com workflow):
  python -m scripts.xg_bivariate --rodada <OUT_DIR> --max_goals 10

Saída:
  <OUT_DIR>/xg_bivariate.csv  com colunas:
    match_id, team_home, team_away, lambda_home, lambda_away,
    p_home, p_draw, p_away, model_type, gamma, dispersion
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd
from scipy.stats import poisson
from math import lgamma


# -------------------------- Utilidades PMF -------------------------- #

def _poisson_pmf_vec(lam: float, gmax: int) -> np.ndarray:
    """Vetor PMF Poisson[0..gmax]."""
    g = np.arange(gmax + 1)
    return poisson.pmf(g, lam)


def _negbin_pmf_vec(mean: float, k: float, gmax: int) -> np.ndarray:
    """
    PMF da Negativa Binomial parametrizada por (mean, k) com:
      Var = mean + mean^2/k  -> overdispersão controlada por k>0
    p = k/(k+mean), r = k  (usamos forma NB(r=k, p))
    PMF(y) = C(y+k-1,y) * (1-p)^y * p^k
           = exp( logGamma(y+k) - logGamma(k) - logGamma(y+1)
                  + y*log(1-p) + k*log(p) )
    """
    if k is None or k <= 0:
        # fallback para Poisson se k inválido
        return _poisson_pmf_vec(mean, gmax)
    p = k / (k + mean)
    logp = math.log(p)
    log1mp = math.log1p(-p)
    y = np.arange(gmax + 1, dtype=float)
    # usa gammaln p/ estabilidade numérica
    logC = (lgamma(y + k) - lgamma(k) - lgamma(y + 1))
    logpmf = logC + y * log1mp + k * logp
    pmf = np.exp(logpmf)
    # normaliza pequenas perdas numéricas
    s = pmf.sum()
    if s > 0:
        pmf = pmf / s
    return pmf


def _apply_dixon_coles_tau(joint: np.ndarray, lam_h: float, lam_a: float, gamma: float) -> np.ndarray:
    """
    Aplica o fator τ de Dixon–Coles para (0,0),(1,0),(0,1),(1,1) e renormaliza.
    joint: matriz (G+1 x G+1) com independência já aplicada.
    """
    if gamma is None:
        return joint
    G = joint.shape[0] - 1

    # fatores τ
    def tau(yh: int, ya: int) -> float:
        if yh == 0 and ya == 0:
            return max(1.0 - gamma * lam_h * lam_a, 1e-12)
        if yh == 1 and ya == 0:
            return max(1.0 + gamma * lam_h, 1e-12)
        if yh == 0 and ya == 1:
            return max(1.0 + gamma * lam_a, 1e-12)
        if yh == 1 and ya == 1:
            return max(1.0 - gamma, 1e-12)
        return 1.0

    joint = joint.copy()
    for (yh, ya) in [(0, 0), (1, 0), (0, 1), (1, 1)]:
        if yh <= G and ya <= G:
            joint[yh, ya] *= tau(yh, ya)

    s = joint.sum()
    if s > 0:
        joint /= s
    return joint


# ------------------------- Núcleo de previsão ------------------------ #

def _clamp_log(x: float, lo=-20.0, hi=20.0) -> float:
    return float(np.clip(x, lo, hi))


def _lambdas_for_match(mu: float, home_adv: float,
                       a_home: float, d_home: float,
                       a_away: float, d_away: float) -> Tuple[float, float]:
    log_lh = _clamp_log(mu + home_adv + a_home - d_away)
    log_la = _clamp_log(mu + a_away - d_home)
    return float(np.exp(log_lh)), float(np.exp(log_la))


def _prob_1x2_from_joint(joint: np.ndarray) -> Tuple[float, float, float]:
    """
    joint: matriz (G+1 x G+1) com distribuição de gols [yh, ya].
    Retorna (p_home, p_draw, p_away). Renormaliza para somar 1.
    """
    G = joint.shape[0] - 1
    p_home = 0.0
    p_draw = 0.0
    p_away = 0.0
    for yh in range(G + 1):
        row = joint[yh, :]
        # vitórias do mandante: yh > ya
        if yh > 0:
            p_home += row[:min(yh, G) + 0].sum()
        # empate
        p_draw += row[yh] if yh <= G else 0.0
        # vitórias visitante: ya > yh
        if yh < G:
            p_away += row[(yh + 1):].sum()
    total = p_home + p_draw + p_away
    if total > 0:
        p_home /= total
        p_draw /= total
        p_away /= total
    return p_home, p_draw, p_away


def _make_joint(lh: float, la: float, gmax: int,
                model_type: str, gamma: float | None, dispersion: float | None) -> np.ndarray:
    """
    Constrói a matriz conjunta de placares até gmax.
    - Se dispersion (k) presente e model_type == 'negative_binomial', usa NB marginal.
    - Caso contrário, usa Poisson marginal.
    - Aplica correção Dixon–Coles quando gamma não for None.
    """
    if model_type == "negative_binomial" and (dispersion is not None) and dispersion > 0:
        pmf_h = _negbin_pmf_vec(lh, dispersion, gmax)
        pmf_a = _negbin_pmf_vec(la, dispersion, gmax)
    else:
        pmf_h = _poisson_pmf_vec(lh, gmax)
        pmf_a = _poisson_pmf_vec(la, gmax)

    joint = np.outer(pmf_h, pmf_a)

    # Aplica Dixon–Coles se gamma disponível
    if gamma is not None and model_type in ("poisson", "dixon_coles"):
        joint = _apply_dixon_coles_tau(joint, lh, la, gamma)

    # Renormaliza (perdas de cauda ou ajustes numéricos)
    s = joint.sum()
    if s > 0:
        joint = joint / s
    return joint


# --------------------------- Leitura de dados ------------------------ #

def _load_state(out_dir: str) -> Tuple[dict, Dict[str, dict]]:
    p = os.path.join(out_dir, "state_params.json")
    if not os.path.exists(p):
        raise FileNotFoundError(f"state_params.json não encontrado em {out_dir}")
    with open(p, "r", encoding="utf-8") as f:
        js = json.load(f)
    meta = js.get("meta", {})
    teams = js.get("teams", {})
    return meta, teams


def _load_matches(out_dir: str) -> pd.DataFrame:
    """
    Preferência: odds_consensus.csv (com team_home/team_away).
    Fallback: matches_whitelist.csv (home/away ou team_home/team_away).
    """
    p1 = os.path.join(out_dir, "odds_consensus.csv")
    p2 = os.path.join(out_dir, "matches_whitelist.csv")

    if os.path.exists(p1):
        df = pd.read_csv(p1)
        # normaliza nomes de colunas
        cols = {c.lower(): c for c in df.columns}
        th = cols.get("team_home") or cols.get("home")
        ta = cols.get("team_away") or cols.get("away")
        mid = cols.get("match_id")
        if th is None or ta is None:
            raise ValueError("odds_consensus.csv sem colunas team_home/team_away (ou home/away).")
        df_out = pd.DataFrame({
            "match_id": df[mid] if mid is not None else range(1, len(df) + 1),
            "team_home": df[th].astype(str),
            "team_away": df[ta].astype(str),
        })
        return df_out

    if os.path.exists(p2):
        df = pd.read_csv(p2)
        cols = {c.lower(): c for c in df.columns}
        th = cols.get("team_home") or cols.get("home")
        ta = cols.get("team_away") or cols.get("away")
        mid = cols.get("match_id")
        if th is None or ta is None:
            raise ValueError("matches_whitelist.csv sem colunas team_home/team_away (ou home/away).")
        df_out = pd.DataFrame({
            "match_id": df[mid] if mid is not None else range(1, len(df) + 1),
            "team_home": df[th].astype(str),
            "team_away": df[ta].astype(str),
        })
        return df_out

    raise FileNotFoundError("Nenhum arquivo de partidas encontrado (odds_consensus.csv ou matches_whitelist.csv).")


# ------------------------------- MAIN -------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (OUT_DIR)")
    ap.add_argument("--max_goals", type=int, default=10, help="Máximo de gols por lado no grid")
    args = ap.parse_args()

    out_dir = args.rodada
    gmax = int(max(4, args.max_goals))

    try:
        meta, teams_state = _load_state(out_dir)
    except Exception as e:
        print(f"[xg_bivar][CRITICAL] Falha lendo state_params.json: {e}")
        return 8

    model_type = str(meta.get("model_type", "poisson"))
    mu = float(meta.get("mu", math.log(1.0)))
    home_adv = float(meta.get("home_adv", math.log(1.2)))
    gamma = meta.get("gamma", None)
    dispersion = meta.get("dispersion", None)
    gamma = float(gamma) if gamma is not None else None
    dispersion = float(dispersion) if dispersion is not None else None

    try:
        df_matches = _load_matches(out_dir)
    except Exception as e:
        print(f"[xg_bivar][CRITICAL] Falha carregando partidas: {e}")
        return 8

    rows = []
    missing = 0

    for idx, r in df_matches.iterrows():
        match_id = r.get("match_id", idx + 1)
        th = str(r["team_home"])
        ta = str(r["team_away"])

        sh = teams_state.get(th, None)
        sa = teams_state.get(ta, None)

        if sh is None or sa is None:
            missing += 1
            # estados neutros se ausentes
            a_h = d_h = a_a = d_a = 0.0
        else:
            a_h = float(sh.get("attack", 0.0))
            d_h = float(sh.get("defense", 0.0))
            a_a = float(sa.get("attack", 0.0))
            d_a = float(sa.get("defense", 0.0))

        lh, la = _lambdas_for_match(mu, home_adv, a_h, d_h, a_a, d_a)

        joint = _make_joint(lh, la, gmax, model_type, gamma, dispersion)
        p_home, p_draw, p_away = _prob_1x2_from_joint(joint)

        rows.append({
            "match_id": match_id,
            "team_home": th,
            "team_away": ta,
            "lambda_home": round(lh, 6),
            "lambda_away": round(la, 6),
            "p_home": round(p_home, 6),
            "p_draw": round(p_draw, 6),
            "p_away": round(p_away, 6),
            "model_type": model_type,
            "gamma": (None if gamma is None else round(gamma, 6)),
            "dispersion": (None if dispersion is None else round(dispersion, 6)),
        })

    out_df = pd.DataFrame(rows, columns=[
        "match_id", "team_home", "team_away",
        "lambda_home", "lambda_away",
        "p_home", "p_draw", "p_away",
        "model_type", "gamma", "dispersion"
    ])

    out_path = os.path.join(out_dir, "xg_bivariate.csv")
    try:
        out_df.to_csv(out_path, index=False)
    except Exception as e:
        print(f"[xg_bivar][CRITICAL] Falha salvando {out_path}: {e}")
        return 8

    print(f"[xg_bivar][OK] {out_path} gerado. Partidas={len(out_df)}  faltando_estado={missing}")
    return 0


if __name__ == "__main__":
    sys.exit(main())