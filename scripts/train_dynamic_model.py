#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
train_dynamic_model.py
----------------------
Treina um modelo dinâmico de forças de ataque/defesa por time para futebol,
adequado ao uso com Poisson Bivariado (núcleo preditivo do pipeline).

Interface (compatível com o workflow):
    python -m scripts.train_dynamic_model \
        --rodada data/out/<RUN_ID> \
        --history data/history/results.csv \
        --features data/history/features.parquet \
        --ewma 0.20 \
        [--model_type poisson|dixon_coles|negative_binomial]

Saída:
    <OUT_DIR>/state_params.json

Formato do JSON:
{
  "meta": {
    "model_type": "poisson" | "dixon_coles" | "negative_binomial",
    "mu": float,
    "home_adv": float,
    "gamma": float | null,         # apenas p/ dixon_coles
    "dispersion": float | null,    # apenas p/ negative_binomial
    "updated_at": "YYYY-MM-DDTHH:MM:SSZ",
    "teams": int,
    "notes": "..."
  },
  "teams": {
    "<TeamName>": {"attack": float, "defense": float, "last_update": "YYYY-MM-DD"}
  }
}

Modelagem (resumo):
- Poisson bivariado com log-linhas:
    log λ_home = mu + home_adv + a_home - d_away
    log λ_away = mu + a_away - d_home
- a_• e d_• evoluem no tempo por um filtro recursivo dirigido pelo score
  (y - λ) com passo ~ EWMA (Kalman-like, estável e leve).
- Para Dixon–Coles, estimamos γ por grid-search rápido nos placares baixos.
- Para Negativa Binomial, estimamos a dispersão (k) por método dos momentos.

Requisitos:
    pandas, numpy, scipy, (opcionalmente pyarrow para ler parquet)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
from collections import defaultdict
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd
from scipy.stats import poisson


# ------------------------- Utilidades I/O ------------------------- #

def _read_features_any(path: str) -> pd.DataFrame | None:
    """
    Lê parquet, caindo para CSV se pyarrow/fastparquet não estiver disponível,
    ou se o arquivo não existir. Retorna None se nada for lido.
    """
    if not path:
        return None
    if not os.path.exists(path):
        # tenta um .csv com mesmo prefixo
        alt_csv = os.path.splitext(path)[0] + ".csv"
        if os.path.exists(alt_csv):
            try:
                return pd.read_csv(alt_csv)
            except Exception:
                return None
        return None
    # tenta parquet
    try:
        return pd.read_parquet(path)
    except Exception:
        # fallback para csv com mesmo nome
        try:
            return pd.read_csv(path)
        except Exception:
            # tenta um .csv alternativo
            alt_csv = os.path.splitext(path)[0] + ".csv"
            if os.path.exists(alt_csv):
                try:
                    return pd.read_csv(alt_csv)
                except Exception:
                    return None
            return None


def _ensure_dir(p: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)


# --------------------- Preparação do histórico -------------------- #

def _coerce_history(df: pd.DataFrame) -> pd.DataFrame:
    keep = [
        "date", "league", "season",
        "home", "away", "home_goals", "away_goals", "match_id"
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = "" if c not in ("home_goals", "away_goals") else 0

    df["date"] = df["date"].astype(str)
    df["home"] = df["home"].astype(str)
    df["away"] = df["away"].astype(str)
    df["home_goals"] = pd.to_numeric(df["home_goals"], errors="coerce").fillna(0).astype(int)
    df["away_goals"] = pd.to_numeric(df["away_goals"], errors="coerce").fillna(0).astype(int)

    # ordena por data para consistência temporal
    try:
        df["_d"] = pd.to_datetime(df["date"], errors="coerce")
    except Exception:
        df["_d"] = pd.NaT
    df = df.sort_values(by=["_d", "league", "home", "away"]).drop(columns=["_d"]).reset_index(drop=True)
    return df


def _baseline_params(df: pd.DataFrame) -> Tuple[float, float]:
    """
    Estima mu e home_adv a partir das médias empíricas do histórico.
    - mu = log(mean_away_goals)
    - home_adv = log(mean_home_goals) - log(mean_away_goals)
    """
    if len(df) == 0:
        # defaults conservadores
        return math.log(1.0), math.log(1.2)  # mu ~ 1 gol fora; HA ~ 20%
    mh = df["home_goals"].mean()
    ma = df["away_goals"].mean()
    mh = float(max(mh, 0.05))
    ma = float(max(ma, 0.05))
    mu = math.log(ma)
    home_adv = math.log(mh) - math.log(ma)
    return mu, home_adv


# ------------------------ Filtro Dinâmico ------------------------- #

class DynamicTeamState:
    __slots__ = ("attack", "defense", "last_date", "count")

    def __init__(self):
        self.attack = 0.0
        self.defense = 0.0
        self.last_date = None  # string YYYY-MM-DD
        self.count = 0


def _exp_clamp(x: float, lo=-20.0, hi=20.0) -> float:
    """Evita overflow/underflow em exp()"""
    return float(np.clip(x, lo, hi))


def _score_update(
    y: int,
    lam: float,
    param: float,
    sign: float,
    k: float,
) -> float:
    """
    Atualiza um parâmetro (ataque ou defesa) pelo score de Poisson.
    - y ~ Poisson(lam)
    - derivada de log verossimilhança wrt log(lam) = y - lam
    - 'sign' define o papel do parâmetro (+1 se soma, -1 se subtrai)
    - k é o ganho (similar ao Kalman gain, ~ EWMA)
    """
    # incremento no espaço do log-lambda
    grad = (y - lam) * sign
    return param + k * grad


def fit_dynamic_states(
    df: pd.DataFrame,
    mu: float,
    home_adv: float,
    ewma_alpha: float = 0.20,
    decay: float = 0.001,
) -> Tuple[Dict[str, DynamicTeamState], List[Tuple[float, float, int, int]]]:
    """
    Percorre o histórico em ordem temporal e atualiza os estados a_t, d_t por time
    via filtro dirigido por score (GAS / Kalman-like).

    Retorna:
        - dict de estados por time
        - lista de tuplas (lam_h_pred, lam_a_pred, y_h, y_a) para análise/diagnóstico
          (predições 'one-step-ahead' antes do update).
    """
    states: Dict[str, DynamicTeamState] = defaultdict(DynamicTeamState)
    # Para cada partida, guardamos as lâmbdas preditas antes do update
    preds: List[Tuple[float, float, int, int]] = []

    # ganho base (limitado para estabilidade)
    k = float(np.clip(ewma_alpha, 0.01, 0.5))

    for _, r in df.iterrows():
        th = r["home"]
        ta = r["away"]
        yh = int(r["home_goals"])
        ya = int(r["away_goals"])
        dstr = str(r["date"])[:10] if isinstance(r["date"], str) else ""

        # obtém estados atuais (antes da atualização)
        sh = states[th]
        sa = states[ta]

        # predição 1-passo-a-frente
        loglam_h = _exp_clamp(mu + home_adv + sh.attack - sa.defense)
        loglam_a = _exp_clamp(mu + sa.attack - sh.defense)
        lam_h = float(np.exp(loglam_h))
        lam_a = float(np.exp(loglam_a))

        # armazena antes de atualizar
        preds.append((lam_h, lam_a, yh, ya))

        # atualiza por score (y - lambda)
        sh.attack = _score_update(yh, lam_h, sh.attack, +1.0, k)
        sa.defense = _score_update(yh, lam_h, sa.defense, -1.0, k)

        sa.attack = _score_update(ya, lam_a, sa.attack, +1.0, k)
        sh.defense = _score_update(ya, lam_a, sh.defense, -1.0, k)

        # pequeno decaimento para evitar drift ilimitado
        if decay > 0.0:
            sh.attack *= (1.0 - decay)
            sh.defense *= (1.0 - decay)
            sa.attack *= (1.0 - decay)
            sa.defense *= (1.0 - decay)

        # clipping leve
        sh.attack = float(np.clip(sh.attack, -3.0, 3.0))
        sh.defense = float(np.clip(sh.defense, -3.0, 3.0))
        sa.attack = float(np.clip(sa.attack, -3.0, 3.0))
        sa.defense = float(np.clip(sa.defense, -3.0, 3.0))

        sh.count += 1
        sa.count += 1
        sh.last_date = dstr or sh.last_date
        sa.last_date = dstr or sa.last_date

    return states, preds


# ----------------- Estimadores adicionais (opcionais) -------------- #

def estimate_dc_gamma(preds: List[Tuple[float, float, int, int]]) -> float:
    """
    Estima o parâmetro γ de Dixon–Coles por grid-search simples,
    maximizando a log-verossimilhança corrigida nos placares baixos.

    Fórmulas do fator τ (Dixon & Coles, 1997):
        (0,0): τ = 1 - γ * λh * λa
        (1,0): τ = 1 + γ * λh
        (0,1): τ = 1 + γ * λa
        (1,1): τ = 1 - γ
    Para demais placares, τ = 1.

    Retorna gamma. Se algo der errado, retorna um default conservador (-0.06).
    """
    if not preds:
        return -0.06

    # pré-calcula as partes independentes de Poisson
    def ll_dc(gamma: float) -> float:
        ll = 0.0
        for lam_h, lam_a, yh, ya in preds:
            # pmfs independentes
            p_ind = poisson.pmf(yh, lam_h) * poisson.pmf(ya, lam_a)
            # τ para correção de baixa contagem
            if yh == 0 and ya == 0:
                tau = max(1.0 - gamma * lam_h * lam_a, 1e-12)
            elif yh == 1 and ya == 0:
                tau = max(1.0 + gamma * lam_h, 1e-12)
            elif yh == 0 and ya == 1:
                tau = max(1.0 + gamma * lam_a, 1e-12)
            elif yh == 1 and ya == 1:
                tau = max(1.0 - gamma, 1e-12)
            else:
                tau = 1.0
            p = max(p_ind * tau, 1e-18)
            ll += math.log(p)
        return ll

    # busca em grade pequena
    grid = np.linspace(-0.20, 0.20, 81)  # passo 0.005
    best_g = None
    best_ll = -1e300
    for g in grid:
        cur = ll_dc(g)
        if cur > best_ll:
            best_ll = cur
            best_g = g

    return float(best_g if best_g is not None else -0.06)


def estimate_negbin_dispersion(preds: List[Tuple[float, float, int, int]]) -> float | None:
    """
    Estima um único parâmetro de dispersão (k) para Negativa Binomial (por lado)
    usando método dos momentos em relação aos resíduos de gols.

    Var[goals] ≈ mean + mean^2 / k  =>  k ≈ mean^2 / (Var - mean)
    Retorna uma média dos k estimados para mandante e visitante (quando Var>mean).
    """
    if not preds:
        return None

    yh_list = []
    ya_list = []
    lamh_list = []
    lama_list = []

    for lam_h, lam_a, yh, ya in preds:
        yh_list.append(yh)
        ya_list.append(ya)
        lamh_list.append(lam_h)
        lama_list.append(lam_a)

    yh_arr = np.array(yh_list, dtype=float)
    ya_arr = np.array(ya_list, dtype=float)
    # usa lambdas estimadas como proxy de mean
    mh = float(np.mean(lamh_list)) if len(lamh_list) else None
    ma = float(np.mean(lama_list)) if len(lama_list) else None

    # variâncias empíricas
    vh = float(np.var(yh_arr)) if yh_arr.size else None
    va = float(np.var(ya_arr)) if ya_arr.size else None

    ks = []
    for m, v in ((mh, vh), (ma, va)):
        if m is None or v is None:
            continue
        if v > m + 1e-9:
            k = (m * m) / (v - m)
            if k > 0:
                ks.append(k)

    if ks:
        return float(np.mean(ks))
    return None


# ------------------------------ MAIN ------------------------------ #

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (saída)")
    ap.add_argument("--history", required=True, help="CSV com resultados históricos (results.csv)")
    ap.add_argument("--features", default="", help="Parquet/CSV com features (opcional)")
    ap.add_argument("--ewma", type=float, default=0.20, help="EWMA alpha do filtro dinâmico (0..1)")
    ap.add_argument(
        "--model_type",
        choices=["poisson", "dixon_coles", "negative_binomial"],
        default="poisson",
        help="Modelo alvo para meta-informação (estrutura de estados é a mesma).",
    )
    args = ap.parse_args()

    out_dir = args.rodada
    hist_path = args.history
    feat_path = args.features
    ewma = float(args.ewma)
    model_type = args.model_type

    # Verificações de entrada
    if not os.path.exists(hist_path):
        print(f"[train][CRITICAL] Histórico não encontrado: {hist_path}")
        return 7

    try:
        df_hist = pd.read_csv(hist_path, dtype=str)
    except Exception as e:
        print(f"[train][CRITICAL] Falha ao ler histórico: {e}")
        return 7

    df_hist = _coerce_history(df_hist)

    # Features são opcionais — podem ser usadas futuramente como covariáveis
    df_feat = _read_features_any(feat_path)
    if df_feat is None:
        print("[train][INFO] Features ausentes ou não lidas — seguindo sem covariáveis.")
    else:
        print(f"[train][INFO] Features carregadas: linhas={len(df_feat)}")

    # Parâmetros base
    mu, home_adv = _baseline_params(df_hist)
    print(f"[train][INFO] mu={mu:.4f}  home_adv={home_adv:.4f}")

    # Ajuste dinâmico (estados por time)
    states, preds = fit_dynamic_states(
        df=df_hist, mu=mu, home_adv=home_adv, ewma_alpha=ewma, decay=0.001
    )
    print(f"[train][INFO] Times com estado: {len(states)}")

    # Estimadores opcionais
    gamma = None
    dispersion = None
    if model_type == "dixon_coles":
        try:
            gamma = estimate_dc_gamma(preds)
            print(f"[train][INFO] Dixon–Coles gamma={gamma:.4f}")
        except Exception as e:
            gamma = -0.06
            print(f"[train][WARN] Falha estimando gamma; usando default {gamma:.2f}. Erro: {e}")

    if model_type == "negative_binomial":
        try:
            dispersion = estimate_negbin_dispersion(preds)
            if dispersion is not None:
                print(f"[train][INFO] NegBin dispersion k≈{dispersion:.3f}")
            else:
                print("[train][INFO] NegBin: não foi possível estimar k (sem overdispersion clara).")
        except Exception as e:
            dispersion = None
            print(f"[train][WARN] Falha estimando dispersão NegBin: {e}")

    # Monta JSON de saída
    out_json = {
        "meta": {
            "model_type": model_type,
            "mu": float(mu),
            "home_adv": float(home_adv),
            "gamma": float(gamma) if gamma is not None else None,
            "dispersion": float(dispersion) if dispersion is not None else None,
            "updated_at": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "teams": int(len(states)),
            "notes": "Estados dinâmicos (ataque/defesa) filtrados por score; prontos para Poisson bivariado."
        },
        "teams": {},
    }

    for team, st in states.items():
        out_json["teams"][team] = {
            "attack": float(st.attack),
            "defense": float(st.defense),
            "last_update": st.last_date if st.last_date else "",
        }

    # Salva
    out_path = os.path.join(out_dir, "state_params.json")
    _ensure_dir(out_path)
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out_json, f, ensure_ascii=False, indent=2)
        print(f"[train][OK] state_params.json salvo em: {out_path}")
    except Exception as e:
        print(f"[train][CRITICAL] Falha ao salvar state_params.json: {e}")
        return 7

    return 0


if __name__ == "__main__":
    sys.exit(main())