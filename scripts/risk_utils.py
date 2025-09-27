# scripts/risk_utils.py
from __future__ import annotations
import numpy as np
import pandas as pd

RNG = np.random.default_rng(2025)

def load_prob_matrix(rodada: str) -> tuple[pd.DataFrame, np.ndarray]:
    """
    Carrega a melhor matriz de probabilidades 14x3 na ordem:
      1) joined_stacked_bivar.csv -> (p_home_final,p_draw_final,p_away_final)
      2) joined_stacked.csv       -> (p_home_final,p_draw_final,p_away_final)
      3) joined.csv               -> (p_home,p_draw,p_away)
    Retorna (df, P) onde P é (14,3) normalizado e sem NaN.
    """
    base = f"data/out/{rodada}"
    tried = [
        ("joined_stacked_bivar.csv", ["p_home_final","p_draw_final","p_away_final"]),
        ("joined_stacked.csv",       ["p_home_final","p_draw_final","p_away_final"]),
        ("joined.csv",               ["p_home","p_draw","p_away"]),
    ]
    import os
    for fname, cols in tried:
        path = f"{base}/{fname}"
        if os.path.exists(path) and os.path.getsize(path) > 0:
            df = pd.read_csv(path).rename(columns=str.lower)
            low = [c for c in cols]
            if not set([c.lower() for c in low]).issubset(df.columns):
                continue
            P = df[low].to_numpy(float, copy=True)
            P = np.clip(P, 1e-12, 1.0)
            P /= P.sum(axis=1, keepdims=True)
            if P.shape[0] < 14:
                raise RuntimeError(f"[risk] Esperava 14 jogos, mas vieram {P.shape[0]} em {fname}")
            return df, P[:14]
    raise RuntimeError("[risk] Nenhum arquivo de probabilidades encontrado.")

def simulate_outcomes(P: np.ndarray, n_sims: int = 50000) -> np.ndarray:
    """
    Simula desfechos: retorna matriz (n_sims, 14) com valores em {0,1,2}.
    """
    n = P.shape[0]
    out = np.empty((n_sims, n), dtype=np.int8)
    for i in range(n):
        out[:, i] = RNG.choice(3, size=n_sims, p=P[i])
    return out

def ticket_hits(sim_outcomes: np.ndarray, ticket: list[set[int]]) -> np.ndarray:
    """
    Dado outcomes (n_sims,14) e um ticket representado como lista de sets de escolhas por jogo,
    retorna vetor (n_sims,) com total de acertos no ticket (considera acerto se outcome ∈ escolhas).
    """
    n_sims, n = sim_outcomes.shape
    acc = np.zeros(n_sims, dtype=np.int16)
    for j in range(n):
        choices = ticket[j]
        if not isinstance(choices, set):
            choices = set([choices])
        mask = np.isin(sim_outcomes[:, j], list(choices))
        acc += mask.astype(np.int16)
    return acc

def portfolio_payouts(sim_outcomes: np.ndarray, tickets: list[list[set[int]]], stakes: np.ndarray, pay_table: dict[int, float] | None = None) -> np.ndarray:
    """
    Calcula 'retorno' relativo do portfólio por simulação.
    - tickets: lista de tickets; cada ticket é lista de 14 sets com escolhas (0=home,1=draw,2=away).
    - stakes: peso relativo de cada ticket (soma 1).
    - pay_table: opcional, mapeia #acertos -> payout (unidade). Se None, usa utilidade: hits/14.
    Retorna vetor (n_sims,) de retornos.
    """
    n_sims = sim_outcomes.shape[0]
    ret = np.zeros(n_sims, dtype=float)
    if pay_table is None:
        # utilidade proxy: fracional por acertos (0..1)
        for t, w in zip(tickets, stakes):
            h = ticket_hits(sim_outcomes, t)
            ret += w * (h / 14.0)
        return ret

    # payout real (se fornecido)
    for t, w in zip(tickets, stakes):
        h = ticket_hits(sim_outcomes, t)
        val = np.zeros(n_sims, dtype=float)
        # lookup
        for k, pay in pay_table.items():
            val[h == k] = pay
        ret += w * val
    return ret

def var_es(returns: np.ndarray, alpha: float = 0.95) -> tuple[float, float]:
    """
    VaR e ES (CVaR) no nível alpha para a distribuição de 'returns'.
    Retornos mais baixos = piores cenários.
    """
    r = np.sort(returns)
    idx = int((1.0 - alpha) * (len(r) - 1))
    var = r[idx]
    es = r[:idx+1].mean() if idx >= 0 else r.mean()
    return float(var), float(es)

def kelly_fraction(p: float, b: float) -> float:
    """
    Kelly para aposta binária: p = prob. sucesso, b = odds decimais - 1 (ganho líquido por 1 unid).
    Aqui usamos para alocação proporcional; limite entre 0 e 1.
    """
    f = (p * (b + 1) - 1) / b if b > 0 else 0.0
    return float(max(0.0, min(1.0, f)))
