# scripts/kelly.py
# -*- coding: utf-8 -*-
"""
Utilitários para cálculo de Kelly em apostas 1X2 com odds decimais.

Fórmula base (odds decimais):
  b = o - 1
  k = (p * (b + 1) - 1) / b = (p * o - 1) / (o - 1)
onde:
  p = probabilidade do evento (0..1)
  o = odd decimal (>1)
Retorna fração do bankroll a alocar no resultado. Negativos => 0.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict
import math

@dataclass
class KellyConfig:
    bankroll: float = 1000.0           # bankroll total (dinheiro disponível)
    kelly_fraction: float = 0.5        # fração da Kelly (1.0 = Kelly cheia, 0.5 = meia-Kelly)
    kelly_cap: float = 0.1             # cap por aposta (máx. 10% do bankroll)
    min_stake: float = 0.0             # aposta mínima absoluta
    max_stake: Optional[float] = None  # aposta máxima absoluta (None = sem teto)
    round_to: float = 1.0              # arredondar aposta para múltiplos (ex.: 1.0 => inteiro)

def kelly_fraction_single(p: float, o: float) -> float:
    """
    Fração de Kelly pura (0..1) para odds decimais.
    Se p <= 0 ou o <= 1, retorna 0.
    """
    if p <= 0.0 or o <= 1.0:
        return 0.0
    b = o - 1.0
    k = (p * o - 1.0) / b
    if not math.isfinite(k) or k <= 0.0:
        return 0.0
    return float(k)

def stake_from_kelly(p: float, o: float, cfg: KellyConfig) -> Dict[str, float]:
    """
    Calcula stake sugerida a partir de p, o e configuração Kelly.
    Retorna dict com: kelly_raw, kelly_used, stake_raw, stake_rounded, ev, roi
    """
    k_raw = kelly_fraction_single(p, o)          # Kelly pura
    k_used = min(k_raw * cfg.kelly_fraction, cfg.kelly_cap)
    k_used = max(0.0, k_used)

    stake = cfg.bankroll * k_used
    if cfg.max_stake is not None:
        stake = min(stake, cfg.max_stake)
    stake = max(stake, cfg.min_stake)

    # arredondamento
    if cfg.round_to and cfg.round_to > 0:
        stake_rounded = math.floor(stake / cfg.round_to + 1e-12) * cfg.round_to
    else:
        stake_rounded = stake

    # EV e ROI (esperança por unidade apostada)
    ev = p * o - 1.0          # lucro esperado por 1 unidade apostada
    roi = ev                  # alias semântico

    return {
        "kelly_raw": k_raw,
        "kelly_used": k_used,
        "stake_raw": stake,
        "stake_rounded": stake_rounded,
        "ev": ev,
        "roi": roi,
    }