# scripts/kelly.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

@dataclass
class KellyConfig:
    bankroll: float = 1000.0
    kelly_fraction: float = 0.5
    kelly_cap: float = 0.10
    min_stake: float = 0.0
    max_stake: float = 0.0
    round_to: float = 1.0
    top_n: Optional[int] = None  # limitar ao top-N de edge, se desejado

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def stake_from_kelly(prob: float, odd: float, cfg: KellyConfig) -> float:
    """
    prob: probabilidade do evento (0..1)
    odd: odd decimal
    retorna stake sugerida
    """
    if odd <= 1.0 or prob <= 0.0 or prob >= 1.0:
        return 0.0
    b = odd - 1.0
    q = 1.0 - prob
    k = (b * prob - q) / b  # Kelly plena
    k = k * cfg.kelly_fraction
    if cfg.kelly_cap > 0.0:
        k = _clip(k, 0.0, cfg.kelly_cap)
    stake = cfg.bankroll * max(0.0, k)
    if cfg.max_stake > 0.0:
        stake = min(stake, cfg.max_stake)
    if cfg.min_stake > 0.0 and stake > 0.0:
        stake = max(stake, cfg.min_stake)
    if cfg.round_to and cfg.round_to > 0:
        # arredonda para múltiplos de round_to
        stake = round(stake / cfg.round_to) * cfg.round_to
    return stake
