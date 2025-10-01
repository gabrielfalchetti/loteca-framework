from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class KellyConfig:
    bankroll: float = 1000.0
    kelly_fraction: float = 0.5      # fração da Kelly (0..1)
    kelly_cap: float = 0.10          # teto de Kelly por aposta (ex.: 0.10 = 10% do bankroll)
    min_stake: float = 0.0           # valor mínimo absoluto por aposta (0 desativa)
    max_stake: float = 0.0           # valor máximo absoluto por aposta (0 desativa)
    round_to: float = 1.0            # arredondar stake para múltiplos (ex.: 1 = inteiro, 0 desativa)
    top_n: int = 14                  # quantos picks priorizar/publicar

    def clamp_stake(self, stake: float) -> float:
        """Aplica limites de stake (min/max) e arredondamento."""
        s = max(stake, self.min_stake) if self.min_stake > 0 else stake
        if self.max_stake > 0:
            s = min(s, self.max_stake)
        if self.round_to and self.round_to > 0:
            s = round(s / self.round_to) * self.round_to
        # Evita -0.0 por arredondamento
        if abs(s) < 1e-12:
            s = 0.0
        return s


def kelly_fraction_for(prob: float, odds_decimal: float) -> float:
    """
    Retorna a fração de Kelly (0..1) para um resultado com probabilidade 'prob' e odds decimais.
    Fórmula: k = (p*o - 1)/(o - 1). Valores negativos => 0.
    """
    if odds_decimal is None or odds_decimal <= 1.0:
        return 0.0
    if prob is None or not (0.0 < prob < 1.0):
        return 0.0
    edge = prob * odds_decimal - 1.0
    denom = odds_decimal - 1.0
    if denom <= 0:
        return 0.0
    k = edge / denom
    if k <= 0:
        return 0.0
    return float(k)


def stake_from_kelly(prob: float, odds_decimal: float, cfg: KellyConfig) -> Tuple[float, float, float]:
    """
    Calcula (stake, kelly_f, edge) para um único resultado.
      - stake já capado, fracionado (cfg.kelly_fraction) e arredondado segundo cfg.
      - kelly_f é a fração "cheia" (antes de aplicar fraction e cap), no intervalo [0..1].
      - edge = p*o - 1.
    """
    if odds_decimal is None or odds_decimal <= 1.0 or prob is None:
        return 0.0, 0.0, 0.0

    k_full = kelly_fraction_for(prob, odds_decimal)  # 0..1
    edge = prob * odds_decimal - 1.0

    if k_full <= 0.0:
        return 0.0, 0.0, edge

    k_capped = min(k_full, max(cfg.kelly_cap, 0.0))
    k_effective = k_capped * max(min(cfg.kelly_fraction, 1.0), 0.0)

    stake_raw = cfg.bankroll * k_effective
    stake = cfg.clamp_stake(stake_raw)

    return stake, k_full, edge