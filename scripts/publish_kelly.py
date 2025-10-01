#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
publish_kelly.py
----------------
Calcula stakes via Kelly usando odds (consenso) e probabilidades (quando disponíveis),
gera data/out/<RODADA>/kelly_stakes.csv e não quebra o pipeline se dados faltarem.

- Lê:
  data/out/<RODADA>/odds_consensus.csv
  (opcional) predictions_*.csv para probabilidades

- Escreve:
  data/out/<RODADA>/kelly_stakes.csv

Uso:
  python scripts/publish_kelly.py --rodada 2025-09-27_1213 [--debug]

Env esperados (com defaults):
  BANKROLL=1000
  KELLY_FRACTION=0.5
  KELLY_CAP=0.1
  MIN_STAKE=0
  MAX_STAKE=0           (0 = sem teto)
  ROUND_TO=1            (arredondar stake para múltiplos de N; 0 = sem arred.)
  KELLY_TOP_N=14
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ==========================
# Config e utilidades
# ==========================

@dataclass
class KellyConfig:
    bankroll: float = 1000.0
    kelly_fraction: float = 0.5
    kelly_cap: float = 0.10
    min_stake: float = 0.0
    max_stake: float = 0.0  # 0 = sem teto
    round_to: float = 1.0   # 0 = sem arredondamento
    top_n: int = 14


def env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    try:
        return float(v) if v is not None and v != "" else default
    except Exception:
        return default


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    try:
        return int(float(v)) if v is not None and v != "" else default
    except Exception:
        return default


def load_cfg_from_env() -> KellyConfig:
    return KellyConfig(
        bankroll=env_float("BANKROLL", 1000.0),
        kelly_fraction=env_float("KELLY_FRACTION", 0.5),
        kelly_cap=env_float("KELLY_CAP", 0.10),
        min_stake=env_float("MIN_STAKE", 0.0),
        max_stake=env_float("MAX_STAKE", 0.0),
        round_to=env_float("ROUND_TO", 1.0),
        top_n=env_int("KELLY_TOP_N", 14),
    )


def safe_round(x: float, step: float) -> float:
    if step and step > 0:
        return round(x / step) * step
    return x


# ==========================
# Kelly core
# ==========================

def kelly_raw(p: float, o: float) -> float:
    """
    Fórmula de Kelly para odds decimais:
      k = (o*p - (1 - p)) / (o - 1)

    Retorna 0 se p fora de [0,1], o <= 1, ou denominador inválido.
    """
    try:
        if p is None or o is None:
            return 0.0
        if not (0.0 <= p <= 1.0):
            return 0.0
        if o <= 1.0:
            return 0.0
        denom = (o - 1.0)
        if abs(denom) < 1e-12:
            return 0.0
        k = (o * p - (1.0 - p)) / denom
        # Se k < 0, Kelly recomenda não apostar.
        return float(k) if math.isfinite(k) else 0.0
    except Exception:
        return 0.0


def edge_from_p_o(p: float, o: float) -> float:
    """
    Edge (valor esperado unitário): EV = p*o - 1
    Se EV > 1? Para odds decimais justas, stake considera kelly_raw.
    """
    try:
        if p is None or o is None:
            return 0.0
        return float(p * o - 1.0)
    except Exception:
        return 0.0


def stake_from_kelly(p: float, o: float, cfg: KellyConfig) -> Tuple[float, float, float]:
    """
    => SEMPRE retorna (stake, kelly_raw, edge)

    stake >= 0; aplica fraction, cap, min/max, arredondamento.
    """
    kr = kelly_raw(p, o)
    if kr <= 0.0:
        return (0.0, kr, edge_from_p_o(p, o))

    # Aplica cap no Kelly puro
    kr_capped = min(kr, cfg.kelly_cap) if cfg.kelly_cap and cfg.kelly_cap > 0 else kr

    # Fractional Kelly
    k_used = kr_capped * (cfg.kelly_fraction if cfg.kelly_fraction > 0 else 1.0)

    stake = cfg.bankroll * k_used

    # Teto de aposta
    if cfg.max_stake and cfg.max_stake > 0:
        stake = min(stake, cfg.max_stake)

    # Piso de aposta (se 0 => sem piso)
    if stake < (cfg.min_stake or 0.0):
        stake = 0.0

    # Arredondamento
    stake = safe_round(stake, cfg.round_to) if cfg.round_to else stake

    # Corrige -0.0
    stake = 0.0 if abs(stake) < 1e-9 else stake

    return (float(stake), float(kr), edge_from_p_o(p, o))


# ==========================
# Leitura de dados
# ==========================

# Possíveis nomes de colunas
HOME_KEYS = ["team_home", "home_team", "home", "mandante"]
AWAY_KEYS = ["team_away", "away_team", "away", "visitante"]
MATCH_KEYS = ["match_key", "match_id", "fixture_id", "id", "key"]

PROB_HOME_KEYS = ["prob_home", "home_prob", "p_home", "probH", "ph"]
PROB_DRAW_KEYS = ["prob_draw", "draw_prob", "p_draw", "probD", "pd"]
PROB_AWAY_KEYS = ["prob_away", "away_prob", "p_away", "probA", "pa"]

ODDS_HOME_KEYS = ["odds_home", "home_odds", "home_price", "oddsH", "H_odds", "price_home"]
ODDS_DRAW_KEYS = ["odds_draw", "draw_odds", "draw_price", "oddsD", "D_odds", "price_draw"]
ODDS_AWAY_KEYS = ["odds_away", "away_odds", "away_price", "oddsA", "A_odds", "price_away"]

# Estruturas “genéricas” (para arquivos pivotados)
# Ex.: outcome in {"H","D","A"} e "odds" ou "price" numa única coluna.
OUTCOME_KEY_CAND = ["outcome", "market_outcome", "pick", "side"]
PRICE_KEY_CAND = ["odds", "price", "decimal", "decimal_odds"]
PROB_KEY_CAND = ["prob", "probability", "p"]

OUTCOME_MAP = {
    "H": "home",
    "D": "draw",
    "A": "away",
    "home": "home",
    "draw": "draw",
    "away": "away",
    "home_win": "home",
    "away_win": "away",
    "x": "draw",
    "1": "home",
    "X": "draw",
    "2": "away",
}


def first_col(df: pd.DataFrame, keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in df.columns:
            return k
    return None


def try_pivot_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se o arquivo estiver “longo” (uma linha por outcome) com colunas genéricas,
    pivoteia para ter colunas odds_home/odds_draw/odds_away (e prob_* se existir).
    """
    oc = first_col(df, OUTCOME_KEY_CAND)
    if oc is None:
        return df

    # Normaliza outcomes
    out = df.copy()
    out["_norm_outcome"] = out[oc].astype(str).str.lower().map(OUTCOME_MAP).fillna(out[oc].astype(str).str.lower())

    price_col = first_col(out, PRICE_KEY_CAND)
    prob_col = first_col(out, PROB_KEY_CAND)

    home_col = first_col(out, HOME_KEYS)
    away_col = first_col(out, AWAY_KEYS)
    match_col = first_col(out, MATCH_KEYS)

    # Se não temos colunas principais, não dá para pivotar
    if (home_col is None and match_col is None) or (price_col is None and prob_col is None):
        return df

    group_cols = []
    if match_col:
        group_cols.append(match_col)
    if home_col:
        group_cols.append(home_col)
    if away_col:
        group_cols.append(away_col)

    pieces = []

    def _pivot(sub: pd.DataFrame, value_col: str, prefix: str) -> pd.DataFrame:
        pvt = sub.pivot_table(index=group_cols, columns="_norm_outcome", values=value_col, aggfunc="first")
        pvt.columns = [f"{prefix}_{c}" for c in pvt.columns]
        pvt = pvt.reset_index()
        return pvt

    base = None
    if price_col:
        pvt_odds = _pivot(out, price_col, "odds")
        base = pvt_odds

    if prob_col:
        pvt_prob = _pivot(out, prob_col, "prob")
        base = pvt_prob if base is None else pd.merge(base, pvt_prob, on=group_cols, how="outer")

    if base is None:
        return df

    # Garante colunas de times/chave
    for col_name, keys in [("team_home", HOME_KEYS), ("team_away", AWAY_KEYS), ("match_key", MATCH_KEYS)]:
        if col_name not in base.columns:
            src = first_col(df, keys)
            if src and src in df.columns and src in group_cols:
                base.rename(columns={src: col_name}, inplace=True)

    # Normaliza nomes de colunas finais (home/draw/away)
    rename_map = {}
    for src in base.columns:
        src_l = src.lower()
        if src_l.endswith("_home"):
            rename_map[src] = src_l.replace("_home", "_home")
        elif src_l.endswith("_draw"):
            rename_map[src] = src_l.replace("_draw", "_draw")
        elif src_l.endswith("_away"):
            rename_map[src] = src_l.replace("_away", "_away")
    base = base.rename(columns=rename_map)

    return base


def load_consensus(path: str, debug: bool = False) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = try_pivot_if_needed(df)

    # Mapeia nomes básicos se não existirem
    if first_col(df, HOME_KEYS) and "team_home" not in df.columns:
        df = df.rename(columns={first_col(df, HOME_KEYS): "team_home"})
    if first_col(df, AWAY_KEYS) and "team_away" not in df.columns:
        df = df.rename(columns={first_col(df, AWAY_KEYS): "team_away"})
    if first_col(df, MATCH_KEYS) and "match_key" not in df.columns:
        df = df.rename(columns={first_col(df, MATCH_KEYS): "match_key"})

    # Odds
    if first_col(df, ODDS_HOME_KEYS) and "odds_home" not in df.columns:
        df = df.rename(columns={first_col(df, ODDS_HOME_KEYS): "odds_home"})
    if first_col(df, ODDS_DRAW_KEYS) and "odds_draw" not in df.columns:
        df = df.rename(columns={first_col(df, ODDS_DRAW_KEYS): "odds_draw"})
    if first_col(df, ODDS_AWAY_KEYS) and "odds_away" not in df.columns:
        df = df.rename(columns={first_col(df, ODDS_AWAY_KEYS): "odds_away"})

    # Prob
    if first_col(df, PROB_HOME_KEYS) and "prob_home" not in df.columns:
        df = df.rename(columns={first_col(df, PROB_HOME_KEYS): "prob_home"})
    if first_col(df, PROB_DRAW_KEYS) and "prob_draw" not in df.columns:
        df = df.rename(columns={first_col(df, PROB_DRAW_KEYS): "prob_draw"})
    if first_col(df, PROB_AWAY_KEYS) and "prob_away" not in df.columns:
        df = df.rename(columns={first_col(df, PROB_AWAY_KEYS): "prob_away"})

    if debug:
        mapping = {
            "team_home": "team_home" if "team_home" in df.columns else None,
            "team_away": "team_away" if "team_away" in df.columns else None,
            "match_key": "match_key" if "match_key" in df.columns else None,
            "prob_home": "prob_home" if "prob_home" in df.columns else None,
            "prob_draw": "prob_draw" if "prob_draw" in df.columns else None,
            "prob_away": "prob_away" if "prob_away" in df.columns else None,
            "odds_home": "odds_home" if "odds_home" in df.columns else None,
            "odds_draw": "odds_draw" if "odds_draw" in df.columns else None,
            "odds_away": "odds_away" if "odds_away" in df.columns else None,
        }
        print("[kelly] mapeamento de colunas final:", json.dumps(mapping, ensure_ascii=False))

    return df


def load_probs_from_models(out_dir: str, debug: bool = False) -> Optional[pd.DataFrame]:
    """
    Tenta carregar probabilidades dos modelos, priorizando a ordem mais “forte”.
    Retorna DF com colunas: match_key, team_home, team_away, prob_home, prob_draw, prob_away
    """
    candidates = [
        "predictions_stacked.csv",
        "predictions_calibrated.csv",
        "predictions_xg_bi.csv",
        "predictions_xg_uni.csv",
    ]
    for fn in candidates:
        p = os.path.join(out_dir, fn)
        if os.path.exists(p):
            try:
                df = pd.read_csv(p)
                # Renomeia colunas de times/chave
                if first_col(df, HOME_KEYS) and "team_home" not in df.columns:
                    df = df.rename(columns={first_col(df, HOME_KEYS): "team_home"})
                if first_col(df, AWAY_KEYS) and "team_away" not in df.columns:
                    df = df.rename(columns={first_col(df, AWAY_KEYS): "team_away"})
                if first_col(df, MATCH_KEYS) and "match_key" not in df.columns:
                    df = df.rename(columns={first_col(df, MATCH_KEYS): "match_key"})

                # Renomeia probabilidades
                if first_col(df, PROB_HOME_KEYS) and "prob_home" not in df.columns:
                    df = df.rename(columns={first_col(df, PROB_HOME_KEYS): "prob_home"})
                if first_col(df, PROB_DRAW_KEYS) and "prob_draw" not in df.columns:
                    df = df.rename(columns={first_col(df, PROB_DRAW_KEYS): "prob_draw"})
                if first_col(df, PROB_AWAY_KEYS) and "prob_away" not in df.columns:
                    df = df.rename(columns={first_col(df, PROB_AWAY_KEYS): "prob_away"})

                # Mantém apenas o que importa
                keep = [c for c in ["match_key", "team_home", "team_away", "prob_home", "prob_draw", "prob_away"] if c in df.columns]
                df = df[keep].drop_duplicates()

                if debug:
                    print(f"[kelly] probabilidades carregadas de: {fn} ({len(df)} linhas)")
                return df
            except Exception as e:
                if debug:
                    print(f"[kelly] falha ao ler {fn}: {e}")
                continue
    return None


# ==========================
# Construção de linhas Kelly
# ==========================

def best_pick_for_row(row: pd.Series) -> Optional[Dict]:
    """Seleciona o melhor pick (home/draw/away) com base em maior stake."""
    options = []
    for side in ["home", "draw", "away"]:
        p = row.get(f"prob_{side}", None)
        o = row.get(f"odds_{side}", None)
        if p is None or o is None:
            options.append((side, 0.0, 0.0, 0.0))  # stake, kelly, edge = 0
        else:
            options.append((side, row.get(f"stake_{side}", 0.0), row.get(f"kelly_raw_{side}", 0.0), row.get(f"edge_{side}", 0.0)))

    # maior stake
    options.sort(key=lambda x: x[1], reverse=True)
    top = options[0]
    if top[1] <= 0:
        return None
    side, stake, kraw, edge = top
    return {
        "pick_side": side,
        "stake": stake,
        "kelly_raw": kraw,
        "edge": edge,
        "prob": row.get(f"prob_{side}", None),
        "odds": row.get(f"odds_{side}", None),
    }


def compute_kelly_rows(df: pd.DataFrame, cfg: KellyConfig, debug: bool = False) -> List[Dict]:
    rows: List[Dict] = []

    for _, r in df.iterrows():
        # Para cada outcome, calcula stake
        picks = {}
        for side in ["home", "draw", "away"]:
            p = r.get(f"prob_{side}", None)
            o = r.get(f"odds_{side}", None)
            stake, kraw, edge = stake_from_kelly(p or 0.0, o or 0.0, cfg)
            picks[side] = (stake, kraw, edge)

        # guarda nos campos temporários (útil para debug/ordenar)
        for side in ["home", "draw", "away"]:
            stake, kraw, edge = picks[side]
            r[f"stake_{side}"] = stake
            r[f"kelly_raw_{side}"] = kraw
            r[f"edge_{side}"] = edge

        best = best_pick_for_row(r)
        match_key = r.get("match_key", None)
        home = r.get("team_home", None)
        away = r.get("team_away", None)

        if best is None:
            # Sem aposta para este jogo
            if debug:
                print(f"[kelly] sem pick: {home} x {away}")
            continue

        rows.append({
            "match_key": match_key,
            "team_home": home,
            "team_away": away,
            "pick": best["pick_side"],           # home/draw/away
            "prob": best["prob"],
            "odds": best["odds"],
            "kelly_raw": best["kelly_raw"],
            "edge": best["edge"],
            "stake": best["stake"],
        })

    # Ordena por stake desc e aplica top_n
    rows.sort(key=lambda d: (d["stake"] or 0.0), reverse=True)
    if cfg.top_n and cfg.top_n > 0:
        rows = rows[: cfg.top_n]

    return rows


# ==========================
# Main
# ==========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    cfg = load_cfg_from_env()
    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    print("[kelly] config:", json.dumps({
        "bankroll": cfg.bankroll,
        "kelly_fraction": cfg.kelly_fraction,
        "kelly_cap": cfg.kelly_cap,
        "min_stake": cfg.min_stake,
        "max_stake": cfg.max_stake,
        "round_to": cfg.round_to,
        "top_n": cfg.top_n,
    }, ensure_ascii=False))
    print("[kelly] out_dir:", out_dir)

    consensus_path = os.path.join(out_dir, "odds_consensus.csv")
    print("[kelly] lendo:", consensus_path)

    if not os.path.exists(consensus_path):
        print("[kelly] AVISO: odds_consensus.csv não encontrado — gerando kelly_stakes vazio.")
        empty = pd.DataFrame(columns=[
            "match_key","team_home","team_away","pick","prob","odds","kelly_raw","edge","stake"
        ])
        empty.to_csv(os.path.join(out_dir, "kelly_stakes.csv"), index=False)
        return

    df = load_consensus(consensus_path, debug=args.debug)

    # Log mapeamento detectado (como no seu log original)
    print("[kelly] mapeamento de colunas detectado:")
    print(f"   -  team_home: {'team_home' if 'team_home' in df.columns else None}")
    print(f"   -  team_away: {'team_away' if 'team_away' in df.columns else None}")
    print(f"   -  match_key: {'match_key' if 'match_key' in df.columns else None}")
    print(f"   -  prob_home: {'prob_home' if 'prob_home' in df.columns else None}")
    print(f"   -  prob_draw: {'prob_draw' if 'prob_draw' in df.columns else None}")
    print(f"   -  prob_away: {'prob_away' if 'prob_away' in df.columns else None}")
    print(f"   -  odds_home: {'odds_home' if 'odds_home' in df.columns else None}")
    print(f"   -  odds_draw: {'odds_draw' if 'odds_draw' in df.columns else None}")
    print(f"   -  odds_away: {'odds_away' if 'odds_away' in df.columns else None}")

    # Se odds faltarem totalmente, não há o que fazer
    has_any_odds = any(c in df.columns for c in ["odds_home","odds_draw","odds_away"])
    if not has_any_odds:
        print("[kelly] AVISO: odds ausentes no consenso — gerando arquivo vazio.")
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","pick","prob","odds","kelly_raw","edge","stake"
        ]).to_csv(os.path.join(out_dir, "kelly_stakes.csv"), index=False)
        return

    # Se probabilities faltarem, tenta carregar de modelos
    has_all_probs = all(c in df.columns for c in ["prob_home","prob_draw","prob_away"])
    if not has_all_probs:
        probs_df = load_probs_from_models(out_dir, debug=args.debug)
        if probs_df is not None:
            on_cols = []
            if "match_key" in df.columns and "match_key" in probs_df.columns:
                on_cols = ["match_key"]
            else:
                # fallback por times — menos confiável, mas é o que dá
                if "team_home" in df.columns and "team_home" in probs_df.columns:
                    on_cols.append("team_home")
                if "team_away" in df.columns and "team_away" in probs_df.columns:
                    on_cols.append("team_away")
            if on_cols:
                df = pd.merge(df, probs_df, on=on_cols, how="left", suffixes=("", "_m"))
                # Garantir nomes finais
                for c in ["prob_home","prob_draw","prob_away"]:
                    if c not in df.columns and f"{c}_m" in df.columns:
                        df[c] = df[f"{c}_m"]
                # limpa colunas _m
                drop_m = [c for c in df.columns if c.endswith("_m")]
                if drop_m:
                    df = df.drop(columns=drop_m)

    # Último recurso: se só temos odds, não chutamos prob. (Poderíamos usar 1/odds normalizada, mas isso induz viés.)
    # Então filtramos linhas onde exista ao menos (prob_x e odds_x) para algum side.
    def has_pair(row):
        for s in ["home","draw","away"]:
            if (row.get(f"prob_{s}", None) is not None) and (row.get(f"odds_{s}", None) is not None):
                try:
                    p = float(row.get(f"prob_{s}", 0))
                    o = float(row.get(f"odds_{s}", 0))
                    if 0 <= p <= 1 and o > 1:
                        return True
                except Exception:
                    pass
        return False

    usable = df[df.apply(has_pair, axis=1)].copy()
    if usable.empty:
        print("[kelly] AVISO: não há pares (prob, odds) válidos — gerando arquivo vazio.")
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","pick","prob","odds","kelly_raw","edge","stake"
        ]).to_csv(os.path.join(out_dir, "kelly_stakes.csv"), index=False)
        return

    picks = compute_kelly_rows(usable, cfg, debug=args.debug)

    out_path = os.path.join(out_dir, "kelly_stakes.csv")
    if not picks:
        print("[kelly] AVISO: nenhuma aposta com stake > 0 — gerando arquivo vazio.")
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","pick","prob","odds","kelly_raw","edge","stake"
        ]).to_csv(out_path, index=False)
        print(f"[kelly] OK -> {out_path} (0 linhas)")
        return

    out_df = pd.DataFrame(picks, columns=[
        "match_key","team_home","team_away","pick","prob","odds","kelly_raw","edge","stake"
    ])
    out_df.to_csv(out_path, index=False)
    print(f"[kelly] OK -> {out_path} ({len(out_df)} linhas)")


if __name__ == "__main__":
    main()
