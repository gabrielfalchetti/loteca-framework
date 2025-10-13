# scripts/train_dynamic_model.py
# -*- coding: utf-8 -*-
"""
Treina um "Poisson Bivariado Dinâmico" leve (equivalente a um modelo de espaço
de estados com suavização exponencial/EWMA para ataque e defesa).
Saída: <rodada>/state_params.json com:
{
  "home_adv": float,
  "teams": {
     "<Time>": {"alpha": float, "beta": float}
  }
}

Entrada mínima necessária:
- <rodada>/matches_whitelist.csv  (colunas: match_id,home,away)
Opcional (melhora o ajuste, se existir):
- data/history/results.csv        (colunas: date,home,away,goals_home,goals_away)

Uso:
  python -m scripts.train_dynamic_model --rodada data/out/<RUN_ID> [--history data/history/results.csv] [--ewma 0.20]
"""
from __future__ import annotations
import argparse, json, os, sys
from typing import Dict, Tuple
import math
import pandas as pd
import numpy as np

def _safe_read_csv(path: str) -> pd.DataFrame | None:
    try:
        if os.path.isfile(path):
            return pd.read_csv(path)
    except Exception:
        pass
    return None

def _normalize_team(s: str) -> str:
    return str(s).strip()

def estimate_home_adv(df_hist: pd.DataFrame) -> float:
    """Estimativa simples de vantagem de mando = média( Ghome - Gaway )."""
    if df_hist is None or df_hist.empty:
        return 0.25  # default moderado
    diff = (df_hist["goals_home"] - df_hist["goals_away"]).astype(float)
    return float(np.clip(diff.mean() * 0.15 + 0.15, 0.0, 0.6))  # compressão

def ewma_updates(df_hist: pd.DataFrame, alpha_ewma: float) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    EWMA para 'força de ataque' (gols feitos) e 'força de defesa' (gols sofridos).
    Retorna dicionários por time: alpha (ataque) e beta (defesa).
    """
    atk = {}
    dfn = {}
    if df_hist is None or df_hist.empty:
        return atk, dfn

    # Ordena temporalmente se tiver data
    if "date" in df_hist.columns:
        try:
            df_hist = df_hist.copy()
            df_hist["date"] = pd.to_datetime(df_hist["date"], errors="coerce")
            df_hist = df_hist.sort_values("date")
        except Exception:
            pass

    for _, row in df_hist.iterrows():
        h, a = _normalize_team(row["home"]), _normalize_team(row["away"])
        gh, ga = float(row["goals_home"]), float(row["goals_away"])

        # ataque (gols marcados)
        prev_h_atk = atk.get(h, gh)  # seed com primeiro valor observado
        prev_a_atk = atk.get(a, ga)

        atk[h] = (1 - alpha_ewma) * prev_h_atk + alpha_ewma * gh
        atk[a] = (1 - alpha_ewma) * prev_a_atk + alpha_ewma * ga

        # defesa (gols sofridos)
        prev_h_def = dfn.get(h, ga)
        prev_a_def = dfn.get(a, gh)

        dfn[h] = (1 - alpha_ewma) * prev_h_def + alpha_ewma * ga
        dfn[a] = (1 - alpha_ewma) * prev_a_def + alpha_ewma * gh

    # Regularização leve para evitar zeros extremos
    for t in set(list(atk.keys()) + list(dfn.keys())):
        atk[t] = float(max(0.2, min(3.0, atk.get(t, 1.1))))
        dfn[t] = float(max(0.2, min(3.0, dfn.get(t, 1.1))))

    return atk, dfn

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex: data/out/<RUN_ID>)")
    ap.add_argument("--history", default="data/history/results.csv", help="Caminho do histórico (opcional)")
    ap.add_argument("--ewma", type=float, default=0.20, help="Fator EWMA (0..1). Default 0.20")
    args = ap.parse_args()

    rodada_dir = args.rodada
    os.makedirs(rodada_dir, exist_ok=True)

    wl_path = os.path.join(rodada_dir, "matches_whitelist.csv")
    wl = _safe_read_csv(wl_path)
    if wl is None or wl.empty:
        print(f"[dynamic][CRITICAL] Whitelist não encontrada ou vazia: {wl_path}", file=sys.stderr)
        return 5

    hist = _safe_read_csv(args.history)
    if hist is not None and not hist.empty:
        needed = {"home", "away", "goals_home", "goals_away"}
        if not needed.issubset(set(hist.columns)):
            hist = None  # ignora se colunas ausentes

    home_adv = estimate_home_adv(hist)  # vantagem de mando média
    atk, dfn = ewma_updates(hist, args.ewma) if hist is not None else ({}, {})

    # Garante que todos times da rodada tenham α/β (mesmo sem histórico)
    teams = set(_normalize_team(x) for x in pd.concat([wl["home"], wl["away"]]).unique())
    params = {}
    for t in teams:
        params[t] = {
            "alpha": float(atk.get(t, 1.15)),  # defaults suaves
            "beta": float(dfn.get(t, 1.05))
        }

    out = {
        "home_adv": float(home_adv),
        "teams": params
    }

    state_path = os.path.join(rodada_dir, "state_params.json")
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[dynamic][OK] Parâmetros salvos em: {state_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())