# scripts/xg_bivariate.py
# -*- coding: utf-8 -*-
"""
Gera probabilidades 1-X-2 via Poisson Bivariado (assumindo independência de gols)
usando parâmetros dinâmicos salvos por train_dynamic_model.py.

Entrada:
- <rodada>/matches_whitelist.csv  (match_id, home, away)
- <rodada>/state_params.json      (home_adv, teams{team:{alpha,beta}})

Saída:
- <rodada>/xg_bivariate.csv  com colunas:
  match_id,team_home,team_away,lambda_home,lambda_away,prob_home,prob_draw,prob_away

Uso:
  python -m scripts.xg_bivariate --rodada data/out/<RUN_ID> [--max_goals 10]
"""
from __future__ import annotations
import argparse, json, os, sys
import numpy as np
import pandas as pd
from math import exp, factorial

def _safe_read_csv(path: str) -> pd.DataFrame | None:
    try:
        if os.path.isfile(path):
            return pd.read_csv(path)
    except Exception:
        pass
    return None

def _poisson_pmf(k: int, lam: float) -> float:
    # pmf(k; λ) = e^{-λ} λ^k / k!
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return float(exp(-lam) * (lam ** k) / factorial(k))

def outcome_probs(lambda_home: float, lambda_away: float, max_goals: int = 10):
    """
    Calcula P(home win), P(draw), P(away win) somando as probabilidades
    de placares (i,j) ~ Poi(λh) x Poi(λa).
    """
    ph = 0.0
    pd = 0.0
    pa = 0.0
    pmf_h = [_poisson_pmf(i, lambda_home) for i in range(max_goals + 1)]
    pmf_a = [_poisson_pmf(j, lambda_away) for j in range(max_goals + 1)]

    for i in range(max_goals + 1):
        for j in range(max_goals + 1):
            p = pmf_h[i] * pmf_a[j]
            if i > j:
                ph += p
            elif i == j:
                pd += p
            else:
                pa += p

    # perda de massa por truncamento: redistribui proporcionalmente
    s = ph + pd + pa
    if s > 0 and s < 0.999999:
        ph /= s; pd /= s; pa /= s
    return float(ph), float(pd), float(pa)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex: data/out/<RUN_ID>)")
    ap.add_argument("--max_goals", type=int, default=10, help="Truncamento da grade de gols (default=10)")
    args = ap.parse_args()

    rodada_dir = args.rodada
    wl_path = os.path.join(rodada_dir, "matches_whitelist.csv")
    state_path = os.path.join(rodada_dir, "state_params.json")

    wl = _safe_read_csv(wl_path)
    if wl is None or wl.empty:
        print(f"[xg_bivar][CRITICAL] Whitelist não encontrada ou vazia: {wl_path}", file=sys.stderr)
        return 7

    if not os.path.isfile(state_path):
        print(f"[xg_bivar][CRITICAL] state_params.json ausente. Rode train_dynamic_model antes: {state_path}", file=sys.stderr)
        return 7

    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)

    home_adv = float(state.get("home_adv", 0.25))
    team_params = state.get("teams", {})

    rows = []
    for _, row in wl.iterrows():
        mid = row["match_id"]
        th = str(row["home"]).strip()
        ta = str(row["away"]).strip()

        phome = team_params.get(th, {"alpha": 1.15, "beta": 1.05})
        paway = team_params.get(ta, {"alpha": 1.15, "beta": 1.05})

        # Poisson mean: λ_home = α_home (ataque) vs β_away (defesa rival) + mando
        #                λ_away = α_away vs β_home
        lam_h = max(0.05, float(phome["alpha"]) * (1.0 / max(0.2, float(paway["beta"]))) + home_adv)
        lam_a = max(0.05, float(paway["alpha"]) * (1.0 / max(0.2, float(phome["beta"]))))

        win_h, draw, win_a = outcome_probs(lam_h, lam_a, args.max_goals)

        rows.append({
            "match_id": mid,
            "team_home": th,
            "team_away": ta,
            "lambda_home": round(lam_h, 4),
            "lambda_away": round(lam_a, 4),
            "prob_home": round(win_h, 6),
            "prob_draw": round(draw, 6),
            "prob_away": round(win_a, 6),
        })

    out_df = pd.DataFrame(rows, columns=[
        "match_id", "team_home", "team_away",
        "lambda_home", "lambda_away",
        "prob_home", "prob_draw", "prob_away"
    ])

    out_path = os.path.join(rodada_dir, "xg_bivariate.csv")
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[xg_bivar][OK] Arquivo gerado: {out_path}  ({len(out_df)} linhas)")
    return 0

if __name__ == "__main__":
    sys.exit(main())