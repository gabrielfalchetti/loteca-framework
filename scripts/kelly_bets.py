# -*- coding: utf-8 -*-
import argparse, csv, os, pandas as pd, numpy as np
from typing import Tuple, List

def _log(msg: str) -> None:
    print(f"[kelly] {msg}", flush=True)

def kelly_fraction(p: float, o: float) -> float:
    """Calcula fração Kelly para odds decimais."""
    if not o or o <= 1.0:
        return 0.0
    b = o - 1.0
    q = 1.0 - p
    edge = (b * p - q)
    f = edge / b if b > 0 else 0.0
    return max(0.0, f)

def calculate_risk_metrics(stakes: np.ndarray, odds: np.ndarray, n_sim: int = 1000) -> Tuple[float, float]:
    """Calcula VaR (95%) e Expected Shortfall (95%) via Monte Carlo."""
    losses = np.random.normal(0, 0.1, n_sim) * stakes * (1 / odds - 1)  # Simula perdas com volatilidade
    var_95 = np.percentile(losses, 95)
    es_95 = np.mean(losses[losses > var_95])
    return var_95, es_95

def calculate_sharpe_ratio(returns: np.ndarray, risk_free: float = 0.0) -> float:
    """Calcula Sharpe Ratio (retorno ajustado por risco)."""
    if len(returns) < 2:
        return 0.0
    mean_return = np.mean(returns)
    std_return = np.std(returns)
    return (mean_return - risk_free) / std_return if std_return > 0 else 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probs", required=True, help="CSV de probs calibradas (ex.: predictions_calibrated.csv)")
    ap.add_argument("--odds_source", default=None, help="CSV com odds (ex.: odds_consensus.csv)")
    ap.add_argument("--bankroll", type=float, required=True)
    ap.add_argument("--fraction", type=float, required=True)
    ap.add_argument("--cap", type=float, required=True)
    ap.add_argument("--top_n", type=int, required=True)
    ap.add_argument("--round_to", type=float, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # Carregar probs
    df_probs = pd.read_csv(args.probs)
    if not all(col in df_probs.columns for col in ["match_id", "team_home", "team_away", "p_home_cal", "p_draw_cal", "p_away_cal"]):
        raise ValueError("CSV de probs sem colunas esperadas")
    probs = df_probs[["p_home_cal", "p_draw_cal", "p_away_cal"]].values
    if not np.all((probs >= 0) & (probs <= 1)):
        raise ValueError("Probs inválidas (fora de [0,1])")
    if not np.allclose(probs.sum(axis=1), 1, atol=0.01):
        _log("Soma de probs != 1, normalizando...")
        probs = probs / probs.sum(axis=1, keepdims=True)

    # Carregar odds (se especificado ou fallback)
    odds_df = None
    if args.odds_source and os.path.exists(args.odds_source):
        odds_df = pd.read_csv(args.odds_source)
        if not all(col in odds_df.columns for col in ["match_id", "odds_home", "odds_draw", "odds_away"]):
            raise ValueError("CSV de odds sem colunas esperadas")
    elif os.path.exists(os.path.join(os.path.dirname(args.out), "odds_consensus.csv")):
        odds_df = pd.read_csv(os.path.join(os.path.dirname(args.out), "odds_consensus.csv"))
    else:
        _log("Sem fonte de odds, usando 1.0 como fallback (sem stakes)")
        odds_df = pd.DataFrame({"match_id": df_probs["match_id"], "odds_home": 1.0, "odds_draw": 1.0, "odds_away": 1.0})

    df = df_probs.merge(odds_df, on="match_id", how="left").fillna(0)

    # Calcular stakes
    bets = []
    stakes, odds_values = [], []
    for _, r in df.iterrows():
        choices = [
            ("H", float(r["p_home_cal"]), float(r["odds_home"] or 0)),
            ("D", float(r["p_draw_cal"]), float(r["odds_draw"] or 0)),
            ("A", float(r["p_away_cal"]), float(r["odds_away"] or 0)),
        ]
        choices = [(k, p, o, kelly_fraction(p, o)) for k, p, o in choices]
        best = max(choices, key=lambda t: t[3], default=("H", 0, 0, 0))
        k, p, o, f = best
        f_eff = min(args.cap, f * args.fraction)
        stake = round(args.bankroll * f_eff / max(args.round_to, 1e-9)) * args.round_to
        ev = p * o - 1.0 if o > 0 else 0.0
        bets.append([r["match_id"], r["team_home"], r["team_away"], k, p, o, f, stake, ev])
        stakes.append(stake)
        odds_values.append(o)

    # Calcular métricas de risco
    stakes_arr = np.array(stakes)
    odds_arr = np.array(odds_values)
    var_95, es_95 = calculate_risk_metrics(stakes_arr, odds_arr)
    sharpe = calculate_sharpe_ratio(np.array([b[-1] for b in bets]) / args.bankroll)

    # Ordenar e limitar top_n
    bets_df = pd.DataFrame(bets, columns=["match_id", "team_home", "team_away", "pick", "p", "odds", "kelly_f", "stake", "expected_value"])
    bets_df = bets_df.sort_values("kelly_f", ascending=False).head(args.top_n)

    # Salvar resultados
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    bets_df.to_csv(args.out, index=False, encoding="utf-8")
    _log(f"OK -> {args.out} (linhas={len(bets_df)}, VaR 95%: {var_95:.2f}, ES 95%: {es_95:.2f}, Sharpe: {sharpe:.2f})")

if __name__ == "__main__":
    main()