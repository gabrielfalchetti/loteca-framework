from __future__ import annotations
import argparse
import numpy as np
import pandas as pd
from pathlib import Path

def inv(p):
    """Converte probabilidades em odds (ajusta zero)."""
    return np.where(p > 1e-9, 1.0 / p, np.nan)

def _apply_lineups(df: pd.DataFrame, P: np.ndarray, cap: float = 0.02) -> np.ndarray:
    """Ajusta probabilidades conforme sinal de lineups."""
    if "lineup_signal_home" not in df.columns or "lineup_signal_away" not in df.columns:
        return P
    adj = P.copy()
    for i, r in df.iterrows():
        if pd.notna(r.get("lineup_signal_home", 0)) and r["lineup_signal_home"] != 0:
            adj[i,0] = max(0.0, adj[i,0] - cap * np.sign(r["lineup_signal_home"]))
        if pd.notna(r.get("lineup_signal_away", 0)) and r["lineup_signal_away"] != 0:
            adj[i,2] = max(0.0, adj[i,2] - cap * np.sign(r["lineup_signal_away"]))
    adj = adj / adj.sum(axis=1, keepdims=True)
    return adj

def _apply_weather(df: pd.DataFrame, P: np.ndarray, cap: float = 0.015) -> np.ndarray:
    """Ajusta probabilidades conforme sinal climático (chuva/altitude etc)."""
    if "weather_signal" not in df.columns:
        return P
    adj = P.copy()
    for i, r in df.iterrows():
        ws = r.get("weather_signal", 0)
        if pd.notna(ws) and ws != 0:
            adj[i,1] = min(1.0, adj[i,1] + cap * np.sign(ws))
    adj = adj / adj.sum(axis=1, keepdims=True)
    return adj

def _apply_movement(df: pd.DataFrame, P: np.ndarray, cap: float = 0.015) -> np.ndarray:
    """Ajusta conforme movimento de odds (alerts_odds_movement.csv)."""
    if "move_signal" not in df.columns:
        return P
    adj = P.copy()
    for i, r in df.iterrows():
        ms = r.get("move_signal", 0)
        if pd.notna(ms) and ms != 0:
            if ms > 0:  # mercado favorece mandante
                adj[i,0] = min(1.0, adj[i,0] + cap)
            elif ms < 0:  # mercado favorece visitante
                adj[i,2] = min(1.0, adj[i,2] + cap)
    adj = adj / adj.sum(axis=1, keepdims=True)
    return adj

def _apply_news(df: pd.DataFrame, P: np.ndarray, cap: float = 0.01) -> np.ndarray:
    """Ajusta probabilidades com base em sinais de notícias (lesões, suspensões, técnico, viagem)."""
    cols = [
        "injury_signal_home", "suspension_signal_home", "coach_change_home", "travel_fatigue_home",
        "injury_signal_away", "suspension_signal_away", "coach_change_away", "travel_fatigue_away",
    ]
    if not all(c in df.columns for c in cols):
        return P

    adj = P.copy()
    for i, r in df.iterrows():
        penalty_home = (
            r.get("injury_signal_home", 0) +
            r.get("suspension_signal_home", 0) +
            r.get("coach_change_home", 0) +
            r.get("travel_fatigue_home", 0)
        )
        penalty_away = (
            r.get("injury_signal_away", 0) +
            r.get("suspension_signal_away", 0) +
            r.get("coach_change_away", 0) +
            r.get("travel_fatigue_away", 0)
        )

        if penalty_home > 0:
            adj[i,0] = max(0.0, adj[i,0] - cap * penalty_home)
            adj[i,2] = min(1.0, adj[i,2] + cap * penalty_home)
        if penalty_away > 0:
            adj[i,2] = max(0.0, adj[i,2] - cap * penalty_away)
            adj[i,0] = min(1.0, adj[i,0] + cap * penalty_away)

    adj = adj / adj.sum(axis=1, keepdims=True)
    return adj

def main():
    ap = argparse.ArgumentParser(description="Ajusta probabilidades pré-jogo (lineups, clima, movimento, notícias)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--cap-lineups", type=float, default=0.02)
    ap.add_argument("--cap-weather", type=float, default=0.015)
    ap.add_argument("--cap-move", type=float, default=0.015)
    ap.add_argument("--cap-news", type=float, default=0.01)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    joined_path = base / "joined.csv"
    if not joined_path.exists() or joined_path.stat().st_size == 0:
        raise RuntimeError(f"[adjust] joined.csv ausente/vazio: {joined_path}")
    df = pd.read_csv(joined_path).rename(columns=str.lower)

    # iniciar matriz P
    P = df[["p_home","p_draw","p_away"]].values.astype(float)

    # aplicar ajustes
    P = _apply_lineups(df, P, cap=args.cap_lineups)
    P = _apply_weather(df, P, cap=args.cap_weather)
    P = _apply_movement(df, P, cap=args.cap_move)

    # tentar aplicar sinais de notícias
    news_path = base / "news_signals.csv"
    if news_path.exists() and news_path.stat().st_size > 0:
        try:
            df_news = pd.read_csv(news_path).rename(columns=str.lower)
            df = df.merge(df_news, on="match_id", how="left")
            P = _apply_news(df, P, cap=args.cap_news)
            print(f"[adjust] notícias aplicadas de {news_path}")
        except Exception as e:
            print(f"[adjust] falha ao aplicar notícias: {e}")

    # salvar ajustado
    out = df.copy()
    out[["p_home","p_draw","p_away"]] = P
    out[["odd_home","odd_draw","odd_away"]] = inv(P)
    out_path = base / "joined_pregame.csv"
    out.to_csv(out_path, index=False)
    print(f"[adjust] OK -> {out_path}")

if __name__ == "__main__":
    main()
