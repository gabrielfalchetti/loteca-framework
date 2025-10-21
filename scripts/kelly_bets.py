# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[kelly_bets] {msg}", flush=True)

def calculate_kelly_bets(probs_df: pd.DataFrame, odds_df: pd.DataFrame, bankroll: float, fraction: float, cap: float, top_n: int, round_to: int) -> pd.DataFrame:
    # Verificar colunas no probs_df
    required_cols_probs = ['team_home', 'team_away', 'prob_home', 'prob_draw', 'prob_away']
    missing_cols_probs = [col for col in required_cols_probs if col not in probs_df.columns]
    if missing_cols_probs:
        _log(f"Aviso: Colunas ausentes no probs.csv: {missing_cols_probs}. Usando valores padrão.")
        # Inicializar DataFrame com valores padrão
        results = []
        for _, row in probs_df.iterrows():
            results.append({
                'team_home': row.get('team_home', 'unknown'),
                'team_away': row.get('team_away', 'unknown'),
                'prob_home': 0.33,
                'prob_draw': 0.33,
                'prob_away': 0.34,
                'bet_home': 0.0,
                'bet_draw': 0.0,
                'bet_away': 0.0
            })
        return pd.DataFrame(results)

    # Verificar colunas no odds_df
    required_cols_odds = ['team_home', 'team_away', 'odds_home', 'odds_draw', 'odds_away']
    missing_cols_odds = [col for col in required_cols_odds if col not in odds_df.columns]
    if missing_cols_odds:
        _log(f"Aviso: Colunas ausentes no odds_consensus.csv: {missing_cols_odds}. Usando odds padrão (2.0).")
        # Inicializar DataFrame com odds padrão
        odds_df = pd.DataFrame({
            'team_home': probs_df['team_home'],
            'team_away': probs_df['team_away'],
            'odds_home': 2.0,
            'odds_draw': 2.0,
            'odds_away': 2.0
        })

    _log(f"Processando {len(probs_df)} jogos para apostas Kelly")

    # Mesclar probs e odds
    merged_df = pd.merge(probs_df, odds_df, on=['team_home', 'team_away'], how='left')

    bets = []
    for _, row in merged_df.iterrows():
        home_team = row['team_home']
        away_team = row['team_away']
        prob_home = row['prob_home']
        prob_draw = row['prob_draw']
        prob_away = row['prob_away']
        odds_home = row.get('odds_home', 2.0)
        odds_draw = row.get('odds_draw', 2.0)
        odds_away = row.get('odds_away', 2.0)

        # Calcular Kelly para cada outcome
        kelly_home = (odds_home * prob_home - 1) / (odds_home - 1) if odds_home > 1 else 0.0
        kelly_draw = (odds_draw * prob_draw - 1) / (odds_draw - 1) if odds_draw > 1 else 0.0
        kelly_away = (odds_away * prob_away - 1) / (odds_away - 1) if odds_away > 1 else 0.0

        # Aplicar fração e cap
        bet_home = max(0, min(kelly_home * fraction * bankroll, cap * bankroll))
        bet_draw = max(0, min(kelly_draw * fraction * bankroll, cap * bankroll))
        bet_away = max(0, min(kelly_away * fraction * bankroll, cap * bankroll))

        # Arredondar
        bet_home = round(bet_home, round_to)
        bet_draw = round(bet_draw, round_to)
        bet_away = round(bet_away, round_to)

        bets.append({
            'team_home': home_team,
            'team_away': away_team,
            'bet_home': bet_home,
            'bet_draw': bet_draw,
            'bet_away': bet_away
        })

        # Atualizar bankroll (simulação, subtrair apostas)
        bankroll -= (bet_home + bet_draw + bet_away)

    df = pd.DataFrame(bets)
    _log(f"Gerado DataFrame com {len(df)} apostas Kelly")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probs", required=True)
    ap.add_argument("--odds_source", required=True)
    ap.add_argument("--bankroll", type=float, required=True)
    ap.add_argument("--fraction", type=float, required=True)
    ap.add_argument("--cap", type=float, required=True)
    ap.add_argument("--top_n", type=int, required=True)
    ap.add_argument("--round_to", type=int, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if not os.path.isfile(args.probs):
        _log(f"Arquivo {args.probs} não encontrado")
        sys.exit(10)

    try:
        probs_df = pd.read_csv(args.probs)
    except Exception as e:
        _log(f"Erro ao ler {args.probs}: {e}")
        sys.exit(10)

    if probs_df.empty:
        _log("Arquivo probs.csv está vazio, gerando DataFrame vazio")
        probs_df = pd.DataFrame(columns=['team_home', 'team_away', 'prob_home', 'prob_draw', 'prob_away'])

    if not os.path.isfile(args.odds_source):
        _log(f"Arquivo {args.odds_source} não encontrado")
        sys.exit(10)

    try:
        odds_df = pd.read_csv(args.odds_source)
    except Exception as e:
        _log(f"Erro ao ler {args.odds_source}: {e}, usando odds padrão")
        odds_df = pd.DataFrame(columns=['team_home', 'team_away', 'odds_home', 'odds_draw', 'odds_away'])

    df = calculate_kelly_bets(probs_df, odds_df, args.bankroll, args.fraction, args.cap, args.top_n, args.round_to)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    _log(f"Arquivo {args.out} gerado com {len(df)} apostas")

if __name__ == "__main__":
    main()