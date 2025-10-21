# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
import numpy as np

def _log(msg: str) -> None:
    print(f"[kelly_bets] {msg}", flush=True)

def calculate_kelly_bets(probs_df: pd.DataFrame, odds_df: pd.DataFrame, bankroll: float, fraction: float, cap: float, top_n: int, round_to: int) -> pd.DataFrame:
    # Verificar colunas em probs_df
    prob_cols = ['team_home', 'team_away', 'prob_home', 'prob_draw', 'prob_away']
    missing_prob_cols = [col for col in prob_cols if col not in probs_df.columns]
    if missing_prob_cols:
        _log(f"Aviso: Colunas ausentes no predictions_calibrated.csv: {missing_prob_cols}. Usando valores padrão.")
        # Inicializar DataFrame com valores padrão
        results = []
        for _, row in probs_df.iterrows():
            results.append({
                'team_home': row.get('team_home', 'unknown'),
                'team_away': row.get('team_away', 'unknown'),
                'bet_home': 0.0,
                'bet_draw': 0.0,
                'bet_away': 0.0
            })
        return pd.DataFrame(results)

    # Verificar colunas em odds_df
    odds_cols = ['team_home', 'team_away', 'odds_home', 'odds_draw', 'odds_away']
    missing_odds_cols = [col for col in odds_cols if col not in odds_df.columns]
    if missing_odds_cols:
        _log(f"Aviso: Colunas ausentes no odds_consensus.csv: {missing_odds_cols}. Usando odds padrão.")
        odds_df = pd.DataFrame(columns=odds_cols)

    _log(f"Processando {len(probs_df)} jogos do predictions_calibrated.csv")
    _log(f"Colunas em predictions_calibrated.csv: {list(probs_df.columns)}")
    _log(f"Colunas em odds_consensus.csv: {list(odds_df.columns)}")

    results = []
    for _, prob_row in probs_df.iterrows():
        home_team = prob_row['team_home']
        away_team = prob_row['team_away']
        prob_home = prob_row['prob_home']
        prob_draw = prob_row['prob_draw']
        prob_away = prob_row['prob_away']

        # Inicializar apostas padrão
        bet_home = 0.0
        bet_draw = 0.0
        bet_away = 0.0

        # Buscar odds correspondentes
        odds_row = odds_df[(odds_df['team_home'] == home_team) & (odds_df['team_away'] == away_team)]
        if not odds_row.empty:
            odds_home = odds_row['odds_home'].iloc[0]
            odds_draw = odds_row['odds_draw'].iloc[0]
            odds_away = odds_row['odds_away'].iloc[0]

            # Calcular Kelly Criterion
            try:
                # Kelly: (prob * odds - 1) / (odds - 1)
                def kelly_bet(prob, odds):
                    if odds <= 1 or prob <= 0:
                        return 0.0
                    k = (prob * odds - 1) / (odds - 1)
                    return max(0.0, min(k * fraction * bankroll, cap * bankroll))

                bet_home = kelly_bet(prob_home, odds_home)
                bet_draw = kelly_bet(prob_draw, odds_draw)
                bet_away = kelly_bet(prob_away, odds_away)

                # Arredondar apostas
                bet_home = round(bet_home, round_to)
                bet_draw = round(bet_draw, round_to)
                bet_away = round(bet_away, round_to)
            except Exception as e:
                _log(f"Erro ao calcular apostas para {home_team} x {away_team}: {e}, usando apostas padrão")
        else:
            _log(f"Sem odds para {home_team} x {away_team}, usando apostas padrão")

        results.append({
            'team_home': home_team,
            'team_away': away_team,
            'bet_home': bet_home,
            'bet_draw': bet_draw,
            'bet_away': bet_away
        })

    df = pd.DataFrame(results)
    if df.empty:
        _log("Nenhum jogo processado, gerando DataFrame vazio")
        return pd.DataFrame(columns=['team_home', 'team_away', 'bet_home', 'bet_draw', 'bet_away'])

    # Selecionar top_n apostas (se necessário)
    if top_n > 0:
        df['max_bet'] = df[['bet_home', 'bet_draw', 'bet_away']].max(axis=1)
        df = df.sort_values('max_bet', ascending=False).head(top_n).drop(columns=['max_bet'])

    _log(f"Gerado DataFrame com {len(df)} apostas")
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

    # Carregar predictions_calibrated.csv
    if not os.path.isfile(args.probs):
        _log(f"Arquivo {args.probs} não encontrado, gerando DataFrame vazio")
        probs_df = pd.DataFrame(columns=['team_home', 'team_away', 'prob_home', 'prob_draw', 'prob_away'])
    else:
        try:
            probs_df = pd.read_csv(args.probs)
        except Exception as e:
            _log(f"Erro ao ler {args.probs}: {e}, gerando DataFrame vazio")
            probs_df = pd.DataFrame(columns=['team_home', 'team_away', 'prob_home', 'prob_draw', 'prob_away'])

    # Carregar odds_consensus.csv
    if not os.path.isfile(args.odds_source):
        _log(f"Arquivo {args.odds_source} não encontrado, gerando DataFrame vazio")
        odds_df = pd.DataFrame(columns=['team_home', 'team_away', 'odds_home', 'odds_draw', 'odds_away'])
    else:
        try:
            odds_df = pd.read_csv(args.odds_source)
        except Exception as e:
            _log(f"Erro ao ler {args.odds_source}: {e}, gerando DataFrame vazio")
            odds_df = pd.DataFrame(columns=['team_home', 'team_away', 'odds_home', 'odds_draw', 'odds_away'])

    df = calculate_kelly_bets(probs_df, odds_df, args.bankroll, args.fraction, args.cap, args.top_n, args.round_to)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    _log(f"Arquivo {args.out} gerado com {len(df)} apostas")

if __name__ == "__main__":
    main()