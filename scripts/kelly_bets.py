# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os

def _log(msg: str) -> None:
    print(f"[kelly_bets] {msg}", flush=True)

def kelly_bets(probs_csv, odds_source, bankroll, fraction, cap, top_n, round_to, out_csv):
    if not os.path.isfile(probs_csv):
        _log(f"Arquivo {probs_csv} não encontrado")
        sys.exit(10)

    try:
        probs = pd.read_csv(probs_csv)
    except Exception as e:
        _log(f"Erro ao ler {probs_csv}: {e}")
        sys.exit(10)

    # Verificar se odds_source existe
    odds = pd.DataFrame(columns=['match_id', 'home_team', 'away_team', 'home_odds', 'draw_odds', 'away_odds'])
    if os.path.isfile(odds_source):
        try:
            odds = pd.read_csv(odds_source)
        except Exception as e:
            _log(f"Erro ao ler {odds_source}: {e}, usando odds padrão")
    else:
        _log(f"Arquivo {odds_source} não encontrado, usando odds padrão")
        # Criar odds padrão com base em probs
        odds = probs[['match_id', 'home_team', 'away_team']].copy()
        odds['home_odds'] = 2.0
        odds['draw_odds'] = 3.0
        odds['away_odds'] = 2.5

    # Alinhar probs e odds por match_id ou teams
    bets = []
    for _, prob_row in probs.iterrows():
        match_id = prob_row.get('match_id', 0)
        home_team = prob_row['home_team']
        away_team = prob_row['away_team']
        home_prob = prob_row.get('home_prob_calibrated', prob_row.get('home_prob', 0.33))
        draw_prob = prob_row.get('draw_prob_calibrated', prob_row.get('draw_prob', 0.33))
        away_prob = prob_row.get('away_prob_calibrated', prob_row.get('away_prob', 0.34))
        # Encontrar odds correspondentes
        odds_row = odds[(odds['match_id'] == match_id) | 
                       ((odds['home_team'] == home_team) & (odds['away_team'] == away_team))].iloc[0] if not odds.empty else None
        if odds_row is not None:
            home_odds = odds_row['home_odds']
            draw_odds = odds_row['draw_odds']
            away_odds = odds_row['away_odds']
        else:
            home_odds = 2.0
            draw_odds = 3.0
            away_odds = 2.5
            _log(f"Sem odds para {home_team} x {away_team}, usando odds padrão")

        kelly_home = max(0, (home_prob * home_odds - 1) / (home_odds - 1)) * fraction
        kelly_draw = max(0, (draw_prob * draw_odds - 1) / (draw_odds - 1)) * fraction
        kelly_away = max(0, (away_prob * away_odds - 1) / (away_odds - 1)) * fraction
        kelly_home = min(kelly_home, cap)
        kelly_draw = min(kelly_draw, cap)
        kelly_away = min(kelly_away, cap)
        bets.append({
            'match_id': match_id,
            'home_team': home_team,
            'away_team': away_team,
            'home_bet': round(bankroll * kelly_home, round_to),
            'draw_bet': round(bankroll * kelly_draw, round_to),
            'away_bet': round(bankroll * kelly_away, round_to)
        })

    df_bets = pd.DataFrame(bets)
    df_bets = df_bets.sort_values(by=['home_bet', 'draw_bet', 'away_bet'], ascending=False).head(top_n)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df_bets.to_csv(out_csv, index=False)
    _log(f"Apostas Kelly salvas em {out_csv}")

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

    kelly_bets(args.probs, args.odds_source, args.bankroll, args.fraction, args.cap, args.top_n, args.round_to, args.out)

if __name__ == "__main__":
    main()