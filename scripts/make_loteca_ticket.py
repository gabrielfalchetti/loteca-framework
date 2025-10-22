# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os

def _log(msg: str) -> None:
    print(f"[loteca] {msg}", flush=True)

def generate_loteca_card(rodada, triples, doubles):
    bets_file = f"{rodada}/bets_kelly.csv"
    if not os.path.isfile(bets_file):
        _log(f"Arquivo {bets_file} não encontrado, prosseguindo com matches_norm.csv")
        bets_df = pd.DataFrame(columns=['home_team', 'away_team', 'home_bet', 'draw_bet', 'away_bet'])
    else:
        try:
            bets_df = pd.read_csv(bets_file)
            _log(f"Carregado {bets_file} com {len(bets_df)} jogos")
        except Exception as e:
            _log(f"Erro ao ler {bets_file}: {e}, prosseguindo com DataFrame vazio")
            bets_df = pd.DataFrame(columns=['home_team', 'away_team', 'home_bet', 'draw_bet', 'away_bet'])

    matches_file = f"{rodada}/matches_norm.csv"
    if not os.path.isfile(matches_file):
        _log(f"Arquivo {matches_file} não encontrado")
        sys.exit(11)

    try:
        matches_df = pd.read_csv(matches_file)
    except Exception as e:
        _log(f"Erro ao ler {matches_file}: {e}")
        sys.exit(11)

    if matches_df.empty:
        _log("Arquivo matches_norm.csv está vazio")
        sys.exit(11)

    home_col = next((col for col in ['team_home', 'home'] if col in matches_df.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in matches_df.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas em matches_norm.csv")
        sys.exit(11)

    _log(f"Processando {len(matches_df)} jogos do matches_norm.csv")

    # Ajustar triples e doubles com base no número de jogos
    num_games = len(matches_df)
    triples = min(triples, num_games)  # Limitar triples ao número de jogos
    doubles = min(doubles, num_games - triples)  # Limitar doubles ao número restante

    _log(f"Usando {triples} triples e {doubles} doubles para {num_games} jogos")

    # Inicializar DataFrame de resultados
    results = []
    for _, row in matches_df.iterrows():
        home_team = row[home_col]
        away_team = row[away_col]
        
        # Obter apostas do bets_kelly.csv, se disponível
        bet_row = bets_df[(bets_df['home_team'] == home_team) & (bets_df['away_team'] == away_team)] if not bets_df.empty else pd.DataFrame()
        if not bet_row.empty:
            bet_home = bet_row['home_bet'].iloc[0]
            bet_draw = bet_row['draw_bet'].iloc[0]
            bet_away = bet_row['away_bet'].iloc[0]
        else:
            bet_home = bet_draw = bet_away = 0.0

        # Determinar escolha com base nas apostas
        choices = ['H']  # Default: escolher time da casa
        if bet_draw > bet_home and bet_draw > bet_away:
            choices = ['D']
        elif bet_away > bet_home and bet_away > bet_draw:
            choices = ['A']
        
        # Aplicar triples e doubles
        if triples > 0:
            choices = ['H', 'D', 'A']
            triples -= 1
        elif doubles > 0:
            choices = ['H', 'D'] if bet_home >= bet_away else ['D', 'A']
            doubles -= 1

        results.append({
            'team_home': home_team,
            'team_away': away_team,
            'choice': ','.join(choices)
        })

    df = pd.DataFrame(results)
    _log(f"Gerado cartão da Loteca com {len(df)} jogos")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--triples", type=int, default=0)
    ap.add_argument("--doubles", type=int, default=0)
    args = ap.parse_args()

    df = generate_loteca_card(args.rodada, args.triples, args.doubles)
    out_file = f"{args.rodada}/loteca_card.csv"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    df.to_csv(out_file, index=False)
    _log(f"Arquivo {out_file} gerado com {len(df)} jogos")

if __name__ == "__main__":
    main()