# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import os

def _log(msg: str) -> None:
    print(f"[generate_consensus] {msg}", flush=True)

def generate_consensus(theodds_file, sportmonks_file, output_file):
    try:
        theodds_df = pd.read_csv(theodds_file)
        _log(f"Carregado {theodds_file} com {len(theodds_df)} linhas")
    except Exception as e:
        _log(f"Erro ao ler {theodds_file}: {e}")
        return

    try:
        sportmonks_df = pd.read_csv(sportmonks_file)
        _log(f"Carregado {sportmonks_file} com {len(sportmonks_df)} linhas")
    except Exception as e:
        _log(f"Erro ao ler {sportmonks_file}: {e}")
        return

    # Renomear colunas, se necessário
    if 'team_home' in theodds_df.columns:
        theodds_df = theodds_df.rename(columns={'team_home': 'home_team', 'team_away': 'away_team'})
    if 'team_home' in sportmonks_df.columns:
        sportmonks_df = sportmonks_df.rename(columns={'team_home': 'home_team', 'team_away': 'away_team'})

    # Merge dos DataFrames usando 'home_team' e 'away_team'
    try:
        consensus = theodds_df.merge(sportmonks_df, on=['home_team', 'away_team'], how='outer', suffixes=('_theodds', '_sportmonks'))
        _log(f"Merge realizado com {len(consensus)} linhas")
    except Exception as e:
        _log(f"Erro ao realizar merge: {e}")
        return

    # Calcular médias das odds
    consensus['home_odds'] = consensus[['home_odds_theodds', 'home_odds_sportmonks']].mean(axis=1).fillna(2.0)
    consensus['draw_odds'] = consensus[['draw_odds_theodds', 'draw_odds_sportmonks']].mean(axis=1).fillna(3.0)
    consensus['away_odds'] = consensus[['away_odds_theodds', 'away_odds_sportmonks']].mean(axis=1).fillna(2.5)

    # Selecionar apenas colunas relevantes
    consensus = consensus[['home_team', 'away_team', 'home_odds', 'draw_odds', 'away_odds']]

    # Salvar o resultado
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    consensus.to_csv(output_file, index=False)
    _log(f"Consenso salvo em {output_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--theodds_file", required=True)
    ap.add_argument("--sportmonks_file", required=True)
    ap.add_argument("--output_file", required=True)
    args = ap.parse_args()
    generate_consensus(args.theodds_file, args.sportmonks_file, args.output_file)

if __name__ == "__main__":
    main()