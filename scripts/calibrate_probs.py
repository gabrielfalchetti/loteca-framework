# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import pickle
import os
import numpy as np

def _log(msg: str) -> None:
    print(f"[calibrate] {msg}", flush=True)

def calibrate_probs(predictions_df: pd.DataFrame, calibrator) -> pd.DataFrame:
    _log(f"Versão do pandas: {pd.__version__}")
    
    # Verificar colunas no predictions_df
    required_cols = ['team_home', 'team_away', 'prob_home', 'prob_draw', 'prob_away']
    missing_cols = [col for col in required_cols if col not in predictions_df.columns]
    if missing_cols:
        _log(f"Aviso: Colunas ausentes no predictions.csv: {missing_cols}. Usando valores padrão.")
        # Inicializar DataFrame com valores padrão
        results = []
        for _, row in predictions_df.iterrows():
            results.append({
                'team_home': row.get('team_home', 'unknown'),
                'team_away': row.get('team_away', 'unknown'),
                'prob_home': 0.33,
                'prob_draw': 0.33,
                'prob_away': 0.34
            })
        return pd.DataFrame(results)

    _log(f"Processando {len(predictions_df)} jogos do predictions.csv")

    results = []
    for _, row in predictions_df.iterrows():
        home_team = row['team_home']
        away_team = row['team_away']
        prob_home = row['prob_home']
        prob_draw = row['prob_draw']
        prob_away = row['prob_away']

        # Aplicar calibrador se disponível
        if calibrator is not None:
            try:
                prob_home = calibrator.predict([prob_home])[0] if not np.isnan(prob_home) else 0.33
                prob_draw = calibrator.predict([prob_draw])[0] if not np.isnan(prob_draw) else 0.33
                prob_away = calibrator.predict([prob_away])[0] if not np.isnan(prob_away) else 0.34
                # Normalizar probabilidades para somar 1
                total = prob_home + prob_draw + prob_away
                if total > 0:
                    prob_home /= total
                    prob_draw /= total
                    prob_away /= total
                else:
                    prob_home, prob_draw, prob_away = 0.33, 0.33, 0.34
            except Exception as e:
                _log(f"Erro ao calibrar para {home_team} x {away_team}: {str(e)}, usando valores padrão")
                prob_home, prob_draw, prob_away = 0.33, 0.33, 0.34
        else:
            _log(f"Calibrador não disponível para {home_team} x {away_team}, usando valores padrão")
            prob_home, prob_draw, prob_away = 0.33, 0.33, 0.34

        results.append({
            'team_home': home_team,
            'team_away': away_team,
            'prob_home': prob_home,
            'prob_draw': prob_draw,
            'prob_away': prob_away
        })

    df = pd.DataFrame(results)
    _log(f"Gerado DataFrame com {len(df)} jogos calibrados")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", required=True, dest="input_csv")
    ap.add_argument("--cal", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # Carregar predictions.csv
    if not os.path.isfile(args.input_csv):
        _log(f"Arquivo {args.input_csv} não encontrado")
        sys.exit(9)

    try:
        predictions_df = pd.read_csv(args.input_csv)
    except Exception as e:
        _log(f"Erro ao ler {args.input_csv}: {e}")
        sys.exit(9)

    if predictions_df.empty:
        _log("Arquivo predictions.csv está vazio, gerando DataFrame vazio")
        predictions_df = pd.DataFrame(columns=['team_home', 'team_away', 'prob_home', 'prob_draw', 'prob_away'])

    # Carregar calibrador
    calibrator = None
    if os.path.isfile(args.cal):
        try:
            with open(args.cal, 'rb') as f:
                calibrator = pickle.load(f)
            _log(f"Calibrador carregado de {args.cal}")
        except Exception as e:
            _log(f"Erro ao carregar calibrador {args.cal}: {e}, prosseguindo sem calibrador")
    else:
        _log(f"Calibrador {args.cal} não encontrado, prosseguindo sem calibrador")

    df = calibrate_probs(predictions_df, calibrator)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    _log(f"Arquivo {args.out} gerado com {len(df)} jogos")

if __name__ == "__main__":
    main()