# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import pickle
import os

def _log(msg: str) -> None:
    print(f"[predict_dynamic] {msg}", flush=True)

def predict(model, state, matches_df: pd.DataFrame) -> pd.DataFrame:
    """Faz previsões com o modelo dinâmico."""
    predictions = []
    for _, row in matches_df.iterrows():
        home_team = row["team_home"]
        away_team = row["team_away"]
        if home_team not in state or away_team not in state:
            _log(f"Times não encontrados: {home_team}, {away_team}")
            continue
        # Placeholder para previsão (substitua com lógica real do modelo)
        predictions.append({
            "match_id": row["match_id"],
            "team_home": home_team,
            "team_away": away_team,
            "p_home": 0.33,
            "p_draw": 0.33,
            "p_away": 0.33
        })

    df = pd.DataFrame(predictions)
    if df.empty:
        _log("Nenhuma previsão gerada — falhando.")
        sys.exit(8)

    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, help="Arquivo PKL do modelo")
    ap.add_argument("--state", required=True, help="Arquivo JSON de estados")
    ap.add_argument("--matches", required=True, help="CSV com jogos")
    ap.add_argument("--out", required=True, help="CSV de saída")
    args = ap.parse_args()

    if not os.path.isfile(args.model):
        _log(f"{args.model} não encontrado")
        sys.exit(8)
    if not os.path.isfile(args.state):
        _log(f"{args.state} não encontrado")
        sys.exit(8)
    if not os.path.isfile(args.matches):
        _log(f"{args.matches} não encontrado")
        sys.exit(8)

    with open(args.model, "rb") as f:
        model = pickle.load(f)
    with open(args.state, "r") as f:
        state = json.load(f)
    matches_df = pd.read_csv(args.matches)

    df = predict(model, state, matches_df)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    _log(f"OK — geradas {len(df)} previsões em {args.out}")

if __name__ == "__main__":
    main()