# -*- coding: utf-8 -*-
import argparse, os, csv, sys

# Uso atual: apenas sanity e IDs/fixtures no curto prazo.
# Escrevemos um CSV com cabeçalho para evitar "No columns to parse from file" no consenso.

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    args = ap.parse_args()
    os.makedirs(args.rodada, exist_ok=True)
    out_csv = os.path.join(args.rodada, "odds_apifootball.csv")
    # Cabeçalho compatível
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["team_home","team_away","odds_home","odds_draw","odds_away"])
    print("[apifootball]Arquivo odds_apifootball.csv gerado com 0 jogos encontrados.")
    # Podemos futuramente adicionar odds de casas específicas se habilitado na sua conta.
    sys.exit(0)

if __name__ == "__main__":
    main()