# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import os

def _log(msg: str) -> None:
    print(f"[generate_consensus] {msg}", flush=True)

def generate_consensus(theodds_file, apifootball_file, output_file):
    try:
        theodds_df = pd.read_csv(theodds_file)
    except Exception as e:
        _log(f"Erro ao ler {theodds_file}: {e}")
        theodds_df = pd.DataFrame()

    try:
        apifootball_df = pd.read_csv(apifootball_file)
    except Exception as e:
        _log(f"Erro ao ler {apifootball_file}: {e}")
        apifootball_df = pd.DataFrame()

    if theodds_df.empty and apifootball_df.empty:
        _log("Ambos os arquivos de odds estão vazios. Gerando consenso padrão.")
        consensus = pd.DataFrame(columns=['team_home', 'team_away', 'odds_home', 'odds_draw', 'odds_away'])
    else:
        if theodds_df.empty:
            consensus = apifootball_df
        elif apifootball_df.empty:
            consensus = theodds_df
        else:
            consensus = theodds_df.merge(apifootball_df, on=['team_home', 'team_away'], how='outer', suffixes=('_theodds', '_api'))
            consensus['odds_home'] = consensus[['odds_home_theodds', 'odds_home_api']].mean(axis=1).fillna(2.0)
            consensus['odds_draw'] = consensus[['odds_draw_theodds', 'odds_draw_api']].mean(axis=1).fillna(3.0)
            consensus['odds_away'] = consensus[['odds_away_theodds', 'odds_away_api']].mean(axis=1).fillna(2.5)
            consensus = consensus[['team_home', 'team_away', 'odds_home', 'odds_draw', 'odds_away']]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    consensus.to_csv(output_file, index=False)
    _log(f"Consenso de odds salvo em {output_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--theodds_file", required=True)
    ap.add_argument("--apifootball_file", required=True)
    ap.add_argument("--output_file", required=True)
    args = ap.parse_args()

    generate_consensus(args.theodds_file, args.apifootball_file, args.output_file)

if __name__ == "__main__":
    main()