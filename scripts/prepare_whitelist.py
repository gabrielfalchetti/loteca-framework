#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Prepara o arquivo matches_whitelist.csv a partir dos arquivos de odds.

CORREÇÃO: Garante que o arquivo de saída seja sempre criado, mesmo que
apenas com o cabeçalho, para evitar erros de "File Not Found" no pipeline.
"""

import os
import sys
import argparse
import csv
import pandas as pd

def log(level, msg):
    tag = "" if level == "INFO" else f"[{level}] "
    print(f"[whitelist]{tag}{msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída da rodada (OUT_DIR)")
    args = ap.parse_args()

    out_dir = args.rodada
    wl_path = os.path.join(out_dir, "matches_whitelist.csv")

    log("INFO", "Gerando whitelist a partir dos arquivos de odds...")

    paths = [
        os.path.join(out_dir, 'odds_apifootball.csv'),
        os.path.join(out_dir, 'odds_theoddsapi.csv')
    ]
    
    rows = []
    for p in paths:
        if os.path.exists(p) and os.path.getsize(p) > 50: # Verifica se o arquivo tem mais que o cabeçalho
            try:
                df = pd.read_csv(p)
                df.columns = [c.lower() for c in df.columns]
                
                required_cols = {'match_id', 'home', 'away'}
                if required_cols.issubset(df.columns):
                    df_subset = df[['match_id', 'home', 'away']].dropna()
                    for r in df_subset.itertuples(index=False):
                        rows.append((int(r.match_id), str(r.home), str(r.away)))
            except Exception as e:
                log("WARN", f"Falha ao processar {os.path.basename(p)}: {e}")

    # Garante que o arquivo seja criado mesmo se não houver jogos
    try:
        with open(wl_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['match_id', 'home', 'away'])
            
            if rows:
                seen = set()
                dedup = []
                for mid, h, a in sorted(rows, key=lambda x: x[0]):
                    key = (str(h).lower(), str(a).lower())
                    if key not in seen:
                        seen.add(key)
                        dedup.append((mid, h, a))

                if dedup:
                    writer.writerows(dedup)
                    log("INFO", f"Whitelist gerada com sucesso em {wl_path} com {len(dedup)} jogos.")
                else:
                    log("WARN", "Nenhum jogo único encontrado para criar a whitelist.")
            else:
                log("WARN", "Nenhum dado de jogo encontrado nos arquivos de odds para gerar a whitelist.")

    except Exception as e:
        log("CRITICAL", f"Falha ao escrever em {wl_path}: {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
