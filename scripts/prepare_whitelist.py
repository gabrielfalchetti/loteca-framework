#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Prepara o arquivo matches_whitelist.csv a partir dos arquivos de odds.

Este script consolida os jogos encontrados nos arquivos de odds, remove duplicatas
e cria uma "whitelist" de jogos a serem processados.
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

    if os.path.exists(wl_path) and os.path.getsize(wl_path) > 0:
        log("INFO", "matches_whitelist.csv já existe. Nenhuma ação necessária.")
        return 0

    log("INFO", "matches_whitelist.csv não encontrado ou vazio. Gerando a partir dos arquivos de odds...")

    paths = [
        os.path.join(out_dir, 'odds_apifootball.csv'),
        os.path.join(out_dir, 'odds_theoddsapi.csv')
    ]
    
    rows = []
    for p in paths:
        if os.path.exists(p) and os.path.getsize(p) > 0:
            try:
                df = pd.read_csv(p)
                # Normaliza nomes de colunas para minúsculas para robustez
                df.columns = [c.lower() for c in df.columns]
                
                required_cols = {'match_id', 'home', 'away'}
                if required_cols.issubset(df.columns):
                    df_subset = df[['match_id', 'home', 'away']].dropna()
                    for r in df_subset.itertuples(index=False):
                        rows.append((int(r.match_id), str(r.home), str(r.away)))
                else:
                    log("WARN", f"Arquivo {os.path.basename(p)} não contém as colunas necessárias: {required_cols - set(df.columns)}")
            except Exception as e:
                log("WARN", f"Falha ao processar {os.path.basename(p)}: {e}")

    if not rows:
        log("WARN", "Nenhum dado de jogo encontrado nos arquivos de odds para gerar a whitelist.")
        return 0

    # De-duplica por (home, away), mantendo o primeiro match_id encontrado
    seen = set()
    dedup = []
    # Ordena por match_id para garantir consistência
    for mid, h, a in sorted(rows, key=lambda x: x[0]):
        key = (h, a)
        if key not in seen:
            seen.add(key)
            dedup.append((mid, h, a))

    if dedup:
        try:
            with open(wl_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['match_id', 'home', 'away'])
                writer.writerows(dedup)
            log("INFO", f"Whitelist gerada com sucesso em {wl_path} com {len(dedup)} jogos.")
        except Exception as e:
            log("CRITICAL", f"Falha ao escrever em {wl_path}: {e}")
            return 1
    else:
        log("WARN", "Nenhum jogo único encontrado para criar a whitelist.")

    return 0

if __name__ == "__main__":
    sys.exit(main())
