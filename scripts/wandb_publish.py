#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
wandb_publish.py
----------------
Publica os principais artefatos e métricas da rodada no Weights & Biases (W&B),
caso o ambiente possua a variável de API configurada (WANDB_API_KEY).

Entrada: arquivos em data/out/<rodada>/
Saída: loga e faz upload automático dos CSVs gerados.
"""

import argparse, os, wandb, pandas as pd

def resolve_out_dir(r):
    if os.path.isdir(r): 
        return r
    p = os.path.join("data", "out", str(r))
    os.makedirs(p, exist_ok=True)
    return p

def log(msg, dbg=False):
    if dbg:
        print(f"[wandb_publish] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = resolve_out_dir(args.rodada)

    # Inicializa projeto no W&B se possível
    try:
        wandb.init(
            project="loteca-framework",
            name=f"rodada-{os.path.basename(out_dir)}",
            config={"rodada": os.path.basename(out_dir)},
            mode="online"
        )
    except Exception as e:
        print(f"::warning::Falha ao iniciar W&B: {e}")
        return

    # lista de arquivos relevantes para logar
    important = [
        "odds_consensus.csv",
        "predictions_market.csv",
        "calibrated_probs.csv",
        "kelly_stakes.csv",
        "loteca_cartao.txt",
        "reality_report.json",
    ]

    for fname in important:
        fpath = os.path.join(out_dir, fname)
        if os.path.isfile(fpath):
            try:
                if fname.endswith(".csv"):
                    df = pd.read_csv(fpath)
                    wandb.log({fname.replace(".csv",""): wandb.Table(dataframe=df)})
                    log(f"Publicado {fname} ({len(df)} linhas)", args.debug)
                else:
                    wandb.save(fpath)
                    log(f"Arquivo de texto salvo: {fname}", args.debug)
            except Exception as e:
                print(f"::warning::Falha ao publicar {fname}: {e}")

    try:
        wandb.finish()
        print("[wandb_publish] publicação concluída com sucesso.")
    except Exception as e:
        print(f"::warning::Erro ao finalizar sessão W&B: {e}")

if __name__ == "__main__":
    main()