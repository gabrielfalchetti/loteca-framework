#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Publica artefatos principais da rodada no Weights & Biases.

Regras:
- WANDB_API_KEY deve estar no ambiente (obrigatório)
- Projeto default: 'loteca-framework' (ajuste por env WANDB_PROJECT)
- Artefatos enviados (se existirem): whitelist, odds_*, news.csv, weather.csv,
  predictions_market.csv, calibrated_probs.csv, kelly_stakes.csv, cartao_loteca.csv
"""

import os
import sys
import argparse
import glob

EXIT_CODE = 19

def eprint(*a, **k):
    print(*a, file=sys.stderr, **k)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório da rodada")
    parser.add_argument("--commit", action="store_true", help="Finaliza o run")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    api_key = os.environ.get("WANDB_API_KEY", "").strip()
    if not api_key:
        eprint("::error::WANDB_API_KEY ausente (obrigatório).")
        sys.exit(EXIT_CODE)

    import wandb  # import aqui pra falhar cedo se faltar o pacote

    project = os.environ.get("WANDB_PROJECT", "loteca-framework")
    entity = os.environ.get("WANDB_ENTITY")  # opcional

    run = wandb.init(project=project, entity=entity, job_type="publish-artifacts", config={"rodada_dir": args.rodada})
    art = wandb.Artifact(f"rodada-{os.path.basename(args.rodada)}", type="rodada")

    patterns = [
        "matches_whitelist.csv",
        "odds_*.csv",
        "news.csv",
        "weather.csv",
        "predictions_market.csv",
        "calibrated_probs.csv",
        "final_probs.csv",
        "kelly_stakes.csv",
        "cartao_loteca.csv",
        "context_features.csv",
    ]

    total = 0
    for p in patterns:
        for path in glob.glob(os.path.join(args.rodada, p)):
            if os.path.isfile(path):
                art.add_file(path)
                total += 1
                if args.debug:
                    eprint(f"[wandb] add {path}")

    if total == 0:
        eprint("::warning::Nenhum arquivo para logar no W&B. (Continuando mesmo assim)")

    run.log_artifact(art)
    if args.commit:
        run.finish()

    if args.debug:
        eprint("[wandb] Artifact enviado.")

if __name__ == "__main__":
    main()