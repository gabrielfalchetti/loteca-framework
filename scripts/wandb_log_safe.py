#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Publica artefatos/CSV da rodada no Weights & Biases (estrito).
Falha sem chave ou sem arquivos obrigatórios.

Uso:
  python scripts/wandb_log_safe.py --rodada data/out/<ID> --project loteca --entity <sua_org> --debug

Requisitos:
  WANDB_API_KEY no env
  Arquivos esperados (se existirem no diretório): odds_theoddsapi.csv, odds_apifootball.csv,
  odds_consensus.csv, predictions_market.csv, kelly_stakes.csv, injuries.csv, weather.csv, news.csv
  (Se nenhum existir, falha.)
"""

import argparse, os, sys, glob
try:
    import wandb
except Exception as e:
    print(f"[wandb] ERRO: pacote 'wandb' indisponível: {e}", file=sys.stderr); sys.exit(9)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--project", required=True)
    ap.add_argument("--entity", required=False, default=None)
    ap.add_argument("--debug", action="store_true")
    args=ap.parse_args()

    api=os.getenv("WANDB_API_KEY","").strip()
    if not api:
        print("::error::[wandb] WANDB_API_KEY não definido.", file=sys.stderr); sys.exit(9)

    out_dir=args.rodada
    if not os.path.isdir(out_dir):
        print(f"::error::[wandb] Diretório {out_dir} ausente.", file=sys.stderr); sys.exit(9)

    # coleta arquivos conhecidos
    patterns=[
        "odds_theoddsapi.csv","odds_apifootball.csv","odds_consensus.csv",
        "predictions_market.csv","kelly_stakes.csv",
        "injuries.csv","weather.csv","news.csv"
    ]
    files=[os.path.join(out_dir,p) for p in patterns if os.path.isfile(os.path.join(out_dir,p))]
    if not files:
        print("::error::[wandb] Nenhum artefato para publicar.", file=sys.stderr); sys.exit(9)

    run=wandb.init(project=args.project, entity=args.entity, job_type="publish", reinit=True)
    table_files={}
    for fp in files:
        art_name=os.path.basename(fp)
        art=wandb.Artifact(art_name.replace(".csv",""), type="dataset")
        art.add_file(fp)
        run.log_artifact(art)
        table_files[art_name]=fp
    run.finish()
    print(f"[wandb] OK -> publicados: {', '.join(sorted(table_files.keys()))}")

if __name__=="__main__":
    main()