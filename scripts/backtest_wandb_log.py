# scripts/backtest_wandb_log.py
# Lê métricas/tabelas do backtest e loga no Weights & Biases.
from __future__ import annotations
import os
from pathlib import Path
import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Log de backtest no W&B")
    ap.add_argument("--project", default=os.getenv("WANDB_PROJECT","loteca-framework"))
    ap.add_argument("--entity", default=os.getenv("WANDB_ENTITY",""))
    args = ap.parse_args()

    try:
        import wandb
    except Exception:
        print("[wandb_log] wandb não instalado; pulando."); return

    key = os.getenv("WANDB_API_KEY","").strip()
    if not key:
        print("[wandb_log] WANDB_API_KEY não definido; pulando."); return
    wandb.login(key=key)

    metrics_p = Path("data/history/metrics.csv")
    rel_p = Path("data/history/reliability.csv")
    hist_p = Path("data/history/calibration.csv")

    if not metrics_p.exists() or not rel_p.exists():
        print("[wandb_log] arquivos de métricas/reliability ausentes; pulando.")
        return

    run = wandb.init(project=args.project, entity=args.entity, job_type="backtest-report")
    try:
        metrics = pd.read_csv(metrics_p).iloc[0].to_dict()
        wandb.log(metrics)

        # tabela de confiabilidade
        rel_tbl = pd.read_csv(rel_p)
        wandb.log({"reliability_table": wandb.Table(dataframe=rel_tbl)})

        # artefatos
        art = wandb.Artifact("loteca-history", type="dataset")
        if hist_p.exists():
            art.add_file(str(hist_p))
        art.add_file(str(metrics_p))
        art.add_file(str(rel_p))
        wandb.log_artifact(art)

        print("[wandb_log] backtest logado no W&B.")
    finally:
        run.finish()

if __name__ == "__main__":
    main()
