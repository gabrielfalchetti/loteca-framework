#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Publicação opcional no Weights & Biases.
- Se WANDB_API_KEY não estiver setada -> sai com 0 e avisa.
- Se a lib wandb não estiver instalada -> sai com 0 e avisa.
- Qualquer erro de runtime -> vira aviso e sai com 0 (nunca quebra o job).

Uso:
  python scripts/wandb_publish.py --rodada data/out/<RID> [--project loteca-framework] [--entity <sua_org>]
"""

from __future__ import annotations
import os
import sys
import argparse
import json
import traceback
from datetime import datetime
from typing import List

SAFE_FILES_CSV = [
    "odds_theoddsapi.csv",
    "odds_apifootball.csv",
    "odds_consensus.csv",
    "predictions_market.csv",
    "features_univariado.csv",
    "features_bivariado.csv",
    "features_xg.csv",
    "predictions_blend.csv",
    "calibrated_probs.csv",
    "kelly_stakes.csv",
    "news.csv",
    "injuries.csv",
    "weather.csv",
]
SAFE_FILES_TXT = [
    "loteca_cartao.txt",
    "reality_report.txt",
]
SAFE_FILES_JSON = [
    "reality_report.json",
]

def notice(msg: str) -> None:
    # Mensagens que não devem derrubar o workflow
    print(f"::notice::{msg}")

def warning(msg: str) -> None:
    # Avisos “amarelos” no GitHub Actions
    print(f"::warning::{msg}")

def info(msg: str) -> None:
    print(f"[wandb] {msg}")

def list_existing(base_dir: str, names: List[str]) -> List[str]:
    out: List[str] = []
    for n in names:
        path = os.path.join(base_dir, n)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            out.append(path)
        else:
            warning(f"Arquivo ausente ou vazio: {path}")
    return out

def read_env_config() -> dict:
    # Captura algumas configs do workflow para guardar no run
    cfg_keys = [
        "SEASON", "REGIONS", "BANKROLL", "KELLY_FRACTION", "KELLY_CAP",
        "KELLY_TOP_N", "ROUND_TO", "DEBUG", "RODADA_ID"
    ]
    cfg = {}
    for k in cfg_keys:
        v = os.getenv(k)
        if v is not None:
            cfg[k] = v
    # Metadados do runner (quando existir)
    for k in ["GITHUB_RUN_ID", "GITHUB_RUN_NUMBER", "GITHUB_SHA", "GITHUB_REF", "GITHUB_REPOSITORY"]:
        v = os.getenv(k)
        if v:
            cfg[k] = v
    return cfg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório data/out/<RID>")
    parser.add_argument("--project", default="loteca-framework", help="Nome do projeto no W&B")
    parser.add_argument("--entity", default=None, help="Entity/Org no W&B (opcional)")
    args = parser.parse_args()

    out_dir = args.rodada
    if not os.path.isdir(out_dir):
        warning(f"Diretório da rodada não encontrado: {out_dir}. Nada a publicar.")
        sys.exit(0)

    api_key = os.getenv("WANDB_API_KEY", "").strip()
    if not api_key:
        notice("WANDB_API_KEY ausente. Pulando publicação no Weights & Biases.")
        sys.exit(0)

    # Import dinâmico para permitir rodar sem a dependência
    try:
        import wandb
    except Exception:
        notice("Biblioteca 'wandb' não instalada. Pulando publicação.")
        sys.exit(0)

    # Coleta de artefatos que existirem
    csvs = list_existing(out_dir, SAFE_FILES_CSV)
    txts = list_existing(out_dir, SAFE_FILES_TXT)
    jsons = list_existing(out_dir, SAFE_FILES_JSON)
    any_artifacts = csvs + txts + jsons
    if not any_artifacts:
        warning("Nenhum artefato elegível encontrado para publicar no W&B.")
        sys.exit(0)

    cfg = read_env_config()
    run_name = f"rodada-{os.path.basename(out_dir)}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    # Inicializa sessão
    try:
        wandb.login(key=api_key)
        init_kwargs = dict(project=args.project, name=run_name, config=cfg)
        if args.entity:
            init_kwargs["entity"] = args.entity
        run = wandb.init(**init_kwargs)
    except Exception as e:
        warning(f"Falha ao iniciar sessão W&B: {e}")
        sys.exit(0)

    try:
        # Salva arquivos como Artifact único “rodada”
        art = wandb.Artifact(
            name=f"rodada_{cfg.get('RODADA_ID', os.path.basename(out_dir))}",
            type="loteca_rodada",
            metadata=cfg
        )

        # Adiciona todos os arquivos existentes
        for p in any_artifacts:
            rel = os.path.relpath(p, out_dir)
            art.add_file(p, name=rel)

        run.log({"num_files_uploaded": len(any_artifacts)})

        # Anexa um pequeno “resumo” se existir reality_report.json
        rr_json = os.path.join(out_dir, "reality_report.json")
        if os.path.exists(rr_json) and os.path.getsize(rr_json) > 0:
            try:
                with open(rr_json, "r", encoding="utf-8") as f:
                    data = json.load(f)
                run.log({"reality_report": data})
            except Exception as e:
                warning(f"Não foi possível anexar reality_report.json ao run: {e}")

        run.log_artifact(art)
        info(f"Publicado no W&B: {args.project} / {run.name} | arquivos={len(any_artifacts)}")
    except Exception as e:
        warning(f"Falha ao publicar artefatos no W&B: {e}\n{traceback.format_exc()}")
        # Não derruba o job
    finally:
        try:
            run.finish()
        except Exception:
            pass

    # Sucesso ou “no-op”: nunca quebra o workflow
    sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except SystemExit as se:
        # Permite sys.exit(0) normais
        raise
    except Exception as e:
        # Qualquer erro inesperado -> aviso e exit 0
        warning(f"Erro inesperado no wandb_publish.py: {e}\n{traceback.format_exc()}")
        sys.exit(0)