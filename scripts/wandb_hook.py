# scripts/wandb_hook.py
# Uso:
#   python scripts/wandb_hook.py start  --rodada 2025-10-05_14 --project loteca-framework --entity SEU_USUARIO
#   python scripts/wandb_hook.py log    --rodada 2025-10-05_14
from __future__ import annotations
import argparse, os
from pathlib import Path
import json
import pandas as pd

RUNFILE = "data/out/{rodada}/wandb_run_id.txt"

def _safe_read_csv(p: Path):
    if p.exists() and p.stat().st_size > 0:
        try: return pd.read_csv(p)
        except Exception: return None
    return None

def cmd_start(args):
    import wandb
    rodada = args.rodada
    run = wandb.init(
        project=args.project or "loteca-framework",
        entity=args.entity or None,
        name=f"rodada-{rodada}",
        config={
            "rodada": rodada,
            "github_sha": os.getenv("GITHUB_SHA", ""),
            "github_ref": os.getenv("GITHUB_REF", ""),
        },
        resume="allow"
    )
    outdir = Path(f"data/out/{rodada}"); outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "wandb_run_id.txt").write_text(run.id or "")
    print(f"[wandb_hook] started run id={run.id} for rodada={rodada}")

def cmd_log(args):
    import wandb
    rodada = args.rodada
    outdir = Path(f"data/out/{rodada}")
    run_id_path = outdir / "wandb_run_id.txt"
    run_id = run_id_path.read_text().strip() if run_id_path.exists() else None
    run = wandb.init(
        project=args.project or "loteca-framework",
        entity=args.entity or None,
        name=f"rodada-{rodada}",
        id=run_id or None,
        resume="allow"
    )

    # métricas rápidas
    m = _safe_read_csv(outdir / "matches.csv")
    o = _safe_read_csv(outdir / "odds.csv")
    f = _safe_read_csv(outdir / "features.csv")
    j = _safe_read_csv(outdir / "joined.csv")
    c = _safe_read_csv(outdir / "cartao.csv")

    metrics = {
        "n_matches": 0 if m is None else len(m),
        "n_odds_rows": 0 if o is None else len(o),
        "n_features": 0 if f is None else len(f),
        "n_joined": 0 if j is None else len(j),
        "n_cartao": 0 if c is None else len(c),
    }
    # distribuição do cartão
    if c is not None and "tipo" in c.columns:
        metrics.update({
            "cartao_secos": int((c["tipo"] == "SECO").sum()),
            "cartao_duplos": int((c["tipo"] == "DUPLO").sum()),
            "cartao_triplos": int((c["tipo"] == "TRIPLO").sum()),
            "cartao_sem_odds": int((c["tipo"] == "SEM_ODDS").sum())
        })

    wandb.log(metrics)

    # artefatos
    atf = wandb.Artifact(f"rodada-{rodada}", type="loteca")
    for fname in ["matches.csv", "odds.csv", "features.csv", "joined.csv", "cartao.csv"]:
        p = outdir / fname
        if p.exists() and p.stat().st_size > 0:
            atf.add_file(str(p))
    wandb.log_artifact(atf)

    run.finish()
    print(f"[wandb_hook] logged metrics & artifacts for rodada={rodada}")

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_s = sub.add_parser("start")
    ap_s.add_argument("--rodada", required=True)
    ap_s.add_argument("--project", default=os.getenv("WANDB_PROJECT", "loteca-framework"))
    ap_s.add_argument("--entity", default=os.getenv("WANDB_ENTITY", ""))

    ap_l = sub.add_parser("log")
    ap_l.add_argument("--rodada", required=True)
    ap_l.add_argument("--project", default=os.getenv("WANDB_PROJECT", "loteca-framework"))
    ap_l.add_argument("--entity", default=os.getenv("WANDB_ENTITY", ""))

    args = ap.parse_args()
    if args.cmd == "start": cmd_start(args)
    elif args.cmd == "log": cmd_log(args)

if __name__ == "__main__":
    main()
