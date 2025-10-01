# scripts/upload_artifacts_cli.py
from __future__ import annotations
import argparse, os, shutil

def main() -> None:
    ap = argparse.ArgumentParser(description="Prepara pasta com artifacts novos para upload no Actions.")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--out", default="data/out/{rodada}/ml_new/artifacts")
    args = ap.parse_args()

    out_dir = args.out.format(rodada=args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    # cole aqui os caminhos que quer empacotar
    candidates = [
        f"data/models/ml_new/univariado/model.pkl",
        f"data/models/ml_new/bivariado/model.pkl",
        f"data/models/ml_new/calibracao/isotonic.pkl",
        f"data/models/ml_new/stacking/stack.pkl",
        f"data/out/{args.rodada}/ml_new/preds.csv",
        f"data/out/{args.rodada}/news_new/news.json",
    ]
    copied = 0
    for src in candidates:
        if os.path.exists(src):
            dst = os.path.join(out_dir, os.path.basename(src))
            shutil.copy2(src, dst)
            copied += 1
    print(f"[artifacts] preparados em {out_dir} ({copied} arquivos)")

if __name__ == "__main__":
    main()
