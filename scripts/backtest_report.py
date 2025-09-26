# scripts/backtest_report.py
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

def nll(probs, y_idx):
    p = np.clip(probs[np.arange(len(y_idx)), y_idx], 1e-12, 1.0)
    return float(-np.mean(np.log(p)))

def brier(probs, y_idx, n_classes=3):
    Y = np.zeros((len(y_idx), n_classes), dtype=float)
    Y[np.arange(len(y_idx)), y_idx] = 1.0
    return float(np.mean(np.sum((probs - Y)**2, axis=1)))

def top1_accuracy(probs, y_idx):
    pred = np.argmax(probs, axis=1)
    return float(np.mean((pred == y_idx).astype(float)))

def ece_multiclass(probs, y_idx, bins=10):
    # ECE por max prob (confidence)
    conf = probs.max(axis=1)
    pred = probs.argmax(axis=1)
    correct = (pred == y_idx).astype(float)
    bin_edges = np.linspace(0, 1, bins+1)
    ece = 0.0
    rows=[]
    for i in range(bins):
        lo, hi = bin_edges[i], bin_edges[i+1]
        mask = (conf >= lo) & (conf < hi) if i<bins-1 else (conf >= lo) & (conf <= hi)
        if mask.sum()==0:
            rows.append({"bin": i+1, "count": 0, "conf_mean": (lo+hi)/2, "acc": np.nan, "gap": np.nan})
            continue
        acc = float(correct[mask].mean())
        cmean = float(conf[mask].mean())
        gap = abs(acc - cmean)
        ece += (mask.mean()) * gap
        rows.append({"bin": i+1, "count": int(mask.sum()), "conf_mean": cmean, "acc": acc, "gap": gap})
    return float(ece), pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser(description="Relatório de backtest e calibração")
    ap.add_argument("--history-path", default="data/history/calibration.csv")
    ap.add_argument("--bins", type=int, default=10)
    args = ap.parse_args()

    hist = Path(args.history_path)
    if not hist.exists() or hist.stat().st_size==0:
        raise RuntimeError(f"[backtest] histórico ausente/vazio: {hist}")

    H = pd.read_csv(hist)
    need = {"p_home","p_draw","p_away","resultado"}
    if not need.issubset(H.columns):
        raise RuntimeError(f"[backtest] histórico inválido; precisa de colunas: {need}")

    map_idx = {"1":0,"X":1,"2":2}
    y = np.array([map_idx[str(v).upper()] for v in H["resultado"].values], dtype=int)
    P = H[["p_home","p_draw","p_away"]].values.astype(float)

    metrics = {
        "n": len(H),
        "top1_acc": top1_accuracy(P, y),
        "brier": brier(P, y),
        "nll": nll(P, y),
    }
    ece, rel = ece_multiclass(P, y, bins=args.bins)
    metrics["ece"] = ece

    out_dir = Path("data/history"); out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([metrics]).to_csv(out_dir/"metrics.csv", index=False)
    rel.to_csv(out_dir/"reliability.csv", index=False)

    print("[backtest] métricas salvas em data/history/metrics.csv")
    print("[backtest] reliability salva em data/history/reliability.csv")
    print(metrics)

if __name__ == "__main__":
    main()
