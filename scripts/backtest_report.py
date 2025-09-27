# scripts/backtest_report.py
# Relatório de calibração (1X2) + compatibilidade de caminhos legados
from __future__ import annotations
import argparse, base64, io, warnings, shutil
from pathlib import Path
import numpy as np
import pandas as pd

try:
    import matplotlib.pyplot as plt
    HAS_MPL = True
except Exception:
    HAS_MPL = False

CLASSES = ["1", "X", "2"]

def _safe_probs(df: pd.DataFrame) -> pd.DataFrame:
    P = df[["p_home","p_draw","p_away"]].to_numpy(dtype=float, copy=True)
    P = np.clip(P, 1e-9, 1.0)
    P /= P.sum(axis=1, keepdims=True)
    return pd.DataFrame(P, columns=["p_home","p_draw","p_away"])

def _onehot(y: pd.Series) -> np.ndarray:
    y = y.astype(str).str.upper().str.strip()
    Y = np.zeros((len(y), 3), dtype=float)
    mapping = {"1":0, "X":1, "2":2}
    for i, v in enumerate(y):
        if v in mapping:
            Y[i, mapping[v]] = 1.0
    return Y

def brier_multiclass(P: np.ndarray, Y: np.ndarray) -> float:
    return float(np.mean(np.sum((P - Y)**2, axis=1))) if len(P) else 0.0

def logloss_multiclass(P: np.ndarray, Y: np.ndarray) -> float:
    if len(P)==0: return 0.0
    idx = np.argmax(Y, axis=1)
    chosen = np.clip(P[np.arange(len(P)), idx], 1e-12, 1.0)
    return float(-np.mean(np.log(chosen)))

def top1_accuracy(P: np.ndarray, Y: np.ndarray) -> float:
    if len(P)==0: return 0.0
    return float((np.argmax(P,axis=1)==np.argmax(Y,axis=1)).mean())

def reliability_bins(Pk: np.ndarray, Yk: np.ndarray, n_bins: int = 10) -> pd.DataFrame:
    n = len(Pk)
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    if n == 0:
        return pd.DataFrame({"bin": list(range(1,n_bins+1)), "p_mean": [np.nan]*n_bins, "y_rate": [np.nan]*n_bins, "count": [0]*n_bins})
    Pk = np.asarray(Pk, dtype=float); Yk = np.asarray(Yk, dtype=float)
    bins = np.digitize(Pk, edges[1:-1], right=True)  # 0..n_bins-1
    rows=[]
    for b in range(n_bins):
        mask = (bins==b)
        if mask.any():
            rows.append({"bin": b+1, "p_mean": float(np.mean(Pk[mask])), "y_rate": float(np.mean(Yk[mask])), "count": int(mask.sum())})
        else:
            rows.append({"bin": b+1, "p_mean": np.nan, "y_rate": np.nan, "count": 0})
    return pd.DataFrame(rows)

def _plot_or_empty(fn):
    try:
        return fn()
    except Exception:
        return b""

def plot_calibration(bin_df: pd.DataFrame, title: str) -> bytes:
    if not HAS_MPL: return b""
    fig, ax = plt.subplots(figsize=(6,5))
    d = bin_df.dropna()
    ax.plot([0,1],[0,1], linestyle="--")
    if not d.empty:
        ax.plot(d["p_mean"], d["y_rate"], marker="o")
    ax.set_xlim(0,1); ax.set_ylim(0,1)
    ax.set_xlabel("Probabilidade prevista (média do bin)")
    ax.set_ylabel("Frequência observada")
    ax.set_title(title)
    buf = io.BytesIO(); fig.tight_layout(); fig.savefig(buf, format="png", dpi=140); plt.close(fig)
    return buf.getvalue()

def plot_hist(Pk: np.ndarray, title: str) -> bytes:
    if not HAS_MPL: return b""
    fig, ax = plt.subplots(figsize=(6,4))
    if len(Pk)>0: ax.hist(Pk, bins=20)
    ax.set_xlabel("Probabilidade prevista"); ax.set_ylabel("Contagem"); ax.set_title(title)
    buf = io.BytesIO(); fig.tight_layout(); fig.savefig(buf, format="png", dpi=140); plt.close(fig)
    return buf.getvalue()

def embed_img_html(png: bytes, caption: str) -> str:
    if not png: return f"<p><em>(Gráfico indisponível)</em> — {caption}</p>"
    b64 = base64.b64encode(png).decode("ascii")
    return f'<figure><img src="data:image/png;base64,{b64}" alt="{caption}"><figcaption>{caption}</figcaption></figure>'

def _write_placeholders(outdir: Path, msg: str):
    outdir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([], columns=["metric","value"]).to_csv(outdir/"metrics.csv", index=False)
    pd.DataFrame([], columns=["n_samples","brier_multiclass","logloss_multiclass","top1_accuracy"]).to_csv(outdir/"calib_summary.csv", index=False)
    pd.DataFrame([], columns=["class","bin","p_mean","y_rate","count"]).to_csv(outdir/"reliability_bins.csv", index=False)
    (outdir/"report.html").write_text(f"<html><body><h1>Relatório de Calibração</h1><p>{msg}</p></body></html>", encoding="utf-8")

def _compat_copies(outdir: Path):
    """Cria cópias de compatibilidade em ./metrics.csv, data/history/metrics.csv e data/history/reliability.csv."""
    # Cópia raiz
    try:
        shutil.copyfile(outdir/"metrics.csv", Path("metrics.csv"))
        print("[report] Compat: cópia criada em ./metrics.csv")
    except Exception as e:
        print(f"[report] Compat: falha ao copiar ./metrics.csv: {e}")
    # Legado em data/history/*
    try:
        hist_dir = Path("data/history"); hist_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(outdir/"metrics.csv", hist_dir/"metrics.csv")
        print("[report] Compat: cópia criada em data/history/metrics.csv")
        # reliability.csv legado vem de reliability_bins.csv
        src_rel = outdir/"reliability_bins.csv"
        dst_rel = hist_dir/"reliability.csv"
        if src_rel.exists() and src_rel.stat().st_size>0:
            shutil.copyfile(src_rel, dst_rel)
        else:
            pd.DataFrame([], columns=["bin","p_mean","y_rate","count"]).to_csv(dst_rel, index=False)
        print("[report] Compat: cópia criada/placeholder em data/history/reliability.csv")
    except Exception as e:
        print(f"[report] Compat: falha ao criar cópias legadas: {e}")

def main():
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    ap = argparse.ArgumentParser(description="Relatório de calibração 1X2 (backtest)")
    ap.add_argument("--history-path", default="data/history/calibration.csv")
    ap.add_argument("--bins", type=int, default=10)
    args = ap.parse_args()

    hist_path = Path(args.history_path)
    outdir = Path("data/history/report"); outdir.mkdir(parents=True, exist_ok=True)

    if not hist_path.exists() or hist_path.stat().st_size == 0:
        _write_placeholders(outdir, "Histórico vazio.")
        _compat_copies(outdir)
        print(f"[report] histórico ausente/vazio. Placeholders gerados em {outdir}")
        print(f"[report] OK -> {outdir/'report.html'}")
        print(f"[report] Resumo -> {outdir/'calib_summary.csv'}")
        print(f"[report] Bins   -> {outdir/'reliability_bins.csv'}")
        print(f"[report] Metrics-> {outdir/'metrics.csv'}")
        return

    df = pd.read_csv(hist_path)
    need = {"p_home","p_draw","p_away","resultado"}
    if not need.issubset(df.columns):
        _write_placeholders(outdir, "Colunas necessárias ausentes.")
        _compat_copies(outdir)
        print(f"[report] colunas necessárias ausentes em {hist_path}. Placeholders gerados.")
        print(f"[report] OK -> {outdir/'report.html'}")
        print(f"[report] Resumo -> {outdir/'calib_summary.csv'}")
        print(f"[report] Bins   -> {outdir/'reliability_bins.csv'}")
        print(f"[report] Metrics-> {outdir/'metrics.csv'}")
        return

    df = df.dropna(subset=["resultado"]).copy()
    Pdf = _safe_probs(df)
    P = Pdf[["p_home","p_draw","p_away"]].to_numpy()
    Y = _onehot(df["resultado"])

    n     = int(len(df))
    brier = brier_multiclass(P, Y)
    ll    = logloss_multiclass(P, Y)
    acc   = top1_accuracy(P, Y)

    # Métricas por classe + bins
    bins_map = {}
    for k, cls in enumerate(CLASSES):
        Pk = P[:,k] if n>0 else np.array([])
        Yk = Y[:,k] if n>0 else np.array([])
        bins_map[cls] = reliability_bins(Pk, Yk, n_bins=args.bins)

    # CSVs oficiais
    pd.DataFrame([{
        "n_samples": n,
        "brier_multiclass": round(brier,6),
        "logloss_multiclass": round(ll,6),
        "top1_accuracy": round(acc,6),
    }]).to_csv(outdir/"calib_summary.csv", index=False)

    bins_out=[]
    for cls, bdf in bins_map.items():
        tmp=bdf.copy(); tmp.insert(0,"class",cls); bins_out.append(tmp)
    pd.concat(bins_out, ignore_index=True).to_csv(outdir/"reliability_bins.csv", index=False)

    pd.DataFrame([
        {"metric":"n_samples","value":n},
        {"metric":"brier_multiclass","value":round(brier,6)},
        {"metric":"logloss_multiclass","value":round(ll,6)},
        {"metric":"top1_accuracy","value":round(acc,6)},
    ]).to_csv(outdir/"metrics.csv", index=False)

    # Compat (raiz + legados)
    _compat_copies(outdir)

    # Gráficos
    imgs={}
    for k, cls in enumerate(CLASSES):
        imgs[f"calibration_{cls}.png"] = _plot_or_empty(lambda: plot_calibration(bins_map[cls], f"Curva de Calibração — {cls}"))
        with open(outdir/f"calibration_{cls}.png","wb") as f: f.write(imgs[f"calibration_{cls}.png"])
        imgs[f"hist_{cls}.png"] = _plot_or_empty(lambda: plot_hist(P[:,k] if n>0 else np.array([]), f"Histograma p({cls})"))
        with open(outdir/f"hist_{cls}.png","wb") as f: f.write(imgs[f"hist_{cls}.png"])

    # HTML
    html = io.StringIO()
    html.write("<!doctype html><html><head><meta charset='utf-8'><title>Relatório de Calibração Loteca</title>")
    html.write("<style>body{font-family:Arial,Helvetica,sans-serif;margin:24px;max-width:980px} figure{margin:0 0 18px 0} figcaption{font-size:12px;color:#555}</style>")
    html.write("</head><body>")
    html.write("<h1>Relatório de Calibração — Loteca</h1>")
    html.write(f"<p><b>Amostras:</b> {n} &nbsp;|&nbsp; <b>Brier:</b> {round(brier,6)} &nbsp;|&nbsp; <b>LogLoss:</b> {round(ll,6)} &nbsp;|&nbsp; <b>Top-1 Acc:</b> {round(acc,6)}</p>")
    html.write("<h2>Curvas de Calibração</h2>")
    for cls in CLASSES:
        html.write(embed_img_html(imgs.get(f"calibration_{cls}.png", b""), f"Curva de calibração — {cls}"))
    html.write("<h2>Distribuição das Probabilidades Previstas</h2>")
    for cls in CLASSES:
        html.write(embed_img_html(imgs.get(f"hist_{cls}.png", b""), f"Histograma de p({cls})"))
    html.write("<hr><p><small>Gerado por backtest_report.py</small></p></body></html>")
    (outdir/"report.html").write_text(html.getvalue(), encoding="utf-8")

    print(f"[report] OK -> {outdir/'report.html'}")
    print(f"[report] Resumo -> {outdir/'calib_summary.csv'}")
    print(f"[report] Bins   -> {outdir/'reliability_bins.csv'}")
    print(f"[report] Metrics-> {outdir/'metrics.csv'}")
    print("[report] Compat: cópias também em ./metrics.csv, data/history/metrics.csv e data/history/reliability.csv")

if __name__ == "__main__":
    main()
