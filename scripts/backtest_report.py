# scripts/backtest_report.py
# Gera relatório de calibração (1X2) a partir de data/history/calibration.csv
# Saídas: CSVs de métricas/bins e PNGs/HTML com gráficos + METRICS.CSV
from __future__ import annotations
import argparse, base64, io, warnings
from pathlib import Path
import numpy as np
import pandas as pd

# Matplotlib é opcional, mas recomendado para gráficos
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
    out = pd.DataFrame(P, columns=["p_home","p_draw","p_away"])
    return out

def _onehot(y: pd.Series) -> np.ndarray:
    y = y.astype(str).str.upper().str.strip()
    Y = np.zeros((len(y), 3), dtype=float)
    mapping = {"1":0, "X":1, "2":2}
    for i, v in enumerate(y):
        if v in mapping:
            Y[i, mapping[v]] = 1.0
    return Y

def brier_multiclass(P: np.ndarray, Y: np.ndarray) -> float:
    if len(P)==0: return 0.0
    return float(np.mean(np.sum((P - Y)**2, axis=1)))

def logloss_multiclass(P: np.ndarray, Y: np.ndarray) -> float:
    if len(P)==0: return 0.0
    idx = np.argmax(Y, axis=1)  # 0/1/2
    chosen = P[np.arange(len(P)), idx]
    chosen = np.clip(chosen, 1e-12, 1.0)
    return float(-np.mean(np.log(chosen)))

def top1_accuracy(P: np.ndarray, Y: np.ndarray) -> float:
    if len(P)==0: return 0.0
    pred = np.argmax(P, axis=1)
    true = np.argmax(Y, axis=1)
    return float((pred==true).mean())

def reliability_bins(Pk: np.ndarray, Yk: np.ndarray, n_bins: int = 10) -> pd.DataFrame:
    """
    Pk: probs para uma classe (n,)
    Yk: outcomes binários (n,)
    Retorna: DataFrame com colunas [bin, p_mean, y_rate, count]
    """
    n = len(Pk)
    if n == 0:
        return pd.DataFrame({"bin": list(range(1, n_bins+1)),
                             "p_mean": [np.nan]*n_bins,
                             "y_rate": [np.nan]*n_bins,
                             "count":  [0]*n_bins})
    Pk = np.asarray(Pk, dtype=float)
    Yk = np.asarray(Yk, dtype=float)

    edges = np.linspace(0.0, 1.0, n_bins + 1)
    bins = np.digitize(Pk, edges[1:-1], right=True)  # 0..n_bins-1

    rows = []
    for b in range(n_bins):
        mask = (bins == b)
        if mask.any():
            p_mean = float(np.mean(Pk[mask]))
            y_rate = float(np.mean(Yk[mask]))
            cnt = int(mask.sum())
        else:
            p_mean = np.nan; y_rate = np.nan; cnt = 0
        rows.append({"bin": b+1, "p_mean": p_mean, "y_rate": y_rate, "count": cnt})
    return pd.DataFrame(rows)

def plot_calibration(bin_df: pd.DataFrame, title: str) -> bytes:
    if not HAS_MPL:
        return b""
    fig, ax = plt.subplots(figsize=(6, 5))
    d = bin_df.dropna()
    ax.plot([0,1],[0,1], linestyle="--")
    if not d.empty:
        ax.plot(d["p_mean"], d["y_rate"], marker="o")
    ax.set_xlim(0,1); ax.set_ylim(0,1)
    ax.set_xlabel("Probabilidade prevista (média do bin)")
    ax.set_ylabel("Frequência observada")
    ax.set_title(title)
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=140)
    plt.close(fig)
    return buf.getvalue()

def plot_hist(Pk: np.ndarray, title: str) -> bytes:
    if not HAS_MPL:
        return b""
    fig, ax = plt.subplots(figsize=(6, 4))
    if len(Pk) > 0:
        ax.hist(Pk, bins=20)
    ax.set_xlabel("Probabilidade prevista")
    ax.set_ylabel("Contagem")
    ax.set_title(title)
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=140)
    plt.close(fig)
    return buf.getvalue()

def embed_img_html(png_bytes: bytes, caption: str) -> str:
    if not png_bytes:
        return f"<p><em>(Gráfico indisponível: matplotlib não instalado)</em> — {caption}</p>"
    b64 = base64.b64encode(png_bytes).decode("ascii")
    return f'<figure><img src="data:image/png;base64,{b64}" alt="{caption}" /><figcaption>{caption}</figcaption></figure>'

def main():
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    ap = argparse.ArgumentParser(description="Relatório de calibração 1X2 (backtest)")
    ap.add_argument("--history-path", default="data/history/calibration.csv")
    ap.add_argument("--bins", type=int, default=10)
    args = ap.parse_args()

    hist_path = Path(args.history_path)
    if not hist_path.exists() or hist_path.stat().st_size == 0:
        # Gera estrutura mínima para não quebrar pipeline
        outdir = Path("data/history/report"); outdir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([], columns=["metric","value"]).to_csv(outdir/"metrics.csv", index=False)
        pd.DataFrame([], columns=["n_samples","brier_multiclass","logloss_multiclass"]).to_csv(outdir/"calib_summary.csv", index=False)
        pd.DataFrame([], columns=["class","bin","p_mean","y_rate","count"]).to_csv(outdir/"reliability_bins.csv", index=False)
        with open(outdir/"report.html","w",encoding="utf-8") as f:
            f.write("<html><body><h1>Relatório de Calibração</h1><p>Histórico vazio.</p></body></html>")
        print(f"[report] histórico ausente/vazio. Placeholders gerados em {outdir}")
        return

    df = pd.read_csv(hist_path)
    need = {"p_home","p_draw","p_away","resultado"}
    if not need.issubset(df.columns):
        # Estrutura mínima para não quebrar
        outdir = Path("data/history/report"); outdir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([], columns=["metric","value"]).to_csv(outdir/"metrics.csv", index=False)
        pd.DataFrame([], columns=["n_samples","brier_multiclass","logloss_multiclass"]).to_csv(outdir/"calib_summary.csv", index=False)
        pd.DataFrame([], columns=["class","bin","p_mean","y_rate","count"]).to_csv(outdir/"reliability_bins.csv", index=False)
        with open(outdir/"report.html","w",encoding="utf-8") as f:
            f.write("<html><body><h1>Relatório de Calibração</h1><p>Colunas necessárias ausentes.</p></body></html>")
        print(f"[report] colunas necessárias ausentes em {hist_path}. Placeholders gerados.")
        return

    # limpa NAs na coluna resultado
    df = df.dropna(subset=["resultado"]).copy()

    # Probabilidades e rótulos
    Pdf = _safe_probs(df)
    P = Pdf[["p_home","p_draw","p_away"]].to_numpy()
    Y = _onehot(df["resultado"])

    # Métricas globais (seguras para n=0)
    n     = int(len(df))
    brier = brier_multiclass(P, Y)
    ll    = logloss_multiclass(P, Y)
    acc   = top1_accuracy(P, Y)

    # Métricas por classe
    per_class = []
    bins_dfs = {}
    for k, cls in enumerate(CLASSES):
        Pk = P[:,k] if n>0 else np.array([])
        Yk = Y[:,k] if n>0 else np.array([])
        # Brier binário por classe
        if n>0:
            brier_k = float(np.mean((Pk - Yk)**2))
            Pk_clamped = np.clip(Pk, 1e-12, 1.0)
            ll_k = float(-np.mean(Yk*np.log(Pk_clamped) + (1-Yk)*np.log(1-Pk_clamped)))
        else:
            brier_k = 0.0; ll_k = 0.0
        per_class.append({"class": cls, "brier": round(brier_k,6), "logloss": round(ll_k,6)})
        bins_dfs[cls] = reliability_bins(Pk, Yk, n_bins=args.bins)

    # Saídas
    outdir = Path("data/history/report")
    outdir.mkdir(parents=True, exist_ok=True)

    # CSVs básicos
    summary = {
        "n_samples": n,
        "brier_multiclass": round(brier, 6),
        "logloss_multiclass": round(ll, 6),
        "top1_accuracy": round(acc, 6),
    }
    sum_df = pd.DataFrame([summary])
    sum_df.to_csv(outdir/"calib_summary.csv", index=False)

    bins_out = []
    for cls, bdf in bins_dfs.items():
        tmp = bdf.copy()
        tmp.insert(0, "class", cls)
        bins_out.append(tmp)
    pd.concat(bins_out, ignore_index=True).to_csv(outdir/"reliability_bins.csv", index=False)

    # METRICS.CSV (para o seu workflow)
    metrics_rows = [
        {"metric": "n_samples",         "value": n},
        {"metric": "brier_multiclass",  "value": round(brier,6)},
        {"metric": "logloss_multiclass","value": round(ll,6)},
        {"metric": "top1_accuracy",     "value": round(acc,6)},
    ]
    pd.DataFrame(metrics_rows).to_csv(outdir/"metrics.csv", index=False)

    # Gráficos
    imgs = {}
    for k, cls in enumerate(CLASSES):
        imgs[f"calibration_{cls}.png"] = plot_calibration(bins_dfs[cls], f"Curva de Calibração — classe {cls}")
        with open(outdir/f"calibration_{cls}.png", "wb") as f:
            f.write(imgs[f"calibration_{cls}.png"])
        imgs[f"hist_{cls}.png"] = plot_hist(P[:,k] if n>0 else np.array([]), f"Distribuição de p(classe {cls})")
        with open(outdir/f"hist_{cls}.png", "wb") as f:
            f.write(imgs[f"hist_{cls}.png"])

    # HTML simples com gráficos embutidos
    html = io.StringIO()
    html.write("<!doctype html><html><head><meta charset='utf-8'><title>Relatório de Calibração Loteca</title>")
    html.write("<style>body{font-family:Arial,Helvetica,sans-serif;margin:24px;max-width:980px} figure{margin:0 0 18px 0} figcaption{font-size:12px;color:#555}</style>")
    html.write("</head><body>")
    html.write("<h1>Relatório de Calibração — Loteca</h1>")
    html.write(f"<p><b>Amostras:</b> {n} &nbsp;|&nbsp; <b>Brier (multiclasse):</b> {summary['brier_multiclass']} &nbsp;|&nbsp; <b>LogLoss:</b> {summary['logloss_multiclass']} &nbsp;|&nbsp; <b>Top-1 Acc:</b> {summary['top1_accuracy']}</p>")

    html.write("<h2>Métricas por Classe</h2><ul>")
    for row in per_class:
        html.write(f"<li>Classe {row['class']}: Brier={row['brier']}, LogLoss={row['logloss']}</li>")
    html.write("</ul>")

    html.write("<h2>Curvas de Calibração</h2>")
    for cls in CLASSES:
        html.write(embed_img_html(imgs.get(f"calibration_{cls}.png", b""), f"Curva de calibração — {cls}"))
    html.write("<h2>Distribuição das Probabilidades Previstas</h2>")
    for cls in CLASSES:
        html.write(embed_img_html(imgs.get(f"hist_{cls}.png", b""), f"Histograma de p({cls})"))
    html.write("<hr><p><small>Gerado por backtest_report.py</small></p></body></html>")

    report_path = outdir/"report.html"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html.getvalue())

    print(f"[report] OK -> {report_path}")
    print(f"[report] Resumo -> {outdir/'calib_summary.csv'}")
    print(f"[report] Bins   -> {outdir/'reliability_bins.csv'}")
    print(f"[report] Metrics-> {outdir/'metrics.csv'}")

if __name__ == "__main__":
    main()
