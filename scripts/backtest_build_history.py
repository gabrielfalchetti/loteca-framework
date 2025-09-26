# scripts/backtest_build_history.py
# Gera data/history/calibration.csv a partir de múltiplas rodadas:
# - lê data/out/<rodada>/joined.csv (ou joined_calibrated.csv se --use-calibrated)
# - lê data/out/<rodada>/results.csv
# - extrai p_home/p_draw/p_away (se não existirem, converte de odds)
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def probs_from_odds(oh, od, oa):
    arr = np.array([oh,od,oa], dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0/arr
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s<=0: return np.array([np.nan,np.nan,np.nan], dtype=float)
    return inv/s

def main():
    ap = argparse.ArgumentParser(description="Build histórico para calibração a partir de rodadas")
    ap.add_argument("--rodadas", nargs="+", required=True, help='Ex.: 2025-09-20_21 2025-10-05_14')
    ap.add_argument("--use-calibrated", action="store_true", help="Se existir joined_calibrated.csv, usar preferencialmente")
    args = ap.parse_args()

    rows=[]
    for rid in args.rodadas:
        base = Path(f"data/out/{rid}")
        # escolhe arquivo de probabilidades
        cand = []
        if args.use_calibrated and (base/"joined_calibrated.csv").exists():
            cand.append(base/"joined_calibrated.csv")
        cand += [base/"joined_enriched.csv", base/"joined_weather.csv", base/"joined_referee.csv", base/"joined.csv"]
        joined = None
        for c in cand:
            if c.exists() and c.stat().st_size>0:
                joined = c; break
        if joined is None:
            print(f"[history] pulando {rid}: joined* ausente")
            continue

        results = base/"results.csv"
        if not results.exists() or results.stat().st_size==0:
            print(f"[history] pulando {rid}: results.csv ausente")
            continue

        dj = pd.read_csv(joined)
        dr = pd.read_csv(results)
        if "match_id" not in dj.columns or "match_id" not in dr.columns or "resultado" not in dr.columns:
            print(f"[history] pulando {rid}: colunas ausentes"); continue

        df = pd.merge(dj, dr[["match_id","resultado"]], on="match_id", how="inner")
        # obter probabilidades
        if set(["p_home","p_draw","p_away"]).issubset(df.columns):
            P = df[["p_home","p_draw","p_away"]].astype(float).values
        else:
            need = ["odd_home","odd_draw","odd_away"]
            if not set(need).issubset(df.columns):
                print(f"[history] pulando {rid}: faltam odds e p_*"); continue
            P = np.vstack([probs_from_odds(oh,od,oa) for oh,od,oa in df[need].values])

        out = pd.DataFrame({
            "rodada": rid,
            "match_id": df["match_id"].values,
            "p_home": P[:,0],
            "p_draw": P[:,1],
            "p_away": P[:,2],
            "resultado": df["resultado"].astype(str).str.upper().str.strip().values
        })
        rows.append(out)

    if not rows:
        raise RuntimeError("[history] nada para salvar — verifique rodadas e arquivos necessários.")

    hist = pd.concat(rows, ignore_index=True)
    hist = hist.dropna(subset=["p_home","p_draw","p_away","resultado"])
    hist_dir = Path("data/history"); hist_dir.mkdir(parents=True, exist_ok=True)
    hist_path = hist_dir/"calibration.csv"
    hist.to_csv(hist_path, index=False)
    print(f"[history] OK -> {hist_path} ({len(hist)} linhas)")

if __name__ == "__main__":
    main()
