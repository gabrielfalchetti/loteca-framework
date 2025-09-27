# scripts/backtest_build_history.py
# Constrói histórico de calibração a partir de múltiplas rodadas.
# Robusto a fontes: joined_calibrated/referee/weather/enriched/joined/odds
# Usa p_* se existir; senão converte de odds. Aceita results em out/ ou in/.
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

PICK_FILES_ORDER = [
    "joined_calibrated.csv",
    "joined_referee.csv",
    "joined_weather.csv",
    "joined_enriched.csv",
    "joined.csv",
    "odds.csv",
]

def probs_from_odds(arr: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0 / arr.astype(float)
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum(axis=1, keepdims=True)
    P = np.divide(inv, np.where(s > 0, s, 1.0))
    P = np.clip(P, 1e-9, 1.0)
    P = P / P.sum(axis=1, keepdims=True)
    return P

def pick_source_file(base: Path) -> Path | None:
    for name in PICK_FILES_ORDER:
        p = base / name
        if p.exists() and p.stat().st_size > 0:
            return p
    return None

def load_probs(df: pd.DataFrame) -> np.ndarray:
    if {"p_home","p_draw","p_away"}.issubset(df.columns):
        P = df[["p_home","p_draw","p_away"]].values.astype(float)
    elif {"odd_home","odd_draw","odd_away"}.issubset(df.columns):
        P = probs_from_odds(df[["odd_home","odd_draw","odd_away"]].values)
    else:
        # último fallback: tudo 1/3 (não ideal, mas não quebra)
        n = len(df)
        P = np.full((n,3), 1/3.0, dtype=float)
    # saneamento final
    P = np.clip(P, 1e-9, 1.0)
    P = P / P.sum(axis=1, keepdims=True)
    return P

def load_results(rodada: str) -> pd.DataFrame | None:
    # tenta em out/, depois em in/
    out_p = Path(f"data/out/{rodada}/results.csv")
    in_p  = Path(f"data/in/{rodada}/results.csv")
    for p in (out_p, in_p):
        if p.exists() and p.stat().st_size > 0:
            df = pd.read_csv(p)
            # normaliza cabeçalhos
            lower = {c: c.lower() for c in df.columns}
            df = df.rename(columns=lower)
            if "match_id" in df.columns and "resultado" in df.columns:
                df["resultado"] = df["resultado"].astype(str).str.upper().str.strip()
                return df[["match_id","resultado"]].copy()
    return None

def main():
    ap = argparse.ArgumentParser(description="Build histórico para calibração a partir de rodadas")
    ap.add_argument("--rodadas", nargs="+", required=True, help='Ex.: 2025-09-20_21 2025-10-05_14')
    ap.add_argument("--use-calibrated", action="store_true", help="Se existir joined_calibrated.csv, usar preferencialmente")
    args = ap.parse_args()

    rows = []
    for rid in args.rodadas:
        base = Path(f"data/out/{rid}")
        src = pick_source_file(base)
        if src is None:
            print(f"[history] pulando {rid}: joined*/odds ausente")
            continue

        try:
            dj = pd.read_csv(src)
        except Exception as e:
            print(f"[history] pulando {rid}: erro ao ler {src.name}: {e}")
            continue

        if "match_id" not in dj.columns:
            print(f"[history] pulando {rid}: {src.name} sem match_id")
            continue

        # garante probabilidades
        try:
            P = load_probs(dj)
        except Exception as e:
            print(f"[history] pulando {rid}: probs inválidas em {src.name}: {e}")
            continue

        # resultados
        dr = load_results(rid)
        if dr is None:
            print(f"[history] pulando {rid}: results.csv ausente (out/ ou in/)")
            continue

        df = pd.merge(dj, dr, on="match_id", how="inner")
        if df.empty:
            print(f"[history] pulando {rid}: sem interseção entre {src.name} e results.csv")
            continue

        n = len(df)
        # alinhar P às linhas mescladas
        # re-extrai P só para as linhas mescladas (mantendo lógica de load_probs)
        if {"p_home","p_draw","p_away"}.issubset(df.columns):
            P_join = df[["p_home","p_draw","p_away"]].values.astype(float)
        elif {"odd_home","odd_draw","odd_away"}.issubset(df.columns):
            P_join = probs_from_odds(df[["odd_home","odd_draw","odd_away"]].values)
        else:
            P_join = np.full((n,3), 1/3.0, dtype=float)

        P_join = np.clip(P_join, 1e-9, 1.0)
        P_join = P_join / P_join.sum(axis=1, keepdims=True)

        out = pd.DataFrame({
            "rodada": rid,
            "match_id": df["match_id"].astype(int).values,
            "p_home": P_join[:,0],
            "p_draw": P_join[:,1],
            "p_away": P_join[:,2],
            "resultado": df["resultado"].astype(str).str.upper().str.strip().values
        })
        rows.append(out)

    hist_dir = Path("data/history")
    hist_dir.mkdir(parents=True, exist_ok=True)
    hist_path = hist_dir / "calibration.csv"

    if not rows:
        # NÃO quebra o pipeline: grava cabeçalho e sai 0
        pd.DataFrame([], columns=["rodada","match_id","p_home","p_draw","p_away","resultado"]).to_csv(hist_path, index=False)
        print("[history] nenhuma rodada válida; arquivo vazio criado em data/history/calibration.csv")
        return

    hist = pd.concat(rows, ignore_index=True)
    hist = hist.dropna(subset=["p_home","p_draw","p_away","resultado"])
    hist.to_csv(hist_path, index=False)
    print(f"[history] OK -> {hist_path} ({len(hist)} linhas)")

if __name__ == "__main__":
    main()
