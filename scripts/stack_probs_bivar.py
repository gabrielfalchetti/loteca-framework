# scripts/stack_probs_bivar.py
# Ensemble 4-fontes:
#  - Consenso de odds (odds.csv: p_home, p_draw, p_away)
#  - xG Poisson univariado (xg_features.csv: p1_xg, px_xg, p2_xg)
#  - Dixon-Coles bivariado (xg_bivar.csv: p1_bv, px_bv, p2_bv)
#  - Modelo de ML (ml_probs.csv: p_home_ml, p_draw_ml, p_away_ml) [OPCIONAL]
# + Calibração isotônica (models/calib_isotonic.pkl) [OPCIONAL]
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

# joblib é opcional (só para calibração). Se não existir, seguimos sem calibração.
try:
    import joblib
except Exception:
    joblib = None

def _safe_probs(df: pd.DataFrame, cols) -> np.ndarray:
    """Extrai colunas -> matriz (n,3), clipa, e normaliza linha a 1. Evita NaN/inf."""
    P = df[list(cols)].to_numpy(dtype=float, copy=True)
    P = np.clip(P, 1e-9, 1.0)
    S = P.sum(axis=1, keepdims=True)
    S[S <= 0] = 1.0
    return P / S

def _apply_isotonic(P: np.ndarray, models) -> np.ndarray:
    """Aplica calibração isotônica por classe (1,X,2). Renormaliza no final."""
    if not models or not isinstance(models, dict):
        return P
    out = P.copy()
    keys = ["1", "X", "2"]
    for i, k in enumerate(keys):
        kind, mdl = models.get(k, ("identity", None))
        if kind == "isotonic" and mdl is not None:
            out[:, i] = mdl.predict(P[:, i])
    s = out.sum(axis=1, keepdims=True)
    s[s <= 0] = 1.0
    return out / s

def _read_required_csv(path: Path, need_cols: set[str], rename_lower=True) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        raise RuntimeError(f"[stack_bivar] arquivo ausente/vazio: {path}")
    df = pd.read_csv(path)
    if rename_lower:
        df = df.rename(columns=str.lower)
    if not need_cols.issubset(df.columns):
        missing = sorted(list(need_cols.difference(set(df.columns))))
        raise RuntimeError(f"[stack_bivar] {path.name} sem colunas necessárias: faltam {missing}")
    return df

def _read_optional_csv(path: Path, want_cols: set[str]) -> pd.DataFrame | None:
    """Lê CSV opcional; retorna None se ausente ou inválido."""
    try:
        if not path.exists() or path.stat().st_size == 0:
            return None
        df = pd.read_csv(path).rename(columns=str.lower)
        if not want_cols.issubset(df.columns):
            return None
        return df
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser(description="Stack odds + xG + Dixon-Coles + ML com calibração opcional")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--w-consensus", type=float, default=0.50, help="peso do consenso de odds")
    ap.add_argument("--w-xg",        type=float, default=0.20, help="peso do xG Poisson univariado")
    ap.add_argument("--w-bivar",     type=float, default=0.20, help="peso do Dixon-Coles bivariado")
    ap.add_argument("--w-ml",        type=float, default=0.10, help="peso do modelo de ML (se existir)")
    ap.add_argument("--calib-path",  default="models/calib_isotonic.pkl", help="arquivo de calibração isotônica (opcional)")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    od_path = base / "odds.csv"
    xg_path = base / "xg_features.csv"
    bv_path = base / "xg_bivar.csv"
    ml_path = base / "ml_probs.csv"  # opcional
    out_path = base / "joined_stacked_bivar.csv"  # mantemos o nome para compatibilidade

    # 1) Ler fontes obrigatórias
    od = _read_required_csv(od_path, {"match_id", "p_home", "p_draw", "p_away"})
    xg = _read_required_csv(xg_path, {"match_id", "p1_xg", "px_xg", "p2_xg"})
    bv = _read_required_csv(bv_path, {"match_id", "p1_bv", "px_bv", "p2_bv"})

    # 2) Ler ML (opcional)
    ml = _read_optional_csv(ml_path, {"match_id", "p_home_ml", "p_draw_ml", "p_away_ml"})

    # 3) Montar data frame base
    # preserva colunas home/away se existirem em odds
    cols_od = ["match_id", "home", "away", "p_home", "p_draw", "p_away"] if {"home", "away"}.issubset(od.columns) \
              else ["match_id", "p_home", "p_draw", "p_away"]
    df = od[cols_od].merge(xg[["match_id", "p1_xg", "px_xg", "p2_xg"]], on="match_id", how="left") \
                    .merge(bv[[c for c in ["match_id", "p1_bv", "px_bv", "p2_bv", "rho_hat"] if c in bv.columns]],
                           on="match_id", how="left")
    if ml is not None:
        df = df.merge(ml[["match_id", "p_home_ml", "p_draw_ml", "p_away_ml"]], on="match_id", how="left")
    else:
        # cria colunas ML nulas para facilitar lógica adiante
        df["p_home_ml"] = np.nan
        df["p_draw_ml"] = np.nan
        df["p_away_ml"] = np.nan

    # 4) Matrizes de prob por fonte (com normalização segura)
    Pco = _safe_probs(df, ["p_home", "p_draw", "p_away"])
    Pxg = _safe_probs(df, ["p1_xg", "px_xg", "p2_xg"])
    Pbv = _safe_probs(df, ["p1_bv", "px_bv", "p2_bv"])

    has_ml = ~(df[["p_home_ml", "p_draw_ml", "p_away_ml"]].isna().any(axis=1))
    if has_ml.any():
        Pml_full = _safe_probs(df, ["p_home_ml", "p_draw_ml", "p_away_ml"])
    else:
        Pml_full = np.full_like(Pco, 1/3.0)  # placeholder; peso será zerado abaixo

    # 5) Pesos (se ML indisponível para todas as linhas, zera w-ml)
    wc = max(0.0, min(1.0, float(args.w_consensus)))
    wx = max(0.0, min(1.0, float(args.w_xg)))
    wb = max(0.0, min(1.0, float(args.w_bivar)))
    wm = max(0.0, min(1.0, float(args.w_ml)))
    if not has_ml.any():
        wm = 0.0

    # se todos pesos 0, usa defaults
    if wc + wx + wb + wm <= 0:
        wc, wx, wb, wm = 0.5, 0.25, 0.25, 0.0

    s = wc + wx + wb + wm
    wc, wx, wb, wm = wc / s, wx / s, wb / s, wm / s

    # 6) Ensemble
    P = wc * Pco + wx * Pxg + wb * Pbv + wm * Pml_full

    # 7) Calibração isotônica (opcional)
    models = None
    cp = Path(args.calib_path)
    if cp.exists() and cp.stat().st_size > 0 and joblib is not None:
        try:
            models = joblib.load(cp)
        except Exception:
            models = None
    if models:
        P = _apply_isotonic(P, models)

    # 8) Salvar
    out = df.copy()
    out["p_home_final"] = P[:, 0]
    out["p_draw_final"] = P[:, 1]
    out["p_away_final"] = P[:, 2]

    out.to_csv(out_path, index=False)
    print(f"[stack_bivar] OK -> {out_path} (w_consensus={wc:.2f}, w_xg={wx:.2f}, w_bivar={wb:.2f}, w_ml={wm:.2f})")

if __name__ == "__main__":
    main()
