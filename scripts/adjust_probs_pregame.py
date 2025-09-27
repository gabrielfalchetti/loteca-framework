from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

# ======================
# Utilitários
# ======================
def _renorm(P: np.ndarray) -> np.ndarray:
    P = np.clip(P, 1e-9, 1.0)
    S = P.sum(axis=1, keepdims=True)
    S[S <= 0] = 1.0
    return P / S

def _safe_read_csv(p: Path) -> pd.DataFrame | None:
    try:
        if p.exists() and p.stat().st_size > 0:
            return pd.read_csv(p)
    except Exception:
        return None
    return None

def _load_probs(base: Path) -> tuple[pd.DataFrame, list[str], str]:
    """
    Tenta carregar as probabilidades na ordem de preferência.
    Retorna (df, colunas_prob, nome_arquivo_usado)
    """
    tries = [
        ("joined_pregame.csv",       ["p_home_final", "p_draw_final", "p_away_final"]),
        ("joined_stacked_bivar.csv", ["p_home_final", "p_draw_final", "p_away_final"]),
        ("joined_stacked.csv",       ["p_home_final", "p_draw_final", "p_away_final"]),
        ("joined.csv",               ["p_home", "p_draw", "p_away"]),
    ]
    for fn, cols in tries:
        p = base / fn
        if p.exists() and p.stat().st_size > 0:
            df = pd.read_csv(p).rename(columns=str.lower)
            have = [c for c in cols if c in df.columns]
            if len(have) == 3:
                return df.copy(), have, fn
    raise RuntimeError("[pregame] nenhum arquivo de probabilidades encontrado (joined_*).")

# ======================
# Ajustes
# ======================
def _apply_lineups(base: Path, df: pd.DataFrame, P: np.ndarray, cap: float) -> np.ndarray:
    """
    Ajuste por prováveis desfalques (lineups). Espera arquivo:
      data/out/<rodada>/lineups_raw.csv com colunas:
      - match_id, home_missing, away_missing
    """
    lp = base / "lineups_raw.csv"
    ln = _safe_read_csv(lp)
    if ln is None:
        return P

    need = {"match_id", "home_missing", "away_missing"}
    if not need.issubset(ln.columns):
        return P

    df2 = df.merge(ln[list(need)], on="match_id", how="left")
    df2[["home_missing", "away_missing"]] = df2[["home_missing", "away_missing"]].fillna(0)

    miss_h = df2["home_missing"].astype(int).to_numpy()
    miss_a = df2["away_missing"].astype(int).to_numpy()
    fav = np.argmax(P, axis=1)

    for i in range(len(df2)):
        s = 0.0
        if fav[i] == 0 and miss_h[i] >= 1:
            s -= 0.005 if miss_h[i] < 3 else 0.015
        if fav[i] == 2 and miss_a[i] >= 1:
            s -= 0.005 if miss_a[i] < 3 else 0.015
        s = float(np.clip(s, -cap, cap))

        if s != 0.0:
            take = min(P[i, fav[i]] - 1e-6, abs(s))
            if take > 0:
                P[i, fav[i]] -= take
                others = [0, 1, 2]
                others.remove(fav[i])
                P[i, others] += take / 2.0
                P[i] = _renorm(P[i][None, :])[0]
    return P

def _apply_weather(base: Path, df: pd.DataFrame, P: np.ndarray, cap: float) -> np.ndarray:
    """
    Ajuste por clima (tende a aumentar prob de empate em chuva/vento).
    Espera arquivo:
      data/out/<rodada>/weather_raw.csv com colunas:
      - match_id, rain_mm, wind_ms
    """
    wp = base / "weather_raw.csv"
    we = _safe_read_csv(wp)
    if we is None:
        return P

    need = {"match_id", "rain_mm", "wind_ms"}
    if not need.issubset(we.columns):
        return P

    df2 = df.merge(we[list(need)], on="match_id", how="left")
    rain = df2["rain_mm"].fillna(0.0).astype(float).to_numpy()
    wind = df2["wind_ms"].fillna(0.0).astype(float).to_numpy()

    for i in range(len(df2)):
        bonus = 0.0
        if rain[i] > 3.0:
            bonus += 0.008
        if wind[i] > 7.0:
            bonus += 0.007
        bonus = min(bonus, cap)

        if bonus > 0:
            tot = P[i, 0] + P[i, 2]
            red = min(bonus, max(1e-6, tot - 1e-6))
            if tot > 1e-9:
                P[i, 0] -= red * (P[i, 0] / tot)
                P[i, 2] -= red * (P[i, 2] / tot)
                P[i, 1] += red
                P[i] = _renorm(P[i][None, :])[0]
    return P

def _apply_movement(base: Path, df: pd.DataFrame, P: np.ndarray, cap: float) -> np.ndarray:
    """
    Ajuste por movimento de mercado (bolsa/casas).
    Espera arquivo:
      data/out/<rodada>/ex_movement.csv com colunas:
      - match_id, d_home_pp, d_away_pp (variação em p.p.)
    """
    mp = base / "ex_movement.csv"
    mv = _safe_read_csv(mp)
    if mv is None:
        return P

    need = {"match_id", "d_home_pp", "d_away_pp"}
    if not need.issubset(mv.columns):
        return P

    df2 = df.merge(mv[list(need)], on="match_id", how="left")
    dH = df2["d_home_pp"].fillna(0.0).astype(float).to_numpy()
    dA = df2["d_away_pp"].fillna(0.0).astype(float).to_numpy()

    for i in range(len(df2)):
        s, side = 0.0, None
        if dH[i] > 1.5 and dH[i] > dA[i]:
            s, side = min(cap, dH[i] / 100.0), 0
        if dA[i] > 1.5 and dA[i] >= dH[i]:
            s, side = min(cap, dA[i] / 100.0), 2
        if side is not None and s > 0:
            others = [0, 1, 2]
            others.remove(side)
            tot = P[i, others].sum()
            red = min(s, max(1e-6, tot - 1e-6))
            if tot > 1e-9:
                for j in others:
                    P[i, j] -= red * (P[i, j] / tot)
                P[i, side] += red
                P[i] = _renorm(P[i][None, :])[0]
    return P

# ======================
# Main
# ======================
def main():
    ap = argparse.ArgumentParser(description="Ajustes pré-jogo: lineups, clima e movimento")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--cap-lineups", type=float, default=0.02)
    ap.add_argument("--cap-weather", type=float, default=0.015)
    ap.add_argument("--cap-move", type=float, default=0.015)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    # Probabilidades base
    probs_df, prob_cols, used_file = _load_probs(base)
    P = probs_df[prob_cols].to_numpy(float, copy=True)
    P = _renorm(P)

    # Matches (para merge de chaves/metadata)
    mp = base / "matches.csv"
    if not mp.exists() or mp.stat().st_size == 0:
        raise RuntimeError(f"[pregame] matches.csv ausente: {mp}")
    matches = pd.read_csv(mp).rename(columns=str.lower)

    df = probs_df.merge(
        matches[["match_id", "home", "away", "date"]],
        on="match_id", how="left"
    )

    # Ajustes (usando arquivos em base/, sem Series de caminho)
    P = _apply_lineups(base, df, P, cap=float(args.cap_lineups))
    P = _apply_weather(base, df, P, cap=float(args.cap_weather))
    P = _apply_movement(base, df, P, cap=float(args.cap_move))

    # Salva como joined_pregame.csv (sempre p_*_final)
    out = df.copy()
    if prob_cols[0].endswith("_final"):
        out[prob_cols] = P
        out.rename(columns={
            prob_cols[0]: "p_home_final",
            prob_cols[1]: "p_draw_final",
            prob_cols[2]: "p_away_final"
        }, inplace=True)
    else:
        out.rename(columns={
            "p_home": "p_home_final",
            "p_draw": "p_draw_final",
            "p_away": "p_away_final"
        }, inplace=True)
        out[["p_home_final", "p_draw_final", "p_away_final"]] = P

    out_path = base / "joined_pregame.csv"
    out.to_csv(out_path, index=False)
    print(f"[pregame] OK -> {out_path}")

if __name__ == "__main__":
    main()
