# scripts/feature_engineer.py
# Gera features a partir de resultados históricos.
# Uso:
#   python -m scripts.feature_engineer \
#       --history data/history/results.csv \
#       --out data/history/features.parquet \
#       --ewma 0.20
#
# Saída (parquet):
#   colunas principais:
#     date_utc (datetime64[ns, UTC])  - data/hora da partida
#     team, opponent (str)            - time e adversário
#     is_home (int: 1 casa / 0 fora)
#     gf, ga (int)                    - gols a favor / contra
#     xg_for, xg_against (float, opcional)
#     ewma_gf, ewma_ga, ewma_xg_for, ewma_xg_against, ewma_pts (float)  - suavizações sem vazamento

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional, List, Dict

import numpy as np
import pandas as pd


def _lower_map(cols: List[str]) -> Dict[str, str]:
    """mapa 'nome_em_minusculo' -> 'NomeOriginal' (para acesso case-insensitive)."""
    return {c.strip().lower(): c for c in cols}


def _pick(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Retorna o primeiro nome de coluna existente dentre candidates (case-insensitive)."""
    cmap = _lower_map(list(df.columns))
    for cand in candidates:
        key = cand.strip().lower()
        if key in cmap:
            return cmap[key]
    return None


def read_history(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"history not found: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError("history is empty")

    # Detecta colunas essenciais (home, away, gols, data)
    col_home = _pick(df, ["home", "home_team", "team_home", "mandante"])
    col_away = _pick(df, ["away", "away_team", "team_away", "visitante"])

    if not col_home or not col_away:
        raise KeyError("history must have home/away team columns (e.g., 'home'/'away').")

    # Gols (vários esquemas possíveis)
    col_hg = _pick(df, ["hg", "home_goals", "home_score", "ft_home_goals", "gols_mandante", "placar_mandante"])
    col_ag = _pick(df, ["ag", "away_goals", "away_score", "ft_away_goals", "gols_visitante", "placar_visitante"])

    if not col_hg or not col_ag:
        raise KeyError(
            "history must have home/away goals columns "
            "(e.g., 'home_goals'/'away_goals' ou 'home_score'/'away_score')."
        )

    # xG (opcional)
    col_hxg = _pick(df, ["xg_home", "home_xg", "hxg"])
    col_axg = _pick(df, ["xg_away", "away_xg", "axg"])

    # Data/Hora - tenta vários padrões
    col_dt = _pick(df, ["utc_kickoff", "kickoff", "datetime", "date_utc", "match_date", "date", "data"])
    if not col_dt:
        raise KeyError("history must have a datetime column (e.g., 'utc_kickoff', 'datetime' ou 'date').")

    # Seleciona e renomeia para padrão interno
    use_cols = [col_dt, col_home, col_away, col_hg, col_ag]
    if col_hxg:
        use_cols.append(col_hxg)
    if col_axg:
        use_cols.append(col_axg)

    df = df[use_cols].copy()
    rename_map = {
        col_dt: "date_raw",
        col_home: "home",
        col_away: "away",
        col_hg: "hg",
        col_ag: "ag",
    }
    if col_hxg:
        rename_map[col_hxg] = "hxg"
    if col_axg:
        rename_map[col_axg] = "axg"
    df.rename(columns=rename_map, inplace=True)

    # Converte data/hora para UTC
    df["date_utc"] = pd.to_datetime(df["date_raw"], utc=True, errors="coerce")
    df.drop(columns=["date_raw"], inplace=True)
    df = df.dropna(subset=["date_utc", "home", "away", "hg", "ag"])
    # Tipos
    df["hg"] = pd.to_numeric(df["hg"], errors="coerce").astype("Int64")
    df["ag"] = pd.to_numeric(df["ag"], errors="coerce").astype("Int64")

    if "hxg" in df.columns:
        df["hxg"] = pd.to_numeric(df["hxg"], errors="coerce")
    if "axg" in df.columns:
        df["axg"] = pd.to_numeric(df["axg"], errors="coerce")

    # Remove linhas sem gols válidos
    df = df.dropna(subset=["hg", "ag"])
    df["hg"] = df["hg"].astype(int)
    df["ag"] = df["ag"].astype(int)

    # Somente resultados passados (evita futuros com placar vazio)
    now = pd.Timestamp.utcnow().tz_localize("UTC")
    df = df[df["date_utc"] <= now]

    df.sort_values("date_utc", inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(f"[features] history loaded: rows={len(df)}  cols={list(df.columns)}")
    return df


def long_format(dfw: pd.DataFrame) -> pd.DataFrame:
    """
    Converte base wide (home/away) em long por time.
    Garante colunas: date_utc, team, opponent, is_home, gf, ga, xg_for, xg_against, points
    """
    rows = []
    has_xg = ("hxg" in dfw.columns) or ("axg" in dfw.columns)

    for _, r in dfw.iterrows():
        date_utc = r["date_utc"]
        home = str(r["home"])
        away = str(r["away"])
        hg = int(r["hg"])
        ag = int(r["ag"])
        hxg = float(r["hxg"]) if "hxg" in dfw.columns and pd.notna(r["hxg"]) else np.nan
        axg = float(r["axg"]) if "axg" in dfw.columns and pd.notna(r["axg"]) else np.nan

        # linha mandante
        rows.append({
            "date_utc": date_utc,
            "team": home,
            "opponent": away,
            "is_home": 1,
            "gf": hg,
            "ga": ag,
            "xg_for": hxg if has_xg else np.nan,
            "xg_against": axg if has_xg else np.nan,
            "points": (3 if hg > ag else (1 if hg == ag else 0)),
        })
        # linha visitante
        rows.append({
            "date_utc": date_utc,
            "team": away,
            "opponent": home,
            "is_home": 0,
            "gf": ag,
            "ga": hg,
            "xg_for": axg if has_xg else np.nan,
            "xg_against": hxg if has_xg else np.nan,
            "points": (3 if ag > hg else (1 if ag == hg else 0)),
        })

    out = pd.DataFrame(rows)
    # Tipos
    out["is_home"] = out["is_home"].astype(int)
    out["gf"] = out["gf"].astype(int)
    out["ga"] = out["ga"].astype(int)
    out["points"] = out["points"].astype(int)
    # Ordena
    out.sort_values(["team", "date_utc"], inplace=True)
    out.reset_index(drop=True, inplace=True)

    print(f"[features] long-format built: rows={len(out)}  cols={list(out.columns)}")
    return out


def add_ewma_features(df_long: pd.DataFrame, alpha: float) -> pd.DataFrame:
    """
    Adiciona EWMA sem vazamento (shift antes do ewm).
    """
    alpha = float(alpha)
    if not (0 < alpha <= 1):
        raise ValueError("--ewma deve ser no intervalo (0,1]. Ex.: 0.20")

    df = df_long.copy()
    df.sort_values(["team", "date_utc"], inplace=True)

    def _by_team(group: pd.DataFrame) -> pd.DataFrame:
        # shift para não "ver" o jogo atual
        gf = group["gf"].shift(1)
        ga = group["ga"].shift(1)
        pts = group["points"].shift(1)
        xgf = group["xg_for"].shift(1)
        xga = group["xg_against"].shift(1)

        ewma_gf = gf.ewm(alpha=alpha, adjust=False).mean()
        ewma_ga = ga.ewm(alpha=alpha, adjust=False).mean()
        ewma_pts = pts.ewm(alpha=alpha, adjust=False).mean()

        # xG podem ser todos NaN — trata bem
        if xgf.notna().any():
            ewma_xgf = xgf.ewm(alpha=alpha, adjust=False).mean()
        else:
            ewma_xgf = pd.Series(index=group.index, dtype="float64")
        if xga.notna().any():
            ewma_xga = xga.ewm(alpha=alpha, adjust=False).mean()
        else:
            ewma_xga = pd.Series(index=group.index, dtype="float64")

        group = group.copy()
        group["ewma_gf"] = ewma_gf
        group["ewma_ga"] = ewma_ga
        group["ewma_pts"] = ewma_pts
        group["ewma_xg_for"] = ewma_xgf
        group["ewma_xg_against"] = ewma_xga
        return group

    df = df.groupby("team", group_keys=False).apply(_by_team)

    # Substitui NaN iniciais por 0 (ou mantém NaN — escolha de projeto).
    for c in ["ewma_gf", "ewma_ga", "ewma_pts", "ewma_xg_for", "ewma_xg_against"]:
        if c in df.columns:
            df[c] = df[c].fillna(0.0)

    print("[features] EWMA features added.")
    return df


def save_parquet(df: pd.DataFrame, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    # tenta pyarrow -> fastparquet -> erro
    engine = None
    try:
        import pyarrow  # noqa: F401
        engine = "pyarrow"
    except Exception:
        try:
            import fastparquet  # noqa: F401
            engine = "fastparquet"
        except Exception:
            engine = None

    if engine is None:
        # Último recurso: avisa e tenta mesmo assim (pandas pode lançar)
        print("[features][WARN] Nenhum engine parquet disponível (pyarrow/fastparquet). Tentando gravar assim mesmo...", file=sys.stderr)

    df.to_parquet(out_path, index=False, engine=engine)
    print(f"[features] written: {out_path} rows={len(df)}")


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Feature engineering com EWMA a partir de resultados históricos.")
    p.add_argument("--history", required=True, help="CSV de resultados históricos (wide: home/away).")
    p.add_argument("--out", required=True, help="Caminho do arquivo .parquet de saída.")
    p.add_argument("--ewma", type=float, default=0.20, help="Alpha do EWMA (0<alpha<=1). Ex.: 0.20")
    args = p.parse_args(argv)

    try:
        df_hist = read_history(args.history)
        df_long = long_format(df_hist)
        df_feat = add_ewma_features(df_long, alpha=args.ewma)
        # Ordena e salva
        df_feat.sort_values(["date_utc", "team"], inplace=True)
        save_parquet(df_feat, args.out)
        return 0
    except Exception as e:
        print(f"[features][CRITICAL] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())