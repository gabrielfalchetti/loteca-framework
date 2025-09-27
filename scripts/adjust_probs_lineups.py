# scripts/adjust_probs_lineups.py
# Ajuste de probabilidades (lineups/weather/referee).
# Versão robusta: se 'joined.csv' não existir, cria um joined mínimo a partir de matches.csv + odds.csv.
# Saídas: joined_enriched.csv (e um joined.csv compatível caso não exista).

from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

RNG = np.random.default_rng(7)

def _from_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Cria p_home/p_draw/p_away a partir de odd_home/odd_draw/odd_away."""
    if not {"odd_home","odd_draw","odd_away"}.issubset(df.columns):
        # fallback uniforme
        n = len(df)
        df["p_home"] = 1/3.0
        df["p_draw"] = 1/3.0
        df["p_away"] = 1/3.0
        return df
    odds = df[["odd_home","odd_draw","odd_away"]].to_numpy(dtype=float, copy=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0 / odds
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum(axis=1, keepdims=True)
    P = np.divide(inv, np.where(s>0, s, 1.0))
    P = np.clip(P, 1e-9, 1.0)
    P = P / P.sum(axis=1, keepdims=True)
    df["p_home"], df["p_draw"], df["p_away"] = P[:,0], P[:,1], P[:,2]
    return df

def _ensure_probs(df: pd.DataFrame) -> pd.DataFrame:
    """Garante colunas p_home/p_draw/p_away (usa odds ou uniforme)."""
    have_p = {"p_home","p_draw","p_away"}.issubset(df.columns)
    if not have_p:
        df = _from_odds(df)
    else:
        P = df[["p_home","p_draw","p_away"]].to_numpy(dtype=float, copy=True)
        P = np.clip(P, 1e-9, 1.0)
        P = P / P.sum(axis=1, keepdims=True)
        df["p_home"], df["p_draw"], df["p_away"] = P[:,0], P[:,1], P[:,2]
    return df

def _pick_base_joined(base: Path) -> pd.DataFrame:
    """Escolhe a melhor base disponível. Se não houver joined, cria a partir de matches+odds."""
    # preferências (se existir, usa)
    for name in ["joined_enriched.csv", "joined_referee.csv", "joined_weather.csv", "joined.csv"]:
        p = base / name
        if p.exists() and p.stat().st_size > 0:
            df = pd.read_csv(p)
            return df

    # criar joined mínimo
    mpath = base / "matches.csv"
    if not mpath.exists() or mpath.stat().st_size == 0:
        raise RuntimeError(f"[adjust] matches.csv ausente/vazio: {mpath}")

    matches = pd.read_csv(mpath).rename(columns=str.lower)
    if not {"match_id","home","away"}.issubset(matches.columns):
        # tenta mapear variações
        ren = {}
        if "home_team" in matches.columns: ren["home_team"] = "home"
        if "away_team" in matches.columns: ren["away_team"] = "away"
        matches = matches.rename(columns=ren)
    if not {"match_id","home","away"}.issubset(matches.columns):
        raise RuntimeError("[adjust] matches.csv inválido; precisa de colunas: match_id,home,away")

    opath = base / "odds.csv"
    if opath.exists() and opath.stat().st_size > 0:
        odds = pd.read_csv(opath).rename(columns=str.lower)
        need = {"match_id","odd_home","odd_draw","odd_away"}
        if need.issubset(odds.columns):
            df = matches.merge(odds[list(need)], on="match_id", how="left")
        else:
            df = matches.copy()
    else:
        df = matches.copy()

    df = _ensure_probs(df)
    return df

def _apply_adjustments(df: pd.DataFrame,
                       country_hint: str | None,
                       alpha: float = 0.05) -> pd.DataFrame:
    """
    Espaço para ajustes de lineups/clima/árbitro.
    Nesta versão robusta, se não houver sinais concretos, mantemos as probabilidades.
    Você pode plugar sinais reais depois; aqui só garantimos que nada quebra.
    """
    # Copiamos as colunas de probabilidade para outras com sufixo _adj (pós-ajuste)
    out = df.copy()
    for col in ["p_home","p_draw","p_away"]:
        if col not in out.columns:
            raise RuntimeError(f"[adjust] coluna de probabilidade ausente: {col}")
    # Exemplo de placeholder: nenhum ajuste -> _adj = original
    out["p_home_adj"] = out["p_home"].astype(float)
    out["p_draw_adj"] = out["p_draw"].astype(float)
    out["p_away_adj"] = out["p_away"].astype(float)

    # Sanidade final: clip + renormaliza
    P = out[["p_home_adj","p_draw_adj","p_away_adj"]].to_numpy(dtype=float, copy=True)
    P = np.clip(P, 1e-9, 1.0)
    P = P / P.sum(axis=1, keepdims=True)
    out["p_home_adj"], out["p_draw_adj"], out["p_away_adj"] = P[:,0], P[:,1], P[:,2]

    # Atualiza odds "implícitas" pós-ajuste para referência
    out["odd_home_adj"] = 1.0 / out["p_home_adj"]
    out["odd_draw_adj"] = 1.0 / out["p_draw_adj"]
    out["odd_away_adj"] = 1.0 / out["p_away_adj"]

    return out

def main():
    ap = argparse.ArgumentParser(description="Ajuste de probabilidades (lineups/weather/referee) — robusto a ausência de joined.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--country-hint", default=None)
    ap.add_argument("--days-window", type=int, default=2)
    ap.add_argument("--min-match", type=int, default=85)
    ap.add_argument("--alpha", type=float, default=0.05)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    # 1) carrega ou constrói joined base
    df = _pick_base_joined(base)
    # normaliza cabeçalhos principais
    df = df.rename(columns=str.lower)
    # garante campos essenciais
    if "match_id" not in df.columns:
        raise RuntimeError("[adjust] arquivo base sem match_id.")
    if "home" not in df.columns or "away" not in df.columns:
        # tenta recuperar de matches.csv
        try:
            m = pd.read_csv(base/"matches.csv").rename(columns=str.lower)
            if "home_team" in m.columns and "home" not in df.columns: m = m.rename(columns={"home_team":"home"})
            if "away_team" in m.columns and "away" not in df.columns: m = m.rename(columns={"away_team":"away"})
            df = df.merge(m[["match_id","home","away"]], on="match_id", how="left", suffixes=("","_m"))
            if "home" not in df.columns and "home_m" in df.columns: df["home"] = df["home_m"]
            if "away" not in df.columns and "away_m" in df.columns: df["away"] = df["away_m"]
            for c in ["home_m","away_m"]:
                if c in df.columns: df = df.drop(columns=[c])
        except Exception:
            pass

    # 2) garante p_home/p_draw/p_away
    df = _ensure_probs(df)

    # 3) aplica ajustes (placeholder seguro; personalize com seus sinais)
    enriched = _apply_adjustments(df, args.country_hint, alpha=args.alpha)

    # 4) salva saídas
    out_enriched = base / "joined_enriched.csv"
    enriched.to_csv(out_enriched, index=False)
    print(f"[adjust] OK -> {out_enriched}")

    # mantém joined.csv compatível para passos posteriores (se não existir)
    joined_csv = base / "joined.csv"
    if not joined_csv.exists() or joined_csv.stat().st_size == 0:
        # cria um joined básico com colunas padrão
        basic = enriched.copy()
        for c_from, c_to in [("p_home_adj","p_home"),("p_draw_adj","p_draw"),("p_away_adj","p_away"),
                             ("odd_home_adj","odd_home"),("odd_draw_adj","odd_draw"),("odd_away_adj","odd_away")]:
            if c_from in basic.columns:
                basic[c_to] = basic[c_from]
        basic_cols = [c for c in ["match_id","home","away","p_home","p_draw","p_away","odd_home","odd_draw","odd_away"] if c in basic.columns]
        basic[basic_cols].to_csv(joined_csv, index=False)
        print(f"[adjust] joined.csv criado/atualizado -> {joined_csv}")

if __name__ == "__main__":
    main()
