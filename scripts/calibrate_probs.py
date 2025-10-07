# scripts/calibrate_probs.py
# -*- coding: utf-8 -*-
"""
Calibração de probabilidades 1X2 com caminhos de fallback robustos.

Entrada prioritária:
  {OUT_DIR}/predictions_market.csv
    - Se tiver prob_home, prob_draw, prob_away -> usa diretamente
    - Senão, tenta p_* de fair probs por odds, ou faz merge com odds_consensus.csv

Fallback:
  {OUT_DIR}/odds_consensus.csv -> converte odds em probs (1/odds) e normaliza

Saída:
  {OUT_DIR}/calibrated_probs.csv
    colunas: match_id, calib_method, calib_home, calib_draw, calib_away
"""

import argparse
import csv
import os
import sys
from typing import Optional, Tuple

import pandas as pd


def log(msg: str, debug: bool = False) -> None:
    if debug:
        print(msg, flush=True)


def read_csv_safe(path: str, debug: bool = False) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        log(f"[calibrate] arquivo não encontrado: {path}", debug)
        return None
    except Exception as e:
        log(f"[calibrate] falha lendo {path}: {e}", debug)
        return None


def implied_from_odds(odd_home, odd_draw, odd_away) -> Tuple[float, float, float]:
    # Converte odds decimais em probabilidades implícitas e normaliza
    vals = []
    for o in (odd_home, odd_draw, odd_away):
        try:
            o = float(o)
            vals.append(0.0 if o <= 0 else 1.0 / o)
        except Exception:
            vals.append(0.0)
    s = sum(vals)
    if s <= 0:
        return 0.0, 0.0, 0.0
    return (vals[0] / s, vals[1] / s, vals[2] / s)


def dirichlet_calibration(p_home: float, p_draw: float, p_away: float, alpha: float = 0.3) -> Tuple[float, float, float]:
    """
    Suavização Dirichlet-like: adiciona pseudo-contagens (alpha) e renormaliza.
    alpha pequeno para não distorcer muito.
    """
    ph = max(p_home, 0.0)
    pd_ = max(p_draw, 0.0)
    pa = max(p_away, 0.0)
    s = ph + pd_ + pa
    if s <= 0:
        ph, pd_, pa = 1/3, 1/3, 1/3
    # aplica suavização
    ph, pd_, pa = ph + alpha, pd_ + alpha, pa + alpha
    s = ph + pd_ + pa
    return ph / s, pd_ / s, pa / s


def ensure_match_id(df: pd.DataFrame, debug: bool = False) -> pd.Series:
    """
    Devolve uma Series 'match_id' coerente:
      - se já existir 'match_id', usa
      - senão, se houver team_home/team_away, monta "home__away"
      - senão, se houver home/away, monta
      - senão, tenta extrair de 'match_key' (ex.: "botafogo-sp__vs__paysandu")
      - caso falhe, gera id incremental baseado no índice
    """
    if "match_id" in df.columns:
        out = df["match_id"].astype(str).fillna("")
        if out.str.len().gt(0).any():
            return out

    def _norm(s):
        return str(s or "").strip()

    home = None
    away = None

    if {"team_home", "team_away"}.issubset(df.columns):
        home = df["team_home"].map(_norm)
        away = df["team_away"].map(_norm)
    elif {"home", "away"}.issubset(df.columns):
        home = df["home"].map(_norm)
        away = df["away"].map(_norm)

    if home is not None and away is not None:
        mi = (home + "__" + away).where(~((home == "") | (away == "")))
    else:
        mi = pd.Series([""] * len(df), index=df.index)

    # tentar extrair de match_key se ainda vazio
    if "match_key" in df.columns:
        mk = df["match_key"].astype(str)
        # padrões comuns: "time-a__vs__time-b" ou "TimeA__TimeB"
        guess = mk.str.replace("__vs__", "__", regex=False)
        mi = mi.where(mi.str.len() > 0, guess)

    # ainda vazio? gera fallback previsível
    if mi.fillna("").str.len().eq(0).any():
        mi = mi.fillna("")
        for i in mi.index[mi.eq("")]:
            mi.at[i] = f"match_{i}"

    if debug:
        sample = mi.head(3).tolist()
        log(f"[calibrate] sample match_id: {sample}", debug)
    return mi


def load_probs_from_predictions(out_dir: str, debug: bool = False) -> Optional[pd.DataFrame]:
    path = os.path.join(out_dir, "predictions_market.csv")
    df = read_csv_safe(path, debug)
    if df is None or df.empty:
        return None

    # Normaliza nomes esperados
    # Tenta detectar colunas de probabilidade
    prob_cols = None
    for trip in [
        ("prob_home", "prob_draw", "prob_away"),
        ("p_home", "p_draw", "p_away"),
        ("fair_p_home", "fair_p_draw", "fair_p_away"),
    ]:
        if set(trip).issubset(df.columns):
            prob_cols = trip
            break

    # Se não tem prob*, mas tem odds no predictions, calcula
    if prob_cols is None and {"odd_home", "odd_draw", "odd_away"}.issubset(df.columns):
        log("[calibrate] predictions_market.csv sem colunas prob_*. Derivando de odds.", debug)
        ph, pd_, pa = zip(*df[["odd_home", "odd_draw", "odd_away"]].apply(
            lambda r: implied_from_odds(r["odd_home"], r["odd_draw"], r["odd_away"]), axis=1
        ))
        df = df.copy()
        df["prob_home"], df["prob_draw"], df["prob_away"] = ph, pd_, pa
        prob_cols = ("prob_home", "prob_draw", "prob_away")

    if prob_cols is None:
        # sem probabilidades e sem odds -> nada a fazer
        return None

    # garante match_id
    df = df.copy()
    df["match_id"] = ensure_match_id(df, debug)
    # renomeia para nomes canônicos
    out = pd.DataFrame({
        "match_id": df["match_id"],
        "prob_home": df[prob_cols[0]].astype(float),
        "prob_draw": df[prob_cols[1]].astype(float),
        "prob_away": df[prob_cols[2]].astype(float),
    })
    return out


def load_probs_from_consensus(out_dir: str, debug: bool = False) -> Optional[pd.DataFrame]:
    path = os.path.join(out_dir, "odds_consensus.csv")
    df = read_csv_safe(path, debug)
    if df is None or df.empty:
        return None
    need = {"team_home", "team_away", "odds_home", "odds_draw", "odds_away"}
    if not need.issubset(df.columns):
        log(f"[calibrate] odds_consensus.csv está sem colunas obrigatórias: {sorted(need - set(df.columns))}", debug)
        return None

    df = df.copy()
    ph, pd_, pa = zip(*df[["odds_home", "odds_draw", "odds_away"]].apply(
        lambda r: implied_from_odds(r["odds_home"], r["odds_draw"], r["odds_away"]), axis=1
    ))
    df["prob_home"], df["prob_draw"], df["prob_away"] = ph, pd_, pa
    df["match_id"] = ensure_match_id(df, debug)
    out = df[["match_id", "prob_home", "prob_draw", "prob_away"]].copy()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--history", required=False, help="(opcional) caminho de histórico/treino", default=None)
    ap.add_argument("--model_path", required=False, help="(opcional) caminho de modelo", default=None)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)
    log("=" * 51, args.debug)
    log("[calibrate] INICIANDO CALIBRAÇÃO DE PROBABILIDADES", args.debug)
    log(f"[calibrate] Diretório de rodada : {out_dir}", args.debug)
    log("=" * 51, args.debug)

    # 1) tenta predictions_market.csv
    dfp = load_probs_from_predictions(out_dir, args.debug)

    # 2) fallback: odds_consensus.csv
    if dfp is None:
        log("[calibrate] predictions_market.csv sem prob*. Usando fallback por odds_consensus.csv.", args.debug)
        dfp = load_probs_from_consensus(out_dir, args.debug)

    # 3) se ainda None, tenta odds do próprio predictions_market (sem prob* e sem fair)
    if dfp is None:
        pm_path = os.path.join(out_dir, "predictions_market.csv")
        dfpm = read_csv_safe(pm_path, args.debug)
        if dfpm is not None and not dfpm.empty and {"odd_home", "odd_draw", "odd_away"}.issubset(dfpm.columns):
            log("[calibrate] Fallback final: odds em predictions_market.csv → probs.", args.debug)
            dfpm = dfpm.copy()
            ph, pd_, pa = zip(*dfpm[["odd_home", "odd_draw", "odd_away"]].apply(
                lambda r: implied_from_odds(r["odd_home"], r["odd_draw"], r["odd_away"]), axis=1
            ))
            dfpm["prob_home"], dfpm["prob_draw"], dfpm["prob_away"] = ph, pd_, pa
            dfpm["match_id"] = ensure_match_id(dfpm, args.debug)
            dfp = dfpm[["match_id", "prob_home", "prob_draw", "prob_away"]].copy()

    if dfp is None or dfp.empty:
        print("::error::[calibrate] Não foi possível derivar probabilidades (predictions_market/odds_consensus ausentes/incompletos).")
        sys.exit(23)

    # Calibração
    rows = []
    for _, r in dfp.iterrows():
        mh = float(r.get("prob_home", 0) or 0)
        md = float(r.get("prob_draw", 0) or 0)
        ma = float(r.get("prob_away", 0) or 0)
        ch, cd, ca = dirichlet_calibration(mh, md, ma, alpha=0.3)
        rows.append({
            "match_id": str(r["match_id"]),
            "calib_method": "Dirichlet",
            "calib_home": ch,
            "calib_draw": cd,
            "calib_away": ca,
        })

    df = pd.DataFrame(rows)

    out = os.path.join(out_dir, "calibrated_probs.csv")
    df[["match_id", "calib_method", "calib_home", "calib_draw", "calib_away"]].to_csv(
        out, index=False, quoting=csv.QUOTE_MINIMAL
    )

    log(f"[calibrate] Salvo em: {out}", args.debug)
    if args.debug:
        try:
            print(df.head(10).to_string(index=False))
        except Exception:
            pass

    print("[ok] Calibração concluída com sucesso.")


if __name__ == "__main__":
    main()