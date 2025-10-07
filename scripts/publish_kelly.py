# scripts/publish_kelly.py
# -*- coding: utf-8 -*-
"""
Publica stakes via critério de Kelly a partir de odds e probabilidades.

Ordem de fontes de probabilidade:
1) calibrated_probs.csv  -> colunas: match_id, calib_home, calib_draw, calib_away
2) predictions_market.csv-> colunas: (prob_home|p_home|fair_p_home, ...)
3) odds_consensus.csv    -> odds -> prob. implícitas (1/odds normalizado)

Odds de referência: odds_consensus.csv (odds_home, odds_draw, odds_away)
Saída: {OUT_DIR}/kelly_stakes.csv
Colunas: match_key,team_home,team_away,pick,prob,odds,edge,kelly_frac_raw,kelly_frac_applied,stake
"""

import os
import csv
import argparse
from typing import Tuple, Optional

import pandas as pd


def log(msg: str, debug: bool = False):
    if debug:
        print(f"[kelly] {msg}", flush=True)


def read_csv_safe(path: str, debug: bool = False) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        log(f"arquivo não encontrado: {path}", debug)
        return None
    except Exception as e:
        log(f"falha lendo {path}: {e}", debug)
        return None


def implied_from_odds(oh: float, od: float, oa: float) -> Tuple[float, float, float]:
    vals = []
    for o in (oh, od, oa):
        try:
            o = float(o)
            vals.append(0.0 if o <= 0 else 1.0 / o)
        except Exception:
            vals.append(0.0)
    s = sum(vals)
    if s <= 0:
        return (0.0, 0.0, 0.0)
    return (vals[0] / s, vals[1] / s, vals[2] / s)


def ensure_match_id(df: pd.DataFrame) -> pd.Series:
    # tenta manter consistência com outros scripts
    if "match_id" in df.columns:
        ser = df["match_id"].astype(str).fillna("")
        if ser.str.len().gt(0).any():
            return ser

    def _norm(x): return str(x or "").strip()

    if {"team_home", "team_away"}.issubset(df.columns):
        home = df["team_home"].map(_norm)
        away = df["team_away"].map(_norm)
        base = (home + "__" + away).where(~((home == "") | (away == "")))
    elif {"home", "away"}.issubset(df.columns):
        home = df["home"].map(_norm)
        away = df["away"].map(_norm)
        base = (home + "__" + away).where(~((home == "") | (away == "")))
    else:
        base = pd.Series([""] * len(df), index=df.index)

    if "match_key" in df.columns:
        mk = df["match_key"].astype(str).str.replace("__vs__", "__", regex=False)
        base = base.where(base.str.len() > 0, mk)

    base = base.fillna("")
    for i in base.index[base.eq("")]:
        base.at[i] = f"match_{i}"
    return base


def pick_from_probs(p_home, p_draw, p_away, oh, od, oa):
    """
    Define pick 1/X/2 pelo maior valor esperado p*(odds-1); retorna (pick, prob, odds).
    """
    ev_home = p_home * max(oh - 1.0, 0.0)
    ev_draw = p_draw * max(od - 1.0, 0.0)
    ev_away = p_away * max(oa - 1.0, 0.0)

    # argmax
    if ev_home >= ev_draw and ev_home >= ev_away:
        return "HOME", p_home, oh
    elif ev_draw >= ev_home and ev_draw >= ev_away:
        return "DRAW", p_draw, od
    else:
        return "AWAY", p_away, oa


def kelly_fraction(prob: float, odds: float) -> float:
    """
    Kelly clássico: f* = (b*p - q)/b, onde b = odds-1, q = 1-p.
    Retorna 0 se odds <= 1 ou se resultado <= 0.
    """
    try:
        b = float(odds) - 1.0
        p = float(prob)
        q = 1.0 - p
        if b <= 0:
            return 0.0
        f = (b * p - q) / b
        return max(0.0, f)
    except Exception:
        return 0.0


def round_to_step(x: float, step: float) -> float:
    """
    Arredonda 'x' para o múltiplo mais próximo de 'step' (ex.: step=1.0, 0.5, 5.0).
    """
    try:
        step = float(step)
        if step <= 0:
            return float(x)
        return round(x / step) * step
    except Exception:
        return float(x)


def get_env_cfg() -> dict:
    def _get_float(name: str, default: float) -> float:
        v = os.environ.get(name, "")
        try:
            return float(v) if str(v).strip() != "" else default
        except Exception:
            return default

    def _get_int(name: str, default: int) -> int:
        v = os.environ.get(name, "")
        try:
            return int(float(v)) if str(v).strip() != "" else default
        except Exception:
            return default

    return {
        "bankroll": _get_float("BANKROLL", 1000.0),
        "kelly_fraction": _get_float("KELLY_FRACTION", 0.5),
        "kelly_cap": _get_float("KELLY_CAP", 0.1),
        # ROUND_TO é interpretado como PASSO de arredondamento monetário (não casas decimais)
        "round_to": _get_float("ROUND_TO", 1.0),
        "top_n": _get_int("KELLY_TOP_N", 14),
    }


def load_probs(out_dir: str, debug: bool = False) -> Optional[pd.DataFrame]:
    """
    Retorna DF com: match_id, team_home, team_away, p_home, p_draw, p_away
    """
    # 1) calibrated
    calib = read_csv_safe(os.path.join(out_dir, "calibrated_probs.csv"), debug)
    if calib is not None and not calib.empty:
        # Precisamos de nomes de equipes — puxar de consensus se possível
        cons = read_csv_safe(os.path.join(out_dir, "odds_consensus.csv"), debug)
        if cons is not None and not cons.empty:
            cons = cons.copy()
            cons["match_id"] = ensure_match_id(cons)
            cons_names = cons[["match_id", "team_home", "team_away"]].drop_duplicates()
            c = calib.copy()
            # Carimbo consistente
            if {"calib_home", "calib_draw", "calib_away"}.issubset(c.columns):
                c = c.merge(cons_names, on="match_id", how="left")
                c = c.rename(columns={
                    "calib_home": "p_home",
                    "calib_draw": "p_draw",
                    "calib_away": "p_away",
                })
                return c[["match_id", "team_home", "team_away", "p_home", "p_draw", "p_away"]]

    # 2) predictions_market
    pm = read_csv_safe(os.path.join(out_dir, "predictions_market.csv"), debug)
    if pm is not None and not pm.empty:
        df = pm.copy()
        # detectar tripla de prob
        for trip in [
            ("prob_home", "prob_draw", "prob_away"),
            ("p_home", "p_draw", "p_away"),
            ("fair_p_home", "fair_p_draw", "fair_p_away"),
        ]:
            if set(trip).issubset(df.columns):
                df["match_id"] = ensure_match_id(df)
                # nomes de franquias
                if "team_home" not in df.columns and "home" in df.columns:
                    df["team_home"] = df["home"]
                if "team_away" not in df.columns and "away" in df.columns:
                    df["team_away"] = df["away"]
                return df[["match_id", "team_home", "team_away", trip[0], trip[1], trip[2]]].rename(
                    columns={trip[0]: "p_home", trip[1]: "p_draw", trip[2]: "p_away"}
                )

    # 3) odds_consensus -> probs implícitas
    cons = read_csv_safe(os.path.join(out_dir, "odds_consensus.csv"), debug)
    if cons is None or cons.empty:
        return None
    need = {"team_home", "team_away", "odds_home", "odds_draw", "odds_away"}
    if not need.issubset(cons.columns):
        return None
    cons = cons.copy()
    cons["match_id"] = ensure_match_id(cons)
    ph, pd_, pa = zip(*cons[["odds_home", "odds_draw", "odds_away"]].apply(
        lambda r: implied_from_odds(r["odds_home"], r["odds_draw"], r["odds_away"]), axis=1
    ))
    cons["p_home"], cons["p_draw"], cons["p_away"] = ph, pd_, pa
    return cons[["match_id", "team_home", "team_away", "p_home", "p_draw", "p_away"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    cfg = get_env_cfg()
    log(f"config: { {k: cfg[k] for k in ['bankroll','kelly_fraction','kelly_cap','round_to','top_n']} }", args.debug)

    # Carregar odds de referência (para payout) e nomes
    cons = read_csv_safe(os.path.join(out_dir, "odds_consensus.csv"), args.debug)
    if cons is None or cons.empty:
        print("::error::odds_consensus.csv ausente ou vazio.")
        raise SystemExit(25)
    cons = cons.copy()
    cons["match_id"] = ensure_match_id(cons)
    # match_key para a saída
    cons["match_key"] = cons["match_id"].astype(str).str.replace("__", "__vs__", regex=False)

    # Probs
    probs = load_probs(out_dir, args.debug)
    if probs is None or probs.empty:
        print("::error::Não foi possível obter probabilidades de nenhuma fonte.")
        raise SystemExit(25)

    df = cons.merge(probs, on=["match_id", "team_home", "team_away"], how="left")

    # Se ainda houver prob nula (merge falhou por nomes), tentar fallback só por match_id
    missing = df["p_home"].isna() | df["p_draw"].isna() | df["p_away"].isna()
    if missing.any():
        probs_mid = probs[["match_id", "p_home", "p_draw", "p_away"]].drop_duplicates()
        df.loc[missing, ["p_home", "p_draw", "p_away"]] = df.loc[missing].drop(columns=["p_home","p_draw","p_away"])\
            .merge(probs_mid, on="match_id", how="left")[["p_home","p_draw","p_away"]].values

    # Qualquer NaN restante -> usar implícita por odds
    m2 = df["p_home"].isna() | df["p_draw"].isna() | df["p_away"].isna()
    if m2.any():
        ph, pd_, pa = zip(*df.loc[m2, ["odds_home", "odds_draw", "odds_away"]].apply(
            lambda r: implied_from_odds(r["odds_home"], r["odds_draw"], r["odds_away"]), axis=1
        ))
        df.loc[m2, "p_home"] = ph
        df.loc[m2, "p_draw"] = pd_
        df.loc[m2, "p_away"] = pa

    # Montar picks e Kelly
    rows = []
    for _, r in df.iterrows():
        oh, od, oa = float(r["odds_home"]), float(r["odds_draw"]), float(r["odds_away"])
        ph, pd_, pa = float(r["p_home"]), float(r["p_draw"]), float(r["p_away"])

        pick, p_use, o_use = pick_from_probs(ph, pd_, pa, oh, od, oa)
        edge = p_use * (o_use - 1.0) - (1.0 - p_use)
        k_raw = kelly_fraction(p_use, o_use)
        k_applied = min(k_raw * cfg["kelly_fraction"], cfg["kelly_cap"])
        stake = cfg["bankroll"] * k_applied

        # arredondamento por passo (evita TypeError do round com ndigits float)
        stake = round_to_step(stake, cfg["round_to"])

        rows.append({
            "match_key": r["match_key"],
            "team_home": r["team_home"],
            "team_away": r["team_away"],
            "pick": {"HOME": "HOME", "DRAW": "DRAW", "AWAY": "AWAY"}[pick],
            "prob": p_use,
            "odds": o_use,
            "edge": edge,
            "kelly_frac_raw": k_raw,
            "kelly_frac_applied": k_applied,
            "stake": float(stake),
        })

    out = pd.DataFrame(rows)

    # Ordena por stake desc e aplica top_n (mantendo demais com stake=0)
    out = out.sort_values(["stake", "edge"], ascending=[False, False]).reset_index(drop=True)
    if cfg["top_n"] > 0 and len(out) > cfg["top_n"]:
        out.loc[cfg["top_n"]:, ["kelly_frac_applied", "stake"]] = 0.0

    # Grava
    out_path = os.path.join(out_dir, "kelly_stakes.csv")
    out.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    log(f"OK -> {out_path} ({len(out)} linhas)", args.debug)

    # Amostra de debug
    if args.debug:
        log("AMOSTRA pós-join (top 5):", args.debug)
        print(out.head(5).to_dict(orient="records"))


if __name__ == "__main__":
    main()