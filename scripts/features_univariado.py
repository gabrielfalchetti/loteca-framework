#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
features_univariado.py
----------------------
Gera features univariadas a partir de odds/market para cada jogo.

Entradas (usa o que existir):
- data/out/<rodada>/predictions_market.csv   (preferencial)
  colunas esperadas: match_key,home,away,odd_home,odd_draw,odd_away[,p_home,p_draw,p_away]
- data/out/<rodada>/odds_consensus.csv       (fallback)
  colunas esperadas: team_home,team_away,odds_home,odds_draw,odds_away[,match_key]

Saída:
- data/out/<rodada>/features_univariado.csv

Colunas de saída:
match_key,home,away,odd_home,odd_draw,odd_away,
imp_home,imp_draw,imp_away,overround,
fair_p_home,fair_p_draw,fair_p_away,
gap_home_away,gap_top_second,
logit_imp_home,logit_imp_draw,logit_imp_away,
fav_label,entropy_bits,
value_home,value_draw,value_away,
has_probs,has_all_odds
"""

import argparse
import csv
import math
import os
from typing import Tuple

import pandas as pd


def log(msg: str, debug: bool = False):
    if debug:
        print(f"[univariado] {msg}", flush=True)


def resolve_out_dir(rodada_arg: str) -> str:
    """Aceita tanto um ID (ex.: '17598...') quanto o caminho 'data/out/<id>'."""
    if os.path.isdir(rodada_arg):
        return rodada_arg
    candidate = os.path.join("data", "out", str(rodada_arg))
    if os.path.isdir(candidate):
        return candidate
    # cria se ainda não existir (permite rodar local)
    os.makedirs(candidate, exist_ok=True)
    return candidate


def norm_team(s):
    if s is None:
        return ""
    return str(s).strip()


def build_match_key(home: str, away: str) -> str:
    h = (home or "").strip().lower().replace(" ", "-")
    a = (away or "").strip().lower().replace(" ", "-")
    return f"{h}__vs__{a}"


def implied(odd: float) -> float:
    try:
        odd = float(odd)
        return 0.0 if odd <= 0 else 1.0 / odd
    except Exception:
        return float("nan")


def entropy_bits(p_home: float, p_draw: float, p_away: float) -> float:
    vals = [p_home, p_draw, p_away]
    e = 0.0
    for p in vals:
        if p is None or not (p > 0.0):
            continue
        e -= p * math.log2(p)
    return e


def safe_logit(p: float) -> float:
    # logit(p) = ln(p / (1-p)) com estabilização
    eps = 1e-9
    try:
        p = float(p)
    except Exception:
        return float("nan")
    p = max(eps, min(1.0 - eps, p))
    return math.log(p / (1.0 - p))


def compute_from_odds_row(home: str, away: str, odd_h: float, odd_d: float, odd_a: float) -> dict:
    # probabilidades implícitas
    imp_h = implied(odd_h)
    imp_d = implied(odd_d)
    imp_a = implied(odd_a)
    s_imp = imp_h + imp_d + imp_a

    # overround (soma de implícitas)
    over = s_imp if s_imp > 0 else float("nan")

    # probabilidades "justas" (normalizadas)
    if s_imp > 0:
        fair_h = imp_h / s_imp
        fair_d = imp_d / s_imp
        fair_a = imp_a / s_imp
    else:
        fair_h = fair_d = fair_a = float("nan")

    # gaps/métricas simples
    gap_home_away = (fair_h - fair_a) if (not math.isnan(fair_h) and not math.isnan(fair_a)) else float("nan")

    # top vs second para margem
    probs = [("home", fair_h), ("draw", fair_d), ("away", fair_a)]
    probs_sorted = sorted(probs, key=lambda x: x[1] if x[1] == x[1] else -1, reverse=True)  # NaN fica por último
    if all(not math.isnan(v[1]) for v in probs_sorted):
        top = probs_sorted[0][1]
        second = probs_sorted[1][1]
        gap_top_second = top - second
        fav_label = {"home": 1, "draw": 0, "away": 2}[probs_sorted[0][0]]
    else:
        gap_top_second = float("nan")
        fav_label = ""

    # logits das implícitas (antes de normalizar)
    logit_h = safe_logit(imp_h / s_imp) if s_imp > 0 else float("nan")
    logit_d = safe_logit(imp_d / s_imp) if s_imp > 0 else float("nan")
    logit_a = safe_logit(imp_a / s_imp) if s_imp > 0 else float("nan")

    # entropia das fair probs
    ent = entropy_bits(fair_h, fair_d, fair_a)

    # (placeholders) value bets – podem ser calculadas quando houver projeções próprias
    value_home = 0.0
    value_draw = 0.0
    value_away = 0.0

    return {
        "home": home,
        "away": away,
        "odd_home": odd_h,
        "odd_draw": odd_d,
        "odd_away": odd_a,
        "imp_home": imp_h if s_imp > 0 else float("nan"),
        "imp_draw": imp_d if s_imp > 0 else float("nan"),
        "imp_away": imp_a if s_imp > 0 else float("nan"),
        "overround": over,
        "fair_p_home": fair_h,
        "fair_p_draw": fair_d,
        "fair_p_away": fair_a,
        "gap_home_away": gap_home_away,
        "gap_top_second": gap_top_second,
        "logit_imp_home": logit_h,
        "logit_imp_draw": logit_d,
        "logit_imp_away": logit_a,
        "fav_label": fav_label,
        "entropy_bits": ent,
        "value_home": value_home,
        "value_draw": value_draw,
        "value_away": value_away,
    }


def load_market_first(out_dir: str, debug: bool = False) -> pd.DataFrame:
    """
    Tenta carregar predictions_market.csv (preferido).
    Fallback: odds_consensus.csv -> renomeia colunas e constrói match_key.
    """
    fp_market = os.path.join(out_dir, "predictions_market.csv")
    if os.path.isfile(fp_market):
        df = pd.read_csv(fp_market)
        log(f"usando predictions_market.csv ({fp_market})", debug)

        # garantir nomes/colunas
        ren = {}
        if "team_home" in df.columns and "home" not in df.columns:
            ren["team_home"] = "home"
        if "team_away" in df.columns and "away" not in df.columns:
            ren["team_away"] = "away"
        if ren:
            df = df.rename(columns=ren)

        # odds podem estar com nomes "odd_*" já corretos; se vierem como "odds_*", alinhar
        r2 = {}
        if "odds_home" in df.columns and "odd_home" not in df.columns:
            r2["odds_home"] = "odd_home"
        if "odds_draw" in df.columns and "odd_draw" not in df.columns:
            r2["odds_draw"] = "odd_draw"
        if "odds_away" in df.columns and "odd_away" not in df.columns:
            r2["odds_away"] = "odd_away"
        if r2:
            df = df.rename(columns=r2)

        # construir match_key se necessário
        if "match_key" not in df.columns:
            df["match_key"] = df.apply(lambda r: build_match_key(str(r.get("home","")), str(r.get("away",""))), axis=1)
        else:
            df["match_key"] = df["match_key"].fillna("").astype(str).str.strip()
            mk_empty = df["match_key"].eq("")
            if mk_empty.any():
                df.loc[mk_empty, "match_key"] = df.loc[mk_empty].apply(
                    lambda r: build_match_key(str(r.get("home","")), str(r.get("away",""))), axis=1
                )

        # tipagem básica
        for c in ["odd_home", "odd_draw", "odd_away"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # flags
        df["has_probs"] = ((df.get("p_home").notna()) & (df.get("p_draw").notna()) & (df.get("p_away").notna())).astype(int)
        df["has_all_odds"] = ((df.get("odd_home").notna()) & (df.get("odd_draw").notna()) & (df.get("odd_away").notna())).astype(int)

        # manter apenas colunas de interesse nessa fase
        keep = ["match_key", "home", "away", "odd_home", "odd_draw", "odd_away", "p_home", "p_draw", "p_away", "has_probs", "has_all_odds"]
        for k in keep:
            if k not in df.columns:
                df[k] = pd.NA
        return df[keep].copy()

    # fallback: consensus
    fp_cons = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(fp_cons):
        raise FileNotFoundError("[univariado] Nenhum dos arquivos encontrados: predictions_market.csv nem odds_consensus.csv")

    dfc = pd.read_csv(fp_cons)
    log(f"fallback em odds_consensus.csv ({fp_cons})", debug)

    ren = {}
    if "team_home" in dfc.columns and "home" not in dfc.columns:
        ren["team_home"] = "home"
    if "team_away" in dfc.columns and "away" not in dfc.columns:
        ren["team_away"] = "away"
    if ren:
        dfc = dfc.rename(columns=ren)

    r2 = {}
    if "odds_home" in dfc.columns and "odd_home" not in dfc.columns:
        r2["odds_home"] = "odd_home"
    if "odds_draw" in dfc.columns and "odd_draw" not in dfc.columns:
        r2["odds_draw"] = "odd_draw"
    if "odds_away" in dfc.columns and "odd_away" not in dfc.columns:
        r2["odds_away"] = "odd_away"
    if r2:
        dfc = dfc.rename(columns=r2)

    # match_key
    if "match_key" not in dfc.columns:
        dfc["match_key"] = dfc.apply(lambda r: build_match_key(str(r.get("home","")), str(r.get("away",""))), axis=1)
    else:
        dfc["match_key"] = dfc["match_key"].fillna("").astype(str).str.strip()
        mk_empty = dfc["match_key"].eq("")
        if mk_empty.any():
            dfc.loc[mk_empty, "match_key"] = dfc.loc[mk_empty].apply(
                lambda r: build_match_key(str(r.get("home","")), str(r.get("away",""))), axis=1
            )

    # tipagem básica
    for c in ["odd_home", "odd_draw", "odd_away"]:
        if c in dfc.columns:
            dfc[c] = pd.to_numeric(dfc[c], errors="coerce")

    dfc["has_probs"] = 0  # não há p_* no consensus
    dfc["has_all_odds"] = ((dfc.get("odd_home").notna()) & (dfc.get("odd_draw").notna()) & (dfc.get("odd_away").notna())).astype(int)

    keep = ["match_key", "home", "away", "odd_home", "odd_draw", "odd_away", "has_probs", "has_all_odds"]
    for k in keep:
        if k not in dfc.columns:
            dfc[k] = pd.NA
    return dfc[keep].copy()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID da rodada OU caminho data/out/<id>")
    ap.add_argument("--season", required=False, help="temporada (não usada diretamente aqui, mantida por consistência)")
    ap.add_argument("--debug", action="store_true", help="Imprime logs detalhados")
    args = ap.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    df_base = load_market_first(out_dir, args.debug)

    rows = []
    for _, r in df_base.iterrows():
        mk = str(r.get("match_key", "") or "").strip()
        home = norm_team(r.get("home"))
        away = norm_team(r.get("away"))
        oh = r.get("odd_home")
        od = r.get("odd_draw")
        oa = r.get("odd_away")

        # calcula features a partir das odds
        feats = compute_from_odds_row(home, away, oh, od, oa)
        feats["match_key"] = mk

        # flags/auxiliares
        feats["has_probs"] = int(r.get("has_probs", 0)) if pd.notna(r.get("has_probs", pd.NA)) else 0
        feats["has_all_odds"] = int(r.get("has_all_odds", 0)) if pd.notna(r.get("has_all_odds", pd.NA)) else 0

        rows.append(feats)

    out_df = pd.DataFrame(rows, columns=[
        "match_key",
        "home", "away",
        "odd_home", "odd_draw", "odd_away",
        "imp_home", "imp_draw", "imp_away",
        "overround",
        "fair_p_home", "fair_p_draw", "fair_p_away",
        "gap_home_away", "gap_top_second",
        "logit_imp_home", "logit_imp_draw", "logit_imp_away",
        "fav_label", "entropy_bits",
        "value_home", "value_draw", "value_away",
        "has_probs", "has_all_odds",
    ])

    # ordena por entropia (jogos "mais definidos" primeiro) apenas para visual
    out_df = out_df.sort_values(by=["entropy_bits"], ascending=True, na_position="last").reset_index(drop=True)

    out_path = os.path.join(out_dir, "features_univariado.csv")
    out_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    log(f"OK -> {out_path} ({len(out_df)} jogos)", args.debug)
    if args.debug and len(out_df) > 0:
        print(out_df.head(10).to_string(index=False))
        print(out_df.head(10).to_csv(index=False))


if __name__ == "__main__":
    main()