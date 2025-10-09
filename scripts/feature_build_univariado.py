#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera features univariadas a partir de odds de consenso e (opcionalmente) das
predições do mercado. Saída: <rodada>/features_univariado.csv

Requisitos de entrada (STRICT):
- <rodada>/odds_consensus.csv  com colunas:
  [match_id, team_home, team_away, odds_home, odds_draw, odds_away]

Opcional (se existir):
- <rodada>/predictions_market.csv  com colunas:
  [match_id, prob_home, prob_draw, prob_away] ou [p_home, p_draw, p_away]
"""

import argparse
import os
import sys
import math
import pandas as pd
import numpy as np


def die(msg: str, code: int = 21):
    print(f"[univariado][ERRO] {msg}", file=sys.stderr)
    sys.exit(code)


def read_csv_required(path: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        die(f"Arquivo obrigatório ausente ou vazio: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"Falha lendo {path}: {e}")
    if df.empty:
        die(f"Arquivo sem linhas: {path}")
    return df


def implied_probs_from_odds(odds_home, odds_draw, odds_away):
    """Converte odds decimais em probabilidades implícitas com normalização de vigorish."""
    o = np.array([odds_home, odds_draw, odds_away], dtype=float)
    o[o <= 1.0] = np.nan  # evita divisões por zero/odds inválidas
    with np.errstate(divide="ignore", invalid="ignore"):
        ip = 1.0 / o
    if np.any(np.isnan(ip)):
        return np.nan, np.nan, np.nan
    s = ip.sum()
    if s <= 0:
        return np.nan, np.nan, np.nan
    p = ip / s
    return float(p[0]), float(p[1]), float(p[2])  # p_home, p_draw, p_away


def entropy(p):
    p = np.clip(np.array(p, dtype=float), 1e-12, 1.0)
    return float(-np.sum(p * np.log(p)))


def favorite_side(odds_h, odds_d, odds_a):
    arr = np.array([odds_h, odds_d, odds_a], dtype=float)
    if np.any(arr <= 1.0):
        return "unknown"
    idx = int(np.argmin(arr))
    return ["home", "draw", "away"][idx]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    out_file = os.path.join(rodada, "features_univariado.csv")

    cons_path = os.path.join(rodada, "odds_consensus.csv")
    cons = read_csv_required(cons_path)

    req_cols = {"match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"}
    missing = req_cols - set(map(str, cons.columns))
    if missing:
        die(f"Colunas faltando em odds_consensus.csv: {sorted(missing)}")

    # tenta ler predictions_market (opcional)
    pred_path = os.path.join(rodada, "predictions_market.csv")
    preds = None
    if os.path.exists(pred_path) and os.path.getsize(pred_path) > 0:
        try:
            preds = pd.read_csv(pred_path)
            # normaliza nomes de colunas
            rename = {}
            for a, b in [("p_home", "prob_home"), ("p_draw", "prob_draw"), ("p_away", "prob_away")]:
                if a in preds.columns and b not in preds.columns:
                    rename[a] = b
            if rename:
                preds = preds.rename(columns=rename)
        except Exception as e:
            print(f"[univariado][WARN] Não consegui ler predictions_market.csv ({e}); seguindo sem.", file=sys.stderr)
            preds = None

    rows = []
    for _, r in cons.iterrows():
        mid = r["match_id"]
        oh, od, oa = float(r["odds_home"]), float(r["odds_draw"]), float(r["odds_away"])
        p_home, p_draw, p_away = implied_probs_from_odds(oh, od, oa)

        if any(
            map(
                lambda x: x is None or (isinstance(x, float) and (math.isnan(x) or x <= 0.0)),
                [p_home, p_draw, p_away],
            )
        ):
            # pula linhas inválidas
            continue

        fav = favorite_side(oh, od, oa)
        fav_price = min(oh, od, oa)
        dog_price = max(oh, od, oa)
        price_ratio = float(dog_price / fav_price) if fav_price > 0 else np.nan

        feat = {
            "match_id": mid,
            "team_home": r["team_home"],
            "team_away": r["team_away"],
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
            "imp_p_home": p_home,
            "imp_p_draw": p_draw,
            "imp_p_away": p_away,
            "imp_entropy": entropy([p_home, p_draw, p_away]),
            "favorite_side": fav,
            "fav_price": fav_price,
            "dog_price": dog_price,
            "price_ratio": price_ratio,
            "log_odds_h": float(np.log(oh)),
            "log_odds_d": float(np.log(od)),
            "log_odds_a": float(np.log(oa)),
        }

        # se houver predições do mercado, calcula edges
        if preds is not None and "match_id" in preds.columns:
            pr = preds.loc[preds["match_id"] == mid]
            if not pr.empty:
                pr = pr.iloc[0].to_dict()
                phat_h = float(pr.get("prob_home", np.nan))
                phat_d = float(pr.get("prob_draw", np.nan))
                phat_a = float(pr.get("prob_away", np.nan))
                feat.update(
                    {
                        "mkt_prob_home": phat_h,
                        "mkt_prob_draw": phat_d,
                        "mkt_prob_away": phat_a,
                        "edge_home": phat_h - p_home,
                        "edge_draw": phat_d - p_draw,
                        "edge_away": phat_a - p_away,
                    }
                )

        rows.append(feat)

    if not rows:
        die("Nenhuma linha válida para gerar features_univariado.csv")

    df = pd.DataFrame(rows)

    preferred = [
        "match_id",
        "team_home",
        "team_away",
        "odds_home",
        "odds_draw",
        "odds_away",
        "imp_p_home",
        "imp_p_draw",
        "imp_p_away",
        "imp_entropy",
        "favorite_side",
        "fav_price",
        "dog_price",
        "price_ratio",
        "log_odds_h",
        "log_odds_d",
        "log_odds_a",
        "mkt_prob_home",
        "mkt_prob_draw",
        "mkt_prob_away",
        "edge_home",
        "edge_draw",
        "edge_away",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    os.makedirs(rodada, exist_ok=True)
    df.to_csv(out_file, index=False)
    if args.debug:
        print(f"[univariado] gravado {out_file} ({len(df)} linhas)")
        print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()