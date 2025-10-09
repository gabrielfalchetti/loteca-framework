#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
calibrate_probs.py

Produz <OUT_DIR>/calibrated_probs.csv aplicando uma calibração Dirichlet
sobre probabilidades vindas (nesta ordem):
  1) predictions_market.csv (p_home/p_draw/p_away ou a partir de odds)
  2) odds_consensus.csv      (a partir de odds)
  3) odds_theoddsapi.csv     (agrega odds por média entre regiões)
  4) matches_whitelist.csv   (fallback uniforme 1/3–1/3–1/3, para não quebrar)

Saída: CSV com colunas:
  match_id,calib_method,calib_home,calib_draw,calib_away

Parâmetros:
  --rodada <OUT_DIR>   (obrigatório)
  --alpha <0..1>       força da suavização (default 0.20)
  --debug              logs detalhados
"""

import argparse
import csv
import os
import sys
from math import isfinite
from statistics import fmean

import pandas as pd


# ---------------- Utils ---------------- #

def ffloat(x, d=0.0):
    try:
        v = float(x)
        return v if isfinite(v) else d
    except Exception:
        return d


def remove_overround(oh: float, od: float, oa: float):
    """Converte odds (com overround) em probabilidades justas."""
    if oh <= 1.01 or od <= 1.01 or oa <= 1.01:
        return (1/3, 1/3, 1/3)
    imp_h = 1.0 / oh
    imp_d = 1.0 / od
    imp_a = 1.0 / oa
    s = imp_h + imp_d + imp_a
    if s <= 0:
        return (1/3, 1/3, 1/3)
    return (imp_h / s, imp_d / s, imp_a / s)


def get_name(df, *cands):
    for c in cands:
        if c in df.columns:
            return c
    return None


def dirichlet_smooth(p_home: float, p_draw: float, p_away: float, alpha: float):
    """
    Calibração tipo Dirichlet: mistura convexa com uniforme (1/3,1/3,1/3).
    alpha = 0  -> sem suavização
    alpha = 1  -> mistura 50/50 com uniforme (pois renormalizamos)
    """
    alpha = max(0.0, min(1.0, float(alpha)))
    u = 1.0 / 3.0
    ch = (1 - alpha) * p_home + alpha * u
    cd = (1 - alpha) * p_draw + alpha * u
    ca = (1 - alpha) * p_away + alpha * u
    s = ch + cd + ca
    if s <= 0:
        return (u, u, u)
    return (ch / s, cd / s, ca / s)


def write_output(out_dir: str, rows, method_label: str, alpha: float, debug: bool):
    out_path = os.path.join(out_dir, "calibrated_probs.csv")
    cols = ["match_id", "calib_method", "calib_home", "calib_draw", "calib_away"]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(cols)
        for mid, ph, pd, pa in rows:
            ch, cd, ca = dirichlet_smooth(ph, pd, pa, alpha)
            wr.writerow([mid, method_label, f"{ch:.9f}", f"{cd:.9f}", f"{ca:.9f}"])
    if debug:
        print(f"[calibrate] Salvo em: {out_path}")
        try:
            prv = pd.read_csv(out_path).head(8)
            print(prv.to_csv(index=False))
        except Exception:
            pass
    return out_path


# ------------- Loaders em cascata ------------- #

def load_from_predictions_market(path: str, debug: bool):
    if not os.path.isfile(path):
        return None
    df = pd.read_csv(path)
    if df.empty:
        if debug:
            print("[calibrate][DEBUG] predictions_market.csv está vazio")
        return None

    c_home = get_name(df, "home", "team_home")
    c_away = get_name(df, "away", "team_away")
    if not c_home or not c_away:
        return None

    # odds e probs (opcionais)
    c_oh = get_name(df, "odd_home", "odds_home", "home_odds")
    c_od = get_name(df, "odd_draw", "odds_draw", "draw_odds")
    c_oa = get_name(df, "odd_away", "odds_away", "away_odds")
    c_ph = get_name(df, "p_home")
    c_pd = get_name(df, "p_draw")
    c_pa = get_name(df, "p_away")

    rows = []
    for _, r in df.iterrows():
        home = str(r[c_home]).strip()
        away = str(r[c_away]).strip()
        mid = f"{home}__{away}"

        if c_ph and c_pd and c_pa and pd.notna(r.get(c_ph)) and pd.notna(r.get(c_pd)) and pd.notna(r.get(c_pa)):
            ph, pdv, pa = ffloat(r[c_ph]), ffloat(r[c_pd]), ffloat(r[c_pa])
        elif c_oh and c_od and c_oa:
            ph, pdv, pa = remove_overround(ffloat(r[c_oh]), ffloat(r[c_od]), ffloat(r[c_oa]))
        else:
            continue

        rows.append((mid, ph, pdv, pa))

    if debug:
        print(f"[calibrate][DEBUG] predictions_market -> {len(rows)} linhas")
    return rows if rows else None


def load_from_odds_consensus(path: str, debug: bool):
    if not os.path.isfile(path):
        return None
    df = pd.read_csv(path)
    if df.empty:
        if debug:
            print("[calibrate][DEBUG] odds_consensus.csv está vazio")
        return None

    c_home = get_name(df, "home", "team_home")
    c_away = get_name(df, "away", "team_away")
    c_oh = get_name(df, "odd_home", "odds_home")
    c_od = get_name(df, "odd_draw", "odds_draw")
    c_oa = get_name(df, "odd_away", "odds_away")
    if not all([c_home, c_away, c_oh, c_od, c_oa]):
        return None

    rows = []
    for _, r in df.iterrows():
        home = str(r[c_home]).strip()
        away = str(r[c_away]).strip()
        mid = f"{home}__{away}"
        ph, pdv, pa = remove_overround(ffloat(r[c_oh]), ffloat(r[c_od]), ffloat(r[c_oa]))
        rows.append((mid, ph, pdv, pa))

    if debug:
        print(f"[calibrate][DEBUG] odds_consensus -> {len(rows)} linhas")
    return rows if rows else None


def load_from_theoddsapi(path: str, debug: bool):
    """
    Lê <OUT_DIR>/odds_theoddsapi.csv e agrega por (home, away) fazendo média das odds
    entre as regiões; converte em probabilidades justas.
    Espera colunas: home/away + odds_home/odds_draw/odds_away (ou odd_*).
    """
    if not os.path.isfile(path):
        return None
    df = pd.read_csv(path)
    if df.empty:
        if debug:
            print("[calibrate][DEBUG] odds_theoddsapi.csv está vazio")
        return None

    c_home = get_name(df, "home", "team_home")
    c_away = get_name(df, "away", "team_away")
    c_oh = get_name(df, "odds_home", "odd_home", "home_odds")
    c_od = get_name(df, "odds_draw", "odd_draw", "draw_odds")
    c_oa = get_name(df, "odds_away", "odd_away", "away_odds")
    if not all([c_home, c_away, c_oh, c_od, c_oa]):
        return None

    # agrega por média
    grp = {}
    for _, r in df.iterrows():
        key = (str(r[c_home]).strip(), str(r[c_away]).strip())
        oh, od, oa = ffloat(r[c_oh]), ffloat(r[c_od]), ffloat(r[c_oa])
        if oh > 1.01 and od > 1.01 and oa > 1.01:
            grp.setdefault(key, {"oh": [], "od": [], "oa": []})
            grp[key]["oh"].append(oh)
            grp[key]["od"].append(od)
            grp[key]["oa"].append(oa)

    rows = []
    for (home, away), d in grp.items():
        if not d["oh"] or not d["od"] or not d["oa"]:
            continue
        oh = fmean(d["oh"])
        od = fmean(d["od"])
        oa = fmean(d["oa"])
        ph, pdv, pa = remove_overround(oh, od, oa)
        rows.append((f"{home}__{away}", ph, pdv, pa))

    if debug:
        print(f"[calibrate][DEBUG] odds_theoddsapi (agregado) -> {len(rows)} linhas")
    return rows if rows else None


def load_uniform_from_whitelist(path: str, debug: bool):
    """
    Fallback final: se nada existir, usa a whitelist para criar entradas
    com probabilidade uniforme (1/3 cada), permitindo o pipeline seguir.
    """
    if not os.path.isfile(path):
        return None
    df = pd.read_csv(path)
    if df.empty:
        return None

    c_home = get_name(df, "team_home", "home")
    c_away = get_name(df, "team_away", "away")
    if not c_home or not c_away:
        return None

    rows = []
    for _, r in df.iterrows():
        home = str(r[c_home]).strip()
        away = str(r[c_away]).strip()
        rows.append((f"{home}__{away}", 1/3, 1/3, 1/3))

    if debug:
        print(f"[calibrate][DEBUG] whitelist fallback -> {len(rows)} linhas (uniforme)")
    return rows if rows else None


# ------------- Main ------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--alpha", type=float, default=0.20, help="força da suavização Dirichlet [0..1]")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    pm_path  = os.path.join(out_dir, "predictions_market.csv")
    oc_path  = os.path.join(out_dir, "odds_consensus.csv")
    to_path  = os.path.join(out_dir, "odds_theoddsapi.csv")
    wl_path  = os.path.join(out_dir, "matches_whitelist.csv")

    # 1) predictions_market
    rows = load_from_predictions_market(pm_path, args.debug)
    method = "Dirichlet"

    # 2) consensus
    if not rows:
        print("[calibrate] predictions_market ausente/incompleto. Tentando odds_consensus.csv ...")
        rows = load_from_odds_consensus(oc_path, args.debug)

    # 3) theoddsapi
    if not rows:
        print("[calibrate] odds_consensus ausente/incompleto. Tentando odds_theoddsapi.csv ...")
        rows = load_from_theoddsapi(to_path, args.debug)

    # 4) whitelist uniforme (para não quebrar a rodada)
    if not rows:
        print("[calibrate] odds_theoddsapi ausente/incompleto. Usando fallback uniforme (whitelist).")
        rows = load_uniform_from_whitelist(wl_path, args.debug)
        method = "FallbackUniform"

    if not rows:
        print("##[error][calibrate] Não foi possível derivar probabilidades de nenhuma fonte.", file=sys.stderr)
        sys.exit(23)

    write_output(out_dir, rows, method, args.alpha, args.debug)
    print("[ok] Calibração concluída com sucesso.")


if __name__ == "__main__":
    try:
        print("=" * 51)
        print("[calibrate] INICIANDO CALIBRAÇÃO DE PROBABILIDADES")
        print(f"[calibrate] Diretório de rodada : {os.environ.get('OUT_DIR','(env OUT_DIR não definido)')}")
        print("=" * 51)
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"##[error][calibrate] Falha inesperada: {e}", file=sys.stderr)
        sys.exit(23)