#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
calibrate_probs.py

Objetivo:
  Ler probabilidades de mercado (ou derivá-las das odds) e aplicar uma
  calibração suave tipo "Dirichlet smoothing" (mistura com distribuição
  uniforme), produzindo:

    <OUT_DIR>/calibrated_probs.csv

Entradas possíveis (nesta ordem de preferência):
  1) <OUT_DIR>/predictions_market.csv
     - Usa colunas p_home, p_draw, p_away se existirem;
     - Caso não existam, tenta derivar das colunas de odds.

  2) <OUT_DIR>/odds_consensus.csv
     - Deriva probabilidades removendo overround.

Parâmetros:
  --rodada <OUT_DIR>         (obrigatório)
  --alpha  <float>           (força da suavização Dirichlet, default 0.20)
  --debug                    (prints extras)

Saída:
  calibrated_probs.csv  com colunas:
    match_id,calib_method,calib_home,calib_draw,calib_away

Notas:
  - 'match_id' sai no estilo "Home__Away" (com dois underlines), compatível
    com o loader do blend (ele normaliza para comparar).
"""

import argparse
import csv
import os
import sys
from math import isfinite

import pandas as pd


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


def get_names(df, *candidates):
    """Retorna o primeiro nome de coluna existente, dentre os candidatos."""
    for name in candidates:
        if name in df.columns:
            return name
    return None


def load_probs_from_predictions_market(path: str, debug: bool):
    if not os.path.isfile(path):
        return None
    df = pd.read_csv(path)
    # nomes de times
    c_home = get_names(df, "home", "team_home")
    c_away = get_names(df, "away", "team_away")
    if not c_home or not c_away:
        raise ValueError("Colunas de times não encontradas em predictions_market.csv.")

    # match_id: "Home__Away"
    if "match_key" in df.columns and df["match_key"].notna().all():
        # pode vir normalizado; reconstruímos 'bonito' com os nomes originais
        pass
    # odds (podem não existir se já veio com p_*)
    c_oh = get_names(df, "odd_home", "odds_home", "home_odds")
    c_od = get_names(df, "odd_draw", "odds_draw", "draw_odds")
    c_oa = get_names(df, "odd_away", "odds_away", "away_odds")

    # probs
    c_ph = get_names(df, "p_home")
    c_pd = get_names(df, "p_draw")
    c_pa = get_names(df, "p_away")

    rows = []
    for _, r in df.iterrows():
        home = str(r[c_home]).strip()
        away = str(r[c_away]).strip()
        mid = f"{home}__{away}"

        if c_ph and c_pd and c_pa and pd.notna(r.get(c_ph)) and pd.notna(r.get(c_pd)) and pd.notna(r.get(c_pa)):
            p_home, p_draw, p_away = ffloat(r[c_ph]), ffloat(r[c_pd]), ffloat(r[c_pa])
        elif c_oh and c_od and c_oa:
            p_home, p_draw, p_away = remove_overround(
                ffloat(r[c_oh]), ffloat(r[c_od]), ffloat(r[c_oa])
            )
        else:
            # linha sem dados suficientes
            continue

        rows.append((mid, p_home, p_draw, p_away))

    if debug:
        print(f"[calibrate][DEBUG] predictions_market -> linhas: {len(rows)}")

    return rows if rows else None


def load_probs_from_odds(path: str, debug: bool):
    if not os.path.isfile(path):
        return None
    df = pd.read_csv(path)

    c_home = get_names(df, "home", "team_home")
    c_away = get_names(df, "away", "team_away")
    c_oh = get_names(df, "odd_home", "odds_home")
    c_od = get_names(df, "odd_draw", "odds_draw")
    c_oa = get_names(df, "odd_away", "odds_away")

    if not all([c_home, c_away, c_oh, c_od, c_oa]):
        # arquivo não está no padrão esperado
        return None

    rows = []
    for _, r in df.iterrows():
        home = str(r[c_home]).strip()
        away = str(r[c_away]).strip()
        mid = f"{home}__{away}"
        p_home, p_draw, p_away = remove_overround(
            ffloat(r[c_oh]), ffloat(r[c_od]), ffloat(r[c_oa])
        )
        rows.append((mid, p_home, p_draw, p_away))

    if debug:
        print(f"[calibrate][DEBUG] odds_consensus -> linhas: {len(rows)}")

    return rows if rows else None


def dirichlet_smooth(p_home: float, p_draw: float, p_away: float, alpha: float):
    """
    Calibração tipo Dirichlet: mistura convexa com uniforme (1/3,1/3,1/3).
    alpha = 0  -> sem suavização
    alpha = 1  -> mistura 50/50 com uniforme
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


def write_output(out_dir: str, rows, alpha: float, debug: bool):
    out_path = os.path.join(out_dir, "calibrated_probs.csv")
    cols = ["match_id", "calib_method", "calib_home", "calib_draw", "calib_away"]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(cols)
        for mid, ph, pd, pa in rows:
            ch, cd, ca = dirichlet_smooth(ph, pd, pa, alpha)
            wr.writerow([mid, "Dirichlet", f"{ch:.9f}", f"{cd:.9f}", f"{ca:.9f}"])
    if debug:
        print(f"[calibrate] Salvo em: {out_path}")
        # preview
        try:
            prv = pd.read_csv(out_path).head(5)
            print(prv.to_csv(index=False))
        except Exception:
            pass
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--alpha", type=float, default=0.20, help="força da suavização Dirichlet [0..1]")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    pm_path = os.path.join(out_dir, "predictions_market.csv")
    oc_path = os.path.join(out_dir, "odds_consensus.csv")

    # 1) tenta pegar do predictions_market
    rows = load_probs_from_predictions_market(pm_path, args.debug)

    # 2) fallback: odds_consensus
    if not rows:
        print("[calibrate] predictions_market.csv sem prob*. Usando fallback por odds_consensus.csv.")
        rows = load_probs_from_odds(oc_path, args.debug)

    if not rows:
        print("##[error][calibrate] Não foi possível derivar probabilidades (predictions_market/odds_consensus ausentes/incompletos).", file=sys.stderr)
        sys.exit(23)

    write_output(out_dir, rows, args.alpha, args.debug)
    print("[ok] Calibração concluída com sucesso.")


if __name__ == "__main__":
    try:
        print("=" * 51)
        print("[calibrate] INICIANDO CALIBRAÇÃO DE PROBABILIDADES")
        print(f"[calibrate] Diretório de rodada : {os.environ.get('OUT_DIR','(não definido via env)')}")
        print("=" * 51)
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"##[error][calibrate] Falha inesperada: {e}", file=sys.stderr)
        sys.exit(23)