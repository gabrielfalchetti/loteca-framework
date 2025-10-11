#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera kelly_stakes.csv a partir de probs_calibrated.csv (ou odds_consensus.csv como fallback).

Uso:
  python -m scripts.kelly_bets \
    --rodada data/out/<RUN_ID> \
    --bankroll 1000 --fraction 0.5 --cap 0.1 --topn 14 --round_to 1

Saída: <rodada>/kelly_stakes.csv

Colunas:
  match_id, team_home, team_away, selection, team_pick,
  prob, odds, kelly_f, stake_raw, stake, stake_pct_bankroll, ev
"""

import os
import sys
import argparse
import math
import pandas as pd


def log(level, msg):
    tag = f"[{level}] " if level else ""
    print(f"[kelly] {tag}{msg}", flush=True)


def read_csv_required(path, required_cols=None):
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    if required_cols is not None:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"{os.path.basename(path)} sem colunas: {missing}")
    return df


def to_float(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def implied_probs(odds_home, odds_draw, odds_away):
    oh = to_float(odds_home)
    od = to_float(odds_draw)
    oa = to_float(odds_away)
    if not oh or not od or not oa or oh <= 1 or od <= 1 or oa <= 1:
        return None, None, None
    ih, idr, ia = 1.0 / oh, 1.0 / od, 1.0 / oa
    s = ih + idr + ia
    if s <= 0:
        return None, None, None
    return ih / s, idr / s, ia / s


def kelly_fraction(p, odds):
    if p is None or odds is None:
        return None
    b = odds - 1.0
    if b <= 0:
        return None
    q = 1.0 - p
    return (b * p - q) / b


def round_to_step(x, step):
    if step is None or step <= 0:
        return x
    return round(x / step) * step


def load_base(rodada_dir: str) -> pd.DataFrame:
    """
    Tenta ler probs_calibrated.csv.
    Fallback: odds_consensus.csv + prob implícita.

    Retorna:
      match_id, team_home, team_away, odds_home, odds_draw, odds_away,
      p_home, p_draw, p_away
    """
    pc = os.path.join(rodada_dir, "probs_calibrated.csv")
    oc = os.path.join(rodada_dir, "odds_consensus.csv")

    if os.path.isfile(pc):
        log("INFO", f"Usando {os.path.basename(pc)}")
        df = pd.read_csv(pc)
        df = df.rename(columns={"home": "team_home", "away": "team_away"})
        needed = [
            "match_id",
            "team_home",
            "team_away",
            "odds_home",
            "odds_draw",
            "odds_away",
            "p_home",
            "p_draw",
            "p_away",
        ]
        missing = [c for c in needed if c not in df.columns]
        if missing:
            log("WARN", f"{os.path.basename(pc)} incompleto ({missing}); tentando completar de {os.path.basename(oc)}")
            if os.path.isfile(oc):
                df_oc = read_csv_required(oc, ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"])
                df = df.merge(
                    df_oc[["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]],
                    on=["team_home", "team_away"],
                    how="left",
                    suffixes=("", "_oc"),
                )
                for c in ["odds_home", "odds_draw", "odds_away"]:
                    if c not in df.columns or df[c].isna().all():
                        alt = c + "_oc"
                        if alt in df.columns:
                            df[c] = df[alt]
                # calcula probs se faltarem
                if any(c not in df.columns for c in ["p_home", "p_draw", "p_away"]):
                    p_h, p_d, p_a = [], [], []
                    for _, r in df.iterrows():
                        a = implied_probs(r.get("odds_home"), r.get("odds_draw"), r.get("odds_away"))
                        p_h.append(a[0] if a[0] is not None else float("nan"))
                        p_d.append(a[1] if a[1] is not None else float("nan"))
                        p_a.append(a[2] if a[2] is not None else float("nan"))
                    df["p_home"], df["p_draw"], df["p_away"] = p_h, p_d, p_a
            else:
                raise ValueError("Sem odds_consensus.csv para completar probs_calibrated.csv")

        for c in ["odds_home", "odds_draw", "odds_away", "p_home", "p_draw", "p_away"]:
            if c in df.columns:
                df[c] = df[c].apply(to_float)

        df = df.dropna(
            subset=[
                "match_id",
                "team_home",
                "team_away",
                "odds_home",
                "odds_draw",
                "odds_away",
                "p_home",
                "p_draw",
                "p_away",
            ]
        )
        return df[
            [
                "match_id",
                "team_home",
                "team_away",
                "odds_home",
                "odds_draw",
                "odds_away",
                "p_home",
                "p_draw",
                "p_away",
            ]
        ].copy()

    # Fallback via odds_consensus.csv
    if os.path.isfile(oc):
        log("WARN", f"{os.path.basename(pc)} ausente; usando {os.path.basename(oc)} com probs implícitas")
        df = read_csv_required(oc, ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"])
        if "match_id" not in df.columns:
            wl = os.path.join(rodada_dir, "matches_whitelist.csv")
            if os.path.isfile(wl):
                df_wl = read_csv_required(wl, ["match_id", "home", "away"]).rename(
                    columns={"home": "team_home", "away": "team_away"}
                )
                df = df.merge(df_wl, on=["team_home", "team_away"], how="left")
            else:
                df["match_id"] = range(1, len(df) + 1)

        p_h, p_d, p_a = [], [], []
        for _, r in df.iterrows():
            a = implied_probs(r.get("odds_home"), r.get("odds_draw"), r.get("odds_away"))
            p_h.append(a[0] if a[0] is not None else float("nan"))
            p_d.append(a[1] if a[1] is not None else float("nan"))
            p_a.append(a[2] if a[2] is not None else float("nan"))
        df["p_home"], df["p_draw"], df["p_away"] = p_h, p_d, p_a

        df["match_id"] = df["match_id"]
        for c in ["odds_home", "odds_draw", "odds_away", "p_home", "p_draw", "p_away"]:
            df[c] = df[c].apply(to_float)
        df = df.dropna(
            subset=[
                "match_id",
                "team_home",
                "team_away",
                "odds_home",
                "odds_draw",
                "odds_away",
                "p_home",
                "p_draw",
                "p_away",
            ]
        )
        return df[
            [
                "match_id",
                "team_home",
                "team_away",
                "odds_home",
                "odds_draw",
                "odds_away",
                "p_home",
                "p_draw",
                "p_away",
            ]
        ].copy()

    raise FileNotFoundError("Nem probs_calibrated.csv nem odds_consensus.csv encontrados.")


def compute_picks(df: pd.DataFrame, bankroll: float, frac: float, cap: float, topn: int, round_step: float) -> pd.DataFrame:
    """
    Para cada jogo, calcula Kelly para home/draw/away, escolhe o melhor positivo,
    aplica fração e cap, arredonda stake. Depois filtra Top-N globais por stake.
    """
    rows = []

    df = df.copy()
    df["match_id"] = df["match_id"]

    for _, r in df.iterrows():
        mid = r["match_id"]
        th, ta = str(r["team_home"]), str(r["team_away"])
        oh, od, oa = to_float(r["odds_home"]), to_float(r["odds_draw"]), to_float(r["odds_away"])
        p_h, p_d, p_a = to_float(r["p_home"]), to_float(r["p_draw"]), to_float(r["p_away"])

        candidates = []
        for sel, p_prob, o_price, team_pick in [
            ("home", p_h, oh, th),
            ("draw", p_d, od, "Draw"),
            ("away", p_a, oa, ta),
        ]:
            if p_prob is None or o_price is None or p_prob <= 0 or p_prob >= 1 or o_price <= 1:
                continue
            f = kelly_fraction(p_prob, o_price)
            if f is None:
                continue
            q = 1.0 - p_prob
            ev = p_prob * (o_price - 1.0) - q  # valor esperado por unidade apostada
            stake_raw = max(0.0, bankroll * frac * f)
            cap_abs = bankroll * cap if cap and cap > 0 else float("inf")
            stake_capped = min(stake_raw, cap_abs)
            stake = round_to_step(stake_capped, round_step)
            stake_pct = (stake / bankroll) if bankroll > 0 else 0.0

            candidates.append(
                {
                    "match_id": mid,
                    "team_home": th,
                    "team_away": ta,
                    "selection": sel,
                    "team_pick": team_pick,
                    "prob": p_prob,
                    "odds": o_price,
                    "kelly_f": f,
                    "stake_raw": stake_raw,
                    "stake": stake,
                    "stake_pct_bankroll": stake_pct,
                    "ev": ev,
                }
            )

        if not candidates:
            continue

        candidates.sort(key=lambda x: (x["kelly_f"], x["stake"]), reverse=True)
        best = candidates[0]
        if best["kelly_f"] > 0 and best["stake"] > 0:
            rows.append(best)

    out_df = pd.DataFrame(
        rows,
        columns=[
            "match_id",
            "team_home",
            "team_away",
            "selection",
            "team_pick",
            "prob",
            "odds",
            "kelly_f",
            "stake_raw",
            "stake",
            "stake_pct_bankroll",
            "ev",
        ],
    )

    if out_df.empty:
        return out_df

    out_df = out_df.sort_values(by=["stake", "ev", "kelly_f"], ascending=[False, False, False]).reset_index(drop=True)
    if topn and topn > 0 and len(out_df) > topn:
        out_df = out_df.head(topn).copy()
    out_df = out_df.sort_values(by=["stake"], ascending=False).reset_index(drop=True)
    return out_df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex: data/out/<RUN_ID>)")
    ap.add_argument("--bankroll", required=True, type=float, help="Tamanho do bankroll")
    ap.add_argument("--fraction", required=True, type=float, help="Fração de Kelly (ex: 0.5)")
    ap.add_argument("--cap", required=True, type=float, help="Cap em %% do bankroll por aposta (ex: 0.1)")
    ap.add_argument("--topn", required=True, type=float, help="Número máximo de picks")
    ap.add_argument("--round_to", required=True, type=float, help="Arredondar stake para múltiplos deste valor")
    args = ap.parse_args()

    rodada = args.rodada
    bankroll = float(args.bankroll)
    frac = float(args.fraction)
    cap = float(args.cap)
    topn = int(round(float(args.topn)))
    round_step = float(args.round_to)

    out_path = os.path.join(rodada, "kelly_stakes.csv")

    log("INFO", f"Rodada: {rodada}")
    log("INFO", f"Bankroll={bankroll} fraction={frac} cap={cap} topN={topn} round_to={round_step}")

    try:
        base = load_base(rodada)
    except Exception as e:
        log("CRITICAL", f"Falha carregando base: {e}")
        # Garante arquivo (mesmo vazio) para o step não quebrar
        pd.DataFrame(
            columns=[
                "match_id",
                "team_home",
                "team_away",
                "selection",
                "team_pick",
                "prob",
                "odds",
                "kelly_f",
                "stake_raw",
                "stake",
                "stake_pct_bankroll",
                "ev",
            ]
        ).to_csv(out_path, index=False)
        return 0

    picks = compute_picks(base, bankroll, frac, cap, topn, round_step)

    # Sempre escreve o arquivo (mesmo vazio com cabeçalho)
    if picks.empty:
        log("WARN", "Nenhuma aposta elegível (Kelly <= 0 ou odds inválidas). Gerando arquivo vazio com cabeçalho.")
        picks.head(0).to_csv(out_path, index=False)
        return 0

    picks.to_csv(out_path, index=False)
    log("INFO", f"Gerado {os.path.basename(out_path)} com {len(picks)} picks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())