#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
blend_models.py
---------------
Gera um ensemble (“blend”) de probabilidades por jogo.

ENTRADAS (em data/out/<rodada>/):
- odds_consensus.csv     (obrigatório)
    colunas mínimas: match_id, team_home, team_away, odds_home, odds_draw, odds_away
- calibrated_probs.csv   (opcional)
    colunas mínimas: match_id, calib_method, calib_home, calib_draw, calib_away

SAÍDA:
- predictions_blend.csv
    colunas: match_id,team_home,team_away,p_home,p_draw,p_away,used_sources,weights

REGRAS:
- Sempre calcula “market” a partir de odds_consensus (p = (1/odds)/soma).
- Se calibrated_probs.csv existir, faz o blend:
      p = wm*market + wc*calib   (normalizando ao final por segurança)
  com pesos padrão: market=0.35, calib=0.65 (ajustáveis via --weights).
- Se não existir, publica somente market (como no log de referência).

USO:
python scripts/blend_models.py --rodada <id|data/out/<id>> [--weights "market:0.35,calib:0.65"] [--debug]
"""

import argparse
import csv
import os
from typing import Dict, Tuple

import pandas as pd


# ----------------------------- utilidades ---------------------------------- #

def log(msg: str, debug: bool = False):
    if debug:
        print(f"[blend] {msg}", flush=True)


def resolve_out_dir(rodada: str) -> str:
    """Aceita ID (ex: 1759844885) ou caminho 'data/out/<id>'."""
    if os.path.isdir(rodada):
        return rodada
    path = os.path.join("data", "out", str(rodada))
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)
    return path


def parse_weights(s: str, debug: bool = False) -> Dict[str, float]:
    """
    Converte string estilo "market:0.35,calib:0.65" em dict.
    Valores inválidos são ignorados; se soma>0, normaliza.
    """
    w = {}
    for part in s.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        k, v = part.split(":", 1)
        k = k.strip().lower()
        try:
            w[k] = float(v)
        except Exception:
            pass
    total = sum(w.values())
    if total > 0:
        for k in list(w):
            w[k] = w[k] / total
    if debug:
        print(f"[blend] pesos parseados (normalizados): {w}")
    return w


def implied_from_odds(odds_h: float, odds_d: float, odds_a: float) -> Tuple[float, float, float]:
    """p = (1/odds)/soma — simples e robusto."""
    ih = 0.0 if odds_h <= 0 else 1.0 / odds_h
    id_ = 0.0 if odds_d <= 0 else 1.0 / odds_d
    ia = 0.0 if odds_a <= 0 else 1.0 / odds_a
    s = ih + id_ + ia
    if s <= 0:
        return 0.0, 0.0, 0.0
    return ih / s, id_ / s, ia / s


# --------------------------- carregamento base ------------------------------ #

def load_market(out_dir: str, debug: bool = False) -> pd.DataFrame:
    """
    Lê odds_consensus.csv e calcula probs 'market'.
    Retorna DF com:
      match_id, team_home, team_away, m_home, m_draw, m_away
    """
    fp = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(fp):
        raise FileNotFoundError(f"[blend] odds_consensus.csv não encontrado em {out_dir}")

    df = pd.read_csv(fp)
    need = ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"[blend] coluna obrigatória ausente em odds_consensus.csv: '{c}'")

    # match_id pode já existir; se não, criamos no formato <home>__<away>
    if "match_id" not in df.columns:
        df["match_id"] = (df["team_home"].astype(str).str.strip() + "__" +
                          df["team_away"].astype(str).str.strip())

    # tipagem
    for c in ["odds_home", "odds_draw", "odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # market probs
    probs = df.apply(
        lambda r: implied_from_odds(r["odds_home"], r["odds_draw"], r["odds_away"]),
        axis=1
    )
    df[["m_home", "m_draw", "m_away"]] = pd.DataFrame(probs.tolist(), index=df.index)

    keep = ["match_id", "team_home", "team_away", "m_home", "m_draw", "m_away"]
    df = df[keep].copy()

    if debug:
        print("[blend] base de nomes vinda de odds_consensus.csv")
        print("[blend] market derivado de odds_consensus (implied)")
        print(df.head(10).to_string(index=False))

    return df


def maybe_load_calibrated(out_dir: str, debug: bool = False) -> pd.DataFrame | None:
    """
    Lê calibrated_probs.csv se existir.
    Espera colunas: match_id, calib_home, calib_draw, calib_away
    Retorna DF com essas colunas OU None se ausente.
    """
    fp = os.path.join(out_dir, "calibrated_probs.csv")
    if not os.path.isfile(fp):
        log(f"arquivo ausente: {fp}", debug)
        return None

    df = pd.read_csv(fp)
    need = ["match_id", "calib_home", "calib_draw", "calib_away"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"[blend] coluna '{c}' ausente em calibrated_probs.csv")

    for c in ["calib_home", "calib_draw", "calib_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    return df[need].copy()


# ------------------------------- blending ---------------------------------- #

def do_blend(df_market: pd.DataFrame,
             df_calib: pd.DataFrame | None,
             weights: Dict[str, float],
             debug: bool = False) -> pd.DataFrame:
    """
    Executa o blend e retorna DF final:
      match_id, team_home, team_away, p_home, p_draw, p_away, used_sources, weights
    """
    # pesos padrão (normalizados)
    w_market = float(weights.get("market", 0.35))
    w_calib = float(weights.get("calib", 0.65))
    # já estão normalizados pelo parse_weights; garantimos soma=1
    s = w_market + w_calib
    if s > 0:
        w_market, w_calib = w_market / s, w_calib / s
    else:
        # fallback seguro
        w_market, w_calib = 1.0, 0.0

    base = df_market.copy()

    used_sources = []
    weight_strs = []
    p_home, p_draw, p_away = [], [], []

    # Faz merge com calibrated se houver
    if df_calib is not None:
        df = base.merge(df_calib, on="match_id", how="left")
        has_calib = df["calib_home"].notna() & df["calib_draw"].notna() & df["calib_away"].notna()

        for _, r in df.iterrows():
            mh, md, ma = float(r["m_home"]), float(r["m_draw"]), float(r["m_away"])
            ch = float(r["calib_home"]) if not pd.isna(r["calib_home"]) else 0.0
            cd = float(r["calib_draw"]) if not pd.isna(r["calib_draw"]) else 0.0
            ca = float(r["calib_away"]) if not pd.isna(r["calib_away"]) else 0.0

            if (ch + cd + ca) <= 0:
                # sem calib válido → usa só market
                uh, ud, ua = mh, md, ma
                used_sources.append("market")
                weight_strs.append(f"market:{w_market:.2f}")
            else:
                uh = w_market * mh + w_calib * ch
                ud = w_market * md + w_calib * cd
                ua = w_market * ma + w_calib * ca
                used_sources.append("market+calib")
                weight_strs.append(f"market:{w_market:.2f};calib:{w_calib:.2f}")

            s = uh + ud + ua
            if s > 0:
                uh, ud, ua = uh / s, ud / s, ua / s

            p_home.append(uh)
            p_draw.append(ud)
            p_away.append(ua)

        out = df[["match_id", "team_home", "team_away"]].copy()
        out["p_home"] = p_home
        out["p_draw"] = p_draw
        out["p_away"] = p_away
        out["used_sources"] = used_sources
        out["weights"] = weight_strs
        return out

    # Sem calibrated → apenas market (replica o comportamento do log de referência)
    for _, r in base.iterrows():
        p_home.append(float(r["m_home"]))
        p_draw.append(float(r["m_draw"]))
        p_away.append(float(r["m_away"]))
        used_sources.append("market")
        # mantém a string de pesos com market apenas (igual ao log)
        weight_strs.append("market:0.35")

    out = base[["match_id", "team_home", "team_away"]].copy()
    out["p_home"] = p_home
    out["p_draw"] = p_draw
    out["p_away"] = p_away
    out["used_sources"] = used_sources
    out["weights"] = weight_strs
    return out


# --------------------------------- main ------------------------------------ #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID da rodada OU caminho data/out/<id>")
    ap.add_argument("--weights", default="market:0.35,calib:0.65",
                    help='Pesos do blend, ex: "market:0.35,calib:0.65" (serão normalizados)')
    ap.add_argument("--debug", action="store_true", help="Logs detalhados")
    args = ap.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    log(f"rodada: {out_dir}", args.debug)

    df_market = load_market(out_dir, args.debug)
    df_calib = maybe_load_calibrated(out_dir, args.debug)

    w = parse_weights(args.weights, args.debug)
    df_blend = do_blend(df_market, df_calib, w, args.debug)

    out_fp = os.path.join(out_dir, "predictions_blend.csv")
    df_blend.to_csv(out_fp, index=False, quoting=csv.QUOTE_MINIMAL)

    log(f"OK -> {out_fp}", args.debug)

    if args.debug:
        try:
            print(df_blend.head(10).to_string(index=False))
        except Exception:
            print(df_blend.head(10))
        print(df_blend.head(10).to_csv(index=False))


if __name__ == "__main__":
    main()