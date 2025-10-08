#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
blend_models.py

Faz o blend entre:
- Probabilidades de mercado (predictions_market.csv) OU odds_consensus -> implícitas
- Probabilidades calibradas (calibrated_probs.csv)
- Ajuste de contexto (context_features.csv) opcional

Saídas:
- {OUT_DIR}/predictions_blend.csv        (blend base market + calib)
- {OUT_DIR}/predictions_final.csv        (blend + ajuste de contexto, se habilitado)
"""

import argparse
import os
import sys
import math
from typing import Optional, Dict
import pandas as pd


def dbg(on: bool, *msg):
    if on:
        print("[blend]", *msg, flush=True)


def read_csv_safe(path: str) -> Optional[pd.DataFrame]:
    if not os.path.isfile(path):
        return None
    try:
        df = pd.read_csv(path)
        if df.shape[0] == 0:
            return None
        return df
    except Exception:
        return None


def softmax3(a, b, c):
    m = max(a, b, c)
    ea, eb, ec = math.exp(a - m), math.exp(b - m), math.exp(c - m)
    s = ea + eb + ec
    return ea / s, eb / s, ec / s


def clamp01(x, lo=1e-6, hi=1.0 - 1e-6):
    return max(lo, min(hi, x))


def normalize_team_key(s: str) -> str:
    # normalização leve para match keys por nome de time
    if not isinstance(s, str):
        s = str(s)
    return " ".join(s.strip().split()).lower()


def make_homeaway_key(home: str, away: str) -> str:
    return f"{home}__{away}"


def implied_probs_from_odds(row):
    # odds -> prob fair
    oh, od, oa = float(row["odds_home"]), float(row["odds_draw"]), float(row["odds_away"])
    imp_h, imp_d, imp_a = 1.0 / oh, 1.0 / od, 1.0 / oa
    total = imp_h + imp_d + imp_a
    return imp_h / total, imp_d / total, imp_a / total


def load_market(rodada: str, debug: bool) -> pd.DataFrame:
    """
    Prioridade:
    1) predictions_market.csv (tem match_key, home, away, p_home, p_draw, p_away)
    2) odds_consensus.csv -> calcula p_fair a partir de odds
    """
    pm_path = os.path.join(rodada, "predictions_market.csv")
    df = read_csv_safe(pm_path)
    if df is not None and {"match_key", "home", "away", "p_home", "p_draw", "p_away"}.issubset(df.columns):
        df = df.copy()
        df["key_norm"] = df["home"].apply(normalize_team_key) + "||" + df["away"].apply(normalize_team_key)
        df["ha_key"] = df.apply(lambda r: make_homeaway_key(str(r["home"]), str(r["away"])), axis=1)
        dbg(debug, f"market: predictions_market.csv ({len(df)} linhas)")
        return df[["match_key", "home", "away", "p_home", "p_draw", "p_away", "key_norm", "ha_key"]]

    # fallback: odds_consensus
    oc_path = os.path.join(rodada, "odds_consensus.csv")
    oc = read_csv_safe(oc_path)
    if oc is None or not {"team_home", "team_away", "odds_home", "odds_draw", "odds_away"}.issubset(oc.columns):
        raise FileNotFoundError("Nem predictions_market.csv nem odds_consensus.csv disponíveis para market.")
    oc = oc.copy()
    probs = oc.apply(implied_probs_from_odds, axis=1, result_type="expand")
    oc[["p_home", "p_draw", "p_away"]] = probs
    oc = oc.rename(columns={"team_home": "home", "team_away": "away"})
    oc["match_key"] = oc["home"].astype(str).str.lower().str.replace(" ", "-") + "__vs__" + oc["away"].astype(str).str.lower().str.replace(" ", "-")
    oc["key_norm"] = oc["home"].apply(normalize_team_key) + "||" + oc["away"].apply(normalize_team_key)
    oc["ha_key"] = oc.apply(lambda r: make_homeaway_key(str(r["home"]), str(r["away"])), axis=1)
    dbg(debug, f"market: odds_consensus.csv ({len(oc)} linhas)")
    return oc[["match_key", "home", "away", "p_home", "p_draw", "p_away", "key_norm", "ha_key"]]


def load_calibrated(rodada: str, market_df: pd.DataFrame, debug: bool) -> Optional[pd.DataFrame]:
    """
    calibrated_probs.csv: match_id no formato "Home__Away"
    Mapeamos para (home, away) do market.
    """
    path = os.path.join(rodada, "calibrated_probs.csv")
    df = read_csv_safe(path)
    if df is None or not {"match_id", "calib_home", "calib_draw", "calib_away"}.issubset(df.columns):
        dbg(debug, "calibrated_probs.csv ausente ou inválido — seguindo sem calib.")
        return None
    df = df.copy()
    # extrair home/away do match_id "Home__Away"
    split = df["match_id"].astype(str).str.split("__", n=1, expand=True)
    if split.shape[1] == 2:
        df["home"] = split[0].astype(str)
        df["away"] = split[1].astype(str)
    else:
        # fallback: tentar match por similaridade leve (não ideal)
        df["home"] = df["match_id"].astype(str)
        df["away"] = ""

    df["key_norm"] = df["home"].apply(normalize_team_key) + "||" + df["away"].apply(normalize_team_key)
    df["ha_key"] = df.apply(lambda r: make_homeaway_key(str(r["home"]), str(r["away"])), axis=1)

    # Realinhar com market por ha_key preferencialmente (strings exatas),
    # se falhar, por key_norm (normalizado).
    cols_keep = ["calib_home", "calib_draw", "calib_away", "key_norm", "ha_key"]
    dbg(debug, f"calib: linhas={len(df)}")
    return df[cols_keep]


def ensure_context_score(ctx: pd.DataFrame, debug: bool) -> pd.DataFrame:
    """
    Se 'context_score' não existir, cria a partir de colunas disponíveis.
    Convenção: score > 0 favorece HOME, score < 0 favorece AWAY.
    """
    ctx = ctx.copy()
    if "context_score" in ctx.columns:
        return ctx

    # pesos heurísticos
    w_gap = 0.30      # gap fair home-away
    w_xg = 0.30       # xg diff
    w_inj = 0.20      # (inj_away - inj_home) penaliza time com mais lesões
    w_news = 0.15     # menções de mídia
    w_weather = 0.05  # clima adverso reduz leve o favoritismo do mandante

    score = pd.Series(0.0, index=ctx.index, dtype=float)

    if "diff_ph_pa" in ctx.columns:
        score = score + w_gap * ctx["diff_ph_pa"].fillna(0.0)

    if "xg_diff_proxy" in ctx.columns:
        score = score + w_xg * ctx["xg_diff_proxy"].fillna(0.0)

    # lesões: mais lesões no mandante reduzem score; mais no visitante aumentam
    if {"inj_home", "inj_away"}.issubset(ctx.columns):
        inj_h = ctx["inj_home"].fillna(0.0).astype(float)
        inj_a = ctx["inj_away"].fillna(0.0).astype(float)
        denom = (inj_h + inj_a).replace(0, 1.0)
        score = score + w_inj * (inj_a - inj_h) / denom

    # notícias: mais menções p/ mandante aumentam score; visitante diminuem
    if {"news_mentions_home", "news_mentions_away"}.issubset(ctx.columns):
        n_h = ctx["news_mentions_home"].fillna(0.0).astype(float)
        n_a = ctx["news_mentions_away"].fillna(0.0).astype(float)
        denom = (n_h + n_a).replace(0, 1.0)
        score = score + w_news * (n_h - n_a) / denom

    # clima: vento/chuva fortes tendem a “nivelar” — leve redução do favoritismo do mandante
    # aplicamos uma pequena penalização se condições acima de thresholds
    if {"wind_speed_kph", "precip_mm"}.issubset(ctx.columns):
        wind = ctx["wind_speed_kph"].fillna(0.0).astype(float)
        rain = ctx["precip_mm"].fillna(0.0).astype(float)
        penal = (wind > 30).astype(float) * 0.5 + (rain > 0.5).astype(float) * 0.5
        score = score - w_weather * penal

    # normalização leve para evitar magnitudes exageradas
    score = score.clip(-1.0, 1.0)
    ctx["context_score"] = score
    return ctx


def load_context(rodada: str, debug: bool) -> Optional[pd.DataFrame]:
    path = os.path.join(rodada, "context_features.csv")
    df = read_csv_safe(path)
    if df is None:
        dbg(debug, "context_features.csv ausente — blend seguirá sem contexto.")
        return None
    # garantir chaves
    for c in ["home", "away", "match_id", "match_key"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    # criar chaves auxiliares de junção
    df["key_norm"] = df["home"].apply(normalize_team_key) + "||" + df["away"].apply(normalize_team_key)
    df["ha_key"] = df.apply(lambda r: make_homeaway_key(str(r["home"]), str(r["away"])), axis=1)

    df = ensure_context_score(df, debug)
    return df[["match_key", "match_id", "home", "away", "key_norm", "ha_key", "context_score"]]


def safe_merge_left(a: pd.DataFrame, b: Optional[pd.DataFrame], on: str, debug: bool, label: str) -> pd.DataFrame:
    if b is None or b.empty:
        dbg(debug, f"merge skip ({label}): dataframe vazio/ausente")
        return a
    out = a.merge(b, on=on, how="left")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--w_calib", type=float, default=0.65)
    ap.add_argument("--w_market", type=float, default=0.35)
    ap.add_argument("--use-context", dest="use_context", action="store_true", default=False)
    ap.add_argument("--context-strength", type=float, default=0.15, help="Intensidade do tilt de contexto (0..1)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    w_calib = float(args.w_calib)
    w_market = float(args.w_market)
    use_context = bool(args.use_context)
    ctx_strength = float(args.context_strength)
    debug = args.debug

    dbg(debug, f"rodada: {rodada}")

    # --- LOAD SOURCES ---
    market = load_market(rodada, debug)
    calib = load_calibrated(rodada, market, debug)
    ctx = load_context(rodada, debug)

    # alinhar base no MARKET
    base = market.copy()

    # anexar calib por ha_key (mais estável), fallback por key_norm
    if calib is not None:
        tmp = base.merge(calib[["calib_home", "calib_draw", "calib_away", "ha_key"]],
                         on="ha_key", how="left", suffixes=("", "_c1"))
        # se falhou muito, tentar por key_norm onde estiver nulo
        miss = tmp["calib_home"].isna().sum()
        if miss > 0:
            tmp2 = base.merge(calib[["calib_home", "calib_draw", "calib_away", "key_norm"]],
                              on="key_norm", how="left", suffixes=("", "_c2"))
            # completar onde nulo
            for c in ["calib_home", "calib_draw", "calib_away"]:
                tmp[c] = tmp[c].fillna(tmp2[c])
        base = tmp
    else:
        # sem calib -> duplicar market para os campos calib_* (neutro)
        base["calib_home"] = base["p_home"]
        base["calib_draw"] = base["p_draw"]
        base["calib_away"] = base["p_away"]

    # --- BLEND MARKET + CALIB ---
    # garantir pesos consistentes
    if w_market < 0 or w_calib < 0:
        raise ValueError("Pesos devem ser >= 0.")
    if w_market + w_calib == 0:
        raise ValueError("Soma dos pesos não pode ser 0.")
    wsum = w_market + w_calib
    w_market /= wsum
    w_calib /= wsum

    base["p_blend_home"] = w_market * base["p_home"] + w_calib * base["calib_home"]
    base["p_blend_draw"] = w_market * base["p_draw"] + w_calib * base["calib_draw"]
    base["p_blend_away"] = w_market * base["p_away"] + w_calib * base["calib_away"]

    # normalizar
    s = base["p_blend_home"] + base["p_blend_draw"] + base["p_blend_away"]
    for c in ["p_blend_home", "p_blend_draw", "p_blend_away"]:
        base[c] = (base[c] / s).clip(1e-6, 1 - 1e-6)

    # salvar retrocompat
    out_blend = base[["match_key", "home", "away", "p_blend_home", "p_blend_draw", "p_blend_away"]].copy()
    out_blend = out_blend.rename(columns={
        "p_blend_home": "p_home",
        "p_blend_draw": "p_draw",
        "p_blend_away": "p_away"
    })
    out_blend.to_csv(os.path.join(rodada, "predictions_blend.csv"), index=False)
    dbg(debug, f"OK -> {os.path.join(rodada, 'predictions_blend.csv')}")

    # --- CONTEXT TILT (opcional) ---
    if use_context and ctx_strength > 0:
        if ctx is not None:
            base = base.merge(ctx[["ha_key", "context_score"]], on="ha_key", how="left")
            base["context_score"] = base["context_score"].fillna(0.0).astype(float)
            # tilt via espaço de logits: favorece home se score>0; away se score<0
            # magnitude controlada por ctx_strength (0..1)
            # logits ~ log(p)
            for idx, row in base.iterrows():
                ph, pd, pa = float(row["p_blend_home"]), float(row["p_blend_draw"]), float(row["p_blend_away"])
                # proteção
                ph = clamp01(ph); pd = clamp01(pd); pa = clamp01(pa)
                lh, ld, la = math.log(ph), math.log(pd), math.log(pa)

                tilt = ctx_strength * float(row["context_score"])
                lh2 = lh + tilt
                la2 = la - tilt
                # draw fica neutro
                ph2, pd2, pa2 = softmax3(lh2, ld, la2)

                base.at[idx, "p_final_home"] = ph2
                base.at[idx, "p_final_draw"] = pd2
                base.at[idx, "p_final_away"] = pa2
        else:
            dbg(debug, "Sem contexto disponível — final = blend.")
            base["p_final_home"] = base["p_blend_home"]
            base["p_final_draw"] = base["p_blend_draw"]
            base["p_final_away"] = base["p_blend_away"]
    else:
        # sem uso de contexto
        base["p_final_home"] = base["p_blend_home"]
        base["p_final_draw"] = base["p_blend_draw"]
        base["p_final_away"] = base["p_blend_away"]

    # --- SAÍDA FINAL ---
    out = base[["match_key", "home", "away", "p_final_home", "p_final_draw", "p_final_away"]].copy()
    out = out.rename(columns={
        "p_final_home": "p_home",
        "p_final_draw": "p_draw",
        "p_final_away": "p_away"
    })
    out["used_sources"] = ["market,calib,context" if (use_context and ctx is not None) else "market,calib"] * len(out)
    out["weights"] = [f"market:{w_market:.2f};calib:{w_calib:.2f};context:{ctx_strength:.2f}" if (use_context and ctx is not None) else f"market:{w_market:.2f};calib:{w_calib:.2f}"] * len(out)

    out_path = os.path.join(rodada, "predictions_final.csv")
    out.to_csv(out_path, index=False)
    dbg(debug, f"OK -> {out_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[blend][ERRO] {e}", file=sys.stderr)
        sys.exit(1)