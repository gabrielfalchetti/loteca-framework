#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
feature_join_context.py

Une todas as features de contexto (univariado, bivariado, xg, clima, lesões e notícias)
em uma única tabela alinhada por match_key/match_id, usando a whitelist como referência.

Saída: {OUT_DIR}/context_features.csv
"""

import argparse
import os
import sys
import math
from typing import Optional, List
import pandas as pd


def dbg(enabled: bool, *msg):
    if enabled:
        print("[context]", *msg, flush=True)


def read_csv_safe(path: str, debug: bool, required: bool = False) -> Optional[pd.DataFrame]:
    if not os.path.isfile(path):
        if required:
            raise FileNotFoundError(f"Arquivo obrigatório não encontrado: {path}")
        dbg(debug, f"AVISO: arquivo opcional ausente -> {path}")
        return None
    try:
        df = pd.read_csv(path)
    except Exception as e:
        if required:
            raise
        dbg(debug, f"AVISO: falha ao ler {path}: {e}")
        return None
    if df.shape[0] == 0:
        if required:
            raise ValueError(f"Arquivo obrigatório vazio: {path}")
        dbg(debug, f"AVISO: arquivo opcional vazio -> {path}")
        return None
    return df


def coerce_cols_str(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df


def ensure_context_score(ctx: pd.DataFrame, debug: bool) -> pd.DataFrame:
    ctx = ctx.copy()
    if "context_score" in ctx.columns:
        return ctx

    w_gap = 0.30
    w_xg = 0.30
    w_inj = 0.20
    w_news = 0.15
    w_weather = 0.05

    score = pd.Series(0.0, index=ctx.index, dtype=float)

    if "diff_ph_pa" in ctx.columns:
        score = score + w_gap * ctx["diff_ph_pa"].fillna(0.0)

    if "xg_diff_proxy" in ctx.columns:
        score = score + w_xg * ctx["xg_diff_proxy"].fillna(0.0)

    if {"inj_home", "inj_away"}.issubset(ctx.columns):
        inj_h = ctx["inj_home"].fillna(0.0).astype(float)
        inj_a = ctx["inj_away"].fillna(0.0).astype(float)
        denom = (inj_h + inj_a).replace(0, 1.0)
        score = score + w_inj * (inj_a - inj_h) / denom

    if {"news_mentions_home", "news_mentions_away"}.issubset(ctx.columns):
        n_h = ctx["news_mentions_home"].fillna(0.0).astype(float)
        n_a = ctx["news_mentions_away"].fillna(0.0).astype(float)
        denom = (n_h + n_a).replace(0, 1.0)
        score = score + w_news * (n_h - n_a) / denom

    if {"wind_speed_kph", "precip_mm"}.issubset(ctx.columns):
        wind = ctx["wind_speed_kph"].fillna(0.0).astype(float)
        rain = ctx["precip_mm"].fillna(0.0).astype(float)
        penal = (wind > 30).astype(float) * 0.5 + (rain > 0.5).astype(float) * 0.5
        score = score - w_weather * penal

    score = score.clip(-1.0, 1.0)
    ctx["context_score"] = score
    return ctx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída da rodada (ex: data/out/123456)")
    ap.add_argument("--debug", action="store_true", help="Logs verbosos")
    args = ap.parse_args()

    out_dir = args.rodada
    debug = args.debug

    wl_path   = os.path.join(out_dir, "matches_whitelist.csv")
    uni_path  = os.path.join(out_dir, "features_univariado.csv")
    bi_path   = os.path.join(out_dir, "features_bivariado.csv")
    xg_path   = os.path.join(out_dir, "features_xg.csv")
    wth_path  = os.path.join(out_dir, "weather.csv")
    inj_path  = os.path.join(out_dir, "injuries.csv")
    news_path = os.path.join(out_dir, "news.csv")

    wl = read_csv_safe(wl_path, debug, required=True)
    wl = coerce_cols_str(wl, ["match_id", "match_key", "team_home", "team_away"])
    wl = wl.rename(columns={"team_home": "home", "team_away": "away"})
    base_cols = [c for c in ["match_id", "match_key", "home", "away"] if c in wl.columns]
    if not {"match_id", "match_key"}.issubset(set(base_cols)):
        raise ValueError("Whitelist deve conter colunas 'match_id' e 'match_key'.")

    df = wl[base_cols].drop_duplicates().copy()
    dbg(debug, f"Base (whitelist) linhas={len(df)}")

    uni = read_csv_safe(uni_path, debug, required=True)
    uni = coerce_cols_str(uni, ["match_key"])
    drop_uni = {"home", "away", "odd_home", "odd_draw", "odd_away"}
    uni_cols = [c for c in uni.columns if c not in drop_uni]
    df = df.merge(uni[uni_cols], on="match_key", how="left")

    bi = read_csv_safe(bi_path, debug, required=True)
    bi = coerce_cols_str(bi, ["match_key"])
    df = df.merge(bi.drop(columns=[c for c in ["home", "away"] if c in bi.columns]),
                  on="match_key", how="left")

    xg = read_csv_safe(xg_path, debug, required=True)
    xg = coerce_cols_str(xg, ["match_key"])
    df = df.merge(xg.drop(columns=[c for c in ["home", "away"] if c in xg.columns]),
                  on="match_key", how="left")

    wth = read_csv_safe(wth_path, debug, required=False)
    if wth is not None:
        wth = coerce_cols_str(wth, ["match_id"])
        keep_w = ["match_id", "temp_c", "apparent_temp_c", "wind_speed_kph",
                  "wind_gust_kph", "wind_dir_deg", "precip_mm", "precip_prob",
                  "relative_humidity", "cloud_cover", "pressure_hpa"]
        keep_w = [c for c in keep_w if c in wth.columns]
        df = df.merge(wth[keep_w], on="match_id", how="left")
    else:
        for c in ["temp_c", "apparent_temp_c", "wind_speed_kph", "wind_gust_kph",
                  "wind_dir_deg", "precip_mm", "precip_prob", "relative_humidity",
                  "cloud_cover", "pressure_hpa"]:
            df[c] = pd.NA

    inj = read_csv_safe(inj_path, debug, required=False)
    if inj is not None:
        team_col = None
        for cand in ["team_name", "team", "time", "club", "squad"]:
            if cand in inj.columns:
                team_col = cand
                break
        if team_col is None:
            for cand in inj.columns:
                if "team" in cand.lower() and "name" in cand.lower():
                    team_col = cand
                    break
        if team_col is not None:
            inj_counts = inj.groupby(team_col, dropna=False).size().reset_index(name="inj_count")
            inj_counts[team_col] = inj_counts[team_col].astype(str)
            inj_map = dict(zip(inj_counts[team_col], inj_counts["inj_count"]))
            df["inj_home"] = df["home"].map(inj_map).fillna(0).astype(int)
            df["inj_away"] = df["away"].map(inj_map).fillna(0).astype(int)
            df["inj_total"] = df["inj_home"] + df["inj_away"]
        else:
            dbg(debug, "AVISO: injuries.csv sem coluna de nome de time reconhecida; preenchendo zeros.")
            for c in ["inj_home", "inj_away", "inj_total"]:
                df[c] = 0
    else:
        for c in ["inj_home", "inj_away", "inj_total"]:
            df[c] = 0

    news = read_csv_safe(news_path, debug, required=False)
    if news is not None and {"title", "description"}.issubset(set(news.columns)):
        tmp = news.copy()
        tmp["__text"] = (tmp["title"].astype(str) + " " + tmp["description"].astype(str)).str.lower()
        def count_mentions(team: str) -> int:
            if team is None or (isinstance(team, float) and math.isnan(team)):
                return 0
            t = str(team).lower()
            return int(tmp["__text"].str.contains(t, na=False).sum())
        df["news_mentions_home"] = df["home"].apply(count_mentions)
        df["news_mentions_away"] = df["away"].apply(count_mentions)
        df["news_mentions_total"] = df["news_mentions_home"] + df["news_mentions_away"]
    else:
        for c in ["news_mentions_home", "news_mentions_away", "news_mentions_total"]:
            df[c] = 0

    df = ensure_context_score(df, debug)

    front = [c for c in ["match_id", "match_key", "home", "away"] if c in df.columns]
    other = [c for c in df.columns if c not in front]
    df = df[front + other]

    out_path = os.path.join(out_dir, "context_features.csv")
    df.to_csv(out_path, index=False)
    dbg(debug, f"OK -> {out_path} (linhas={len(df)}, colunas={len(df.columns)})")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[context][ERRO] {e}", file=sys.stderr)
        sys.exit(1)