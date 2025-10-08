#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
feature_join_context.py
Une features (uni, bivariado, xg) + sinais leves de weather/injuries/news
e gera um context_score normalizado por jogo.

Saída: data/out/<RID>/context_features.csv
"""

from __future__ import annotations
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import re

def _read_csv(p: Path, needed: list[str] | None = None) -> pd.DataFrame:
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=needed or [])
    df = pd.read_csv(p)
    if needed:
        for c in needed:
            if c not in df.columns:
                df[c] = np.nan
    return df

def _safe_norm(v: pd.Series) -> pd.Series:
    v = v.astype(float)
    if v.std(ddof=0) == 0 or not np.isfinite(v.std(ddof=0)):
        return pd.Series(0.0, index=v.index)
    x = (v - v.mean()) / (v.std(ddof=0) + 1e-9)
    # squashing para [-1, 1]
    x = np.tanh(x)
    return x

def _mk_match_id(row):
    # tenta match_key; se não houver, cria a partir de home/away
    if "match_key" in row and isinstance(row["match_key"], str) and "__vs__" in row["match_key"]:
        return row["match_key"].replace("__vs__", "__").title()
    if "home" in row and "away" in row:
        return f"{str(row['home']).title()}__{str(row['away']).title()}"
    if "team_home" in row and "team_away" in row:
        return f"{str(row['team_home']).title()}__{str(row['team_away']).title()}"
    return np.nan

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Ex.: data/out/1759959606")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    out_dir = Path(args.rodada)

    # Base probabilística (preferir blend; se não existir, usar market)
    p_blend = out_dir / "predictions_blend.csv"
    p_market = out_dir / "predictions_market.csv"
    if p_blend.exists() and p_blend.stat().st_size > 0:
        base = pd.read_csv(p_blend)
        # uniformiza naming
        if not {"home","away"}.issubset(base.columns):
            base = base.rename(columns={"team_home":"home","team_away":"away"})
    else:
        base = pd.read_csv(p_market)
    if "match_id" not in base.columns:
        base["match_id"] = base.apply(_mk_match_id, axis=1)

    # Carrega features
    f_uni = _read_csv(out_dir / "features_univariado.csv")
    f_bi  = _read_csv(out_dir / "features_bivariado.csv")
    f_xg  = _read_csv(out_dir / "features_xg.csv")

    # Normaliza chaves nessas bases
    for df in (f_uni, f_bi, f_xg):
        if "match_key" in df.columns and "__vs__" in str(df["match_key"].iloc[0] if len(df) else ""):
            df["match_id"] = df["match_key"].str.replace("__vs__", "__", regex=False).str.title()
        elif {"home","away"}.issubset(df.columns):
            df["match_id"] = (df["home"].astype(str).str.title()+"__"+df["away"].astype(str).str.title())

    # Signals de weather
    w = _read_csv(out_dir / "weather.csv", needed=["match_id","temp_c","wind_speed_kph","precip_mm","relative_humidity"])
    # se não existir match_id em weather, tenta casar por lat/lon de matches_source (fora do escopo aqui)

    # Signals de injuries: simples contagem por time (se houver)
    inj = _read_csv(out_dir / "injuries.csv")
    inj_cols = [c for c in inj.columns]
    if "team_name" in inj_cols:
        # agrupar por time
        inj_grp = inj.groupby("team_name", dropna=False).size().reset_index(name="inj_count")
    else:
        inj_grp = pd.DataFrame(columns=["team_name","inj_count"])

    # Signals de news: se tiver, usar contagem/score bruto por time (bem leve)
    news = _read_csv(out_dir / "news.csv", needed=["title","description"])
    if not news.empty:
        text = (news.get("title","").astype(str).str.lower() + " " + news.get("description","").astype(str).str.lower())
        # muito simples: menções por time (home/away) serão computadas depois, via contains
        news["text"] = text.fillna("")

    # --- Seleção de features principais ---
    # Univariado: gap_home_away (quanto maior, mais pro mandante)
    uni_gap = f_uni[["match_id","gap_home_away","gap_top_second","overround"]].copy() if not f_uni.empty else pd.DataFrame(columns=["match_id","gap_home_away","gap_top_second","overround"])
    # Bivariado: diff_ph_pa (p_home - p_away), ratio_ph_pa
    bi_cols = ["match_id","diff_ph_pa","ratio_ph_pa","entropy_x_gap","overround_x_entropy"]
    bi_gap = f_bi[bi_cols].copy() if not f_bi.empty else pd.DataFrame(columns=bi_cols)
    # xG proxy: xg_diff_proxy
    xg_cols = ["match_id","xg_diff_proxy","xg_home_proxy","xg_away_proxy"]
    xg_gap = f_xg[xg_cols].copy() if not f_xg.empty else pd.DataFrame(columns=xg_cols)

    # Merge features
    df = base.merge(uni_gap, on="match_id", how="left").merge(bi_gap, on="match_id", how="left").merge(xg_gap, on="match_id", how="left")

    # Weather join: assumir mesma ordem de whitelist -> se match_id não existir, ignorar silenciosamente
    if "match_id" in w.columns:
        df = df.merge(w[["match_id","temp_c","wind_speed_kph","precip_mm","relative_humidity"]], on="match_id", how="left")

    # Injuries: contagem home/away
    def count_inj(team):
        if inj_grp.empty or pd.isna(team): return 0
        team = str(team).lower()
        # heurística básica de matching
        row = inj_grp[inj_grp["team_name"].astype(str).str.lower().str.contains(re.escape(team), na=False)]
        return int(row["inj_count"].sum()) if len(row) else 0

    df["inj_home"] = df["home"].apply(count_inj)
    df["inj_away"] = df["away"].apply(count_inj)
    df["inj_diff"] = df["inj_away"] - df["inj_home"]  # positivo: prejudica o visitante menos (ou o mandante mais)

    # News: menções (bruto)
    def news_hits(team):
        if news.empty or pd.isna(team): return 0
        team = str(team).lower()
        return int(news["text"].str.contains(re.escape(team), na=False).sum())
    df["news_home_hits"] = df["home"].astype(str).str.lower().apply(news_hits)
    df["news_away_hits"] = df["away"].astype(str).str.lower().apply(news_hits)
    df["news_diff"] = df["news_home_hits"] - df["news_away_hits"]

    # --- Construção do context_score (−1 .. +1) ---
    # sinais que favorecem o mandante:
    signals = []
    signals.append(_safe_norm(df.get("gap_home_away", pd.Series(0, index=df.index))))
    signals.append(_safe_norm(df.get("diff_ph_pa", pd.Series(0, index=df.index))))
    signals.append(_safe_norm(df.get("xg_diff_proxy", pd.Series(0, index=df.index))))

    # clima ruim pode reduzir vantagem casa (vento/chuva) — simples penalização
    wind = _safe_norm(df.get("wind_speed_kph", pd.Series(0, index=df.index))) * (-0.1)
    rain = _safe_norm(df.get("precip_mm", pd.Series(0, index=df.index))) * (-0.15)
    signals.append(wind)
    signals.append(rain)

    # injuries: mais lesões no mandante -> piora score; mais no visitante -> melhora score
    inj_norm = _safe_norm(df.get("inj_diff", pd.Series(0, index=df.index))) * (-0.2)  # inj_diff = away - home
    signals.append(inj_norm)

    # news: home mais “em alta” (menções) melhora levemente
    news_norm = _safe_norm(df.get("news_diff", pd.Series(0, index=df.index))) * (0.05)
    signals.append(news_norm)

    ctx = sum(signals)
    # clipping para [-1, 1]
    df["context_score"] = ctx.clip(-1.0, 1.0).astype(float)

    # Exporta
    keep = ["match_id","home","away","context_score",
            "gap_home_away","diff_ph_pa","xg_diff_proxy",
            "wind_speed_kph","precip_mm","inj_home","inj_away","news_home_hits","news_away_hits"]
    for c in keep:
        if c not in df.columns:
            df[c] = np.nan
    out = df[keep].copy()
    out.to_csv(out_dir / "context_features.csv", index=False)
    if args.debug:
        print("[context] OK ->", out_dir / "context_features.csv")
        print(out.head().to_string(index=False))

if __name__ == "__main__":
    main()
