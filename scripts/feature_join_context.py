#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Une features e contexto (weather/news) + whitelist -> context_features.csv

Regra:
- Obrigatório: matches_whitelist.csv
- Pelo menos UM entre: features_univariado.csv, features_bivariado.csv, features_xg.csv
- Opcional (mas se existirem são incorporados): weather.csv, news.csv
- Saída: {OUT_DIR}/context_features.csv (com match_id, team_home, team_away sempre presentes)
"""

import os
import sys
import argparse
import pandas as pd

EXIT_CODE = 28

REQ_FEATURE_FILES = [
    "features_univariado.csv",
    "features_bivariado.csv",
    "features_xg.csv",
]

def eprint(*a, **k):
    print(*a, file=sys.stderr, **k)

def read_csv_safe(path: str, required=False, err_code=EXIT_CODE):
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        if required:
            eprint(f"::error::Arquivo obrigatório ausente/vazio: {path}")
            sys.exit(err_code)
        return None
    try:
        return pd.read_csv(path)
    except Exception as ex:
        if required:
            eprint(f"::error::Falha ao ler {path}: {ex}")
            sys.exit(err_code)
        eprint(f"[join] aviso ao ler {path}: {ex}")
        return None

def normalize_teams(df, home_col="home", away_col="away"):
    cols = {c.lower(): c for c in df.columns}
    home = cols.get("team_home") or cols.get("home")
    away = cols.get("team_away") or cols.get("away")
    if home is None or away is None:
        raise ValueError("Colunas de time não encontradas (home/away).")
    if "team_home" not in df.columns:
        df = df.rename(columns={home: "team_home"})
    if "team_away" not in df.columns:
        df = df.rename(columns={away: "team_away"})
    return df

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="Diretório da rodada (OUT_DIR)")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    wl_path = os.path.join(out_dir, "matches_whitelist.csv")
    wl = read_csv_safe(wl_path, required=True)
    wl = normalize_teams(wl)
    # mantém colunas essenciais
    base = wl[["match_id", "team_home", "team_away"]].copy()

    # Carrega features disponíveis
    feats = base.copy()
    any_feature = False
    for fname in REQ_FEATURE_FILES:
        path = os.path.join(out_dir, fname)
        df = read_csv_safe(path, required=False)
        if df is None:
            continue
        # normaliza chaves
        if "match_id" not in df.columns:
            # tenta inferir
            maybe = [c for c in df.columns if c.lower() == "match_id"]
            if maybe:
                df = df.rename(columns={maybe[0]: "match_id"})
        df = normalize_teams(df)
        # evita colisão de colunas duplicadas
        for c in ["team_home", "team_away"]:
            if c in df.columns and c not in feats.columns:
                pass
        # junta
        feats = feats.merge(df.drop_duplicates(subset=["match_id"]), on=["match_id", "team_home", "team_away"], how="left", suffixes=("", f"_{fname.split('.')[0]}"))
        any_feature = True

    if not any_feature:
        eprint("::error::Nenhuma feature encontrada (esperado ao menos uma entre univariado/bivariado/xg).")
        sys.exit(EXIT_CODE)

    # Weather (opcional)
    weather_path = os.path.join(out_dir, "weather.csv")
    weather = read_csv_safe(weather_path, required=False)
    if weather is not None and "match_id" in weather.columns:
        weather_cols = [c for c in weather.columns if c not in {"home","away"}]
        feats = feats.merge(weather[["match_id"] + weather_cols].drop_duplicates("match_id"),
                            on="match_id", how="left")

    # News (opcional)
    news_path = os.path.join(out_dir, "news.csv")
    news = read_csv_safe(news_path, required=False)
    if news is not None:
        # compacta num score simples: número de artigos
        if "articles_json" in news.columns:
            news["_news_count"] = news["articles_json"].fillna("[]").apply(lambda s: len(eval(s)) if isinstance(s, str) else 0)
        elif "articles" in news.columns:
            news["_news_count"] = news["articles"].fillna("[]").apply(lambda s: len(eval(s)) if isinstance(s, str) else 0)
        else:
            news["_news_count"] = 0
        keep = ["match_id", "_news_count"]
        if "updated_at" in news.columns:
            keep.append("updated_at")
        news_small = news[keep].drop_duplicates("match_id")
        feats = feats.merge(news_small, on="match_id", how="left")

    out_csv = os.path.join(out_dir, "context_features.csv")
    feats.to_csv(out_csv, index=False)

    if os.path.getsize(out_csv) == 0:
        eprint("::error::context_features.csv não gerado")
        sys.exit(EXIT_CODE)

    if args.debug:
        eprint(f"[context] OK gerado: {out_csv}  linhas={len(feats)}")

if __name__ == "__main__":
    main()