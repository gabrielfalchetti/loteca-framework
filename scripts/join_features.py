#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import pandas as pd
import numpy as np

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    return ap.parse_args()

def to_str_id(s):
    # padroniza match_id como string para evitar erros de merge (object vs int)
    if pd.isna(s):
        return ""
    return str(s).strip()

def load_matches(rodada):
    f = f"data/in/{rodada}/matches_source.csv"
    df = pd.read_csv(f)
    # colunas mínimas
    req = ["match_id","home_team","away_team"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"[join_features] Campo obrigatório ausente em {f}: {c}")
    # campos úteis opcionais
    for c in ["league_name","country","kickoff_utc","season"]:
        if c not in df.columns:
            df[c] = np.nan
    # normaliza tipos
    df["match_id"] = df["match_id"].apply(to_str_id)
    # garante datetime (se existir)
    if "kickoff_utc" in df.columns:
        df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce")
    return df

def load_odds(rodada):
    f = f"data/out/{rodada}/odds.csv"
    if not os.path.exists(f):
        # odds podem não existir ainda — retorna df vazio com colunas padrão
        return pd.DataFrame(columns=["match_id","k1","kx","k2"])
    df = pd.read_csv(f)

    # padroniza match_id
    if "match_id" not in df.columns:
        df["match_id"] = ""
    df["match_id"] = df["match_id"].apply(to_str_id)

    # Mapeia nomes antigos/novos para k1/kx/k2
    # Novo esquema (proposto): home_price/draw_price/away_price
    if set(["home_price","draw_price","away_price"]).issubset(df.columns):
        df["k1"] = pd.to_numeric(df["home_price"], errors="coerce")
        df["kx"] = pd.to_numeric(df["draw_price"], errors="coerce")
        df["k2"] = pd.to_numeric(df["away_price"], errors="coerce")
    # Antigo esquema: já vem k1/kx/k2
    else:
        for c in ["k1","kx","k2"]:
            if c not in df.columns:
                df[c] = np.nan
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # agrega por match_id (média de várias casas, se houver)
    out = df.groupby("match_id", as_index=False).agg({"k1":"mean","kx":"mean","k2":"mean"})
    return out

def implied_probs_no_vig(k1, kx, k2):
    # Converte odds decimais para prob. implícitas sem retirar vigorish
    p1 = 1.0 / k1 if (k1 and k1 > 0) else np.nan
    pX = 1.0 / kx if (kx and kx > 0) else np.nan
    p2 = 1.0 / k2 if (k2 and k2 > 0) else np.nan
    return p1, pX, p2

def remove_vigorish(p1, pX, p2):
    # Normaliza para somar 1 quando as três estiverem presentes
    vec = np.array([p1, pX, p2], dtype=float)
    if np.isnan(vec).any():
        return p1, pX, p2
    s = vec.sum()
    if s <= 0:
        return p1, pX, p2
    return tuple(vec / s)

def main():
    args = parse_args()
    rodada = args.rodada
    outdir = f"data/out/{rodada}"
    os.makedirs(outdir, exist_ok=True)

    matches = load_matches(rodada)
    odds = load_odds(rodada)

    # faz o merge (sempre por string)
    feats = matches.merge(odds[["match_id","k1","kx","k2"]], on="match_id", how="left")

    # Probabilidades implícitas + desvig
    p_cols = {"p1_raw":[],"pX_raw":[],"p2_raw":[],"p1":[],"pX":[],"p2":[]}
    for _, r in feats.iterrows():
        k1, kx, k2 = r.get("k1"), r.get("kx"), r.get("k2")
        p1_raw, pX_raw, p2_raw = implied_probs_no_vig(k1, kx, k2)
        p1, pX, p2 = remove_vigorish(p1_raw, pX_raw, p2_raw)
        p_cols["p1_raw"].append(p1_raw)
        p_cols["pX_raw"].append(pX_raw)
        p_cols["p2_raw"].append(p2_raw)
        p_cols["p1"].append(p1)
        p_cols["pX"].append(pX)
        p_cols["p2"].append(p2)

    for k, v in p_cols.items():
        feats[k] = v

    # Ordena e salva artefatos
    cols_first = ["match_id","country","league_name","season","kickoff_utc","home_team","away_team",
                  "k1","kx","k2","p1_raw","pX_raw","p2_raw","p1","pX","p2"]
    # adiciona colunas que existirem; ignora as ausentes
    cols_final = [c for c in cols_first if c in feats.columns] + \
                 [c for c in feats.columns if c not in cols_first]

    matches_out = f"{outdir}/matches.csv"
    feats_out   = f"{outdir}/features_base.csv"

    # matches.csv “limpo”
    matches[["match_id","home_team","away_team","country","league_name","season","kickoff_utc"]].to_csv(matches_out, index=False)
    feats[cols_final].to_csv(feats_out, index=False)

    print(f"[join_features] matches -> {matches_out} ({len(matches)} linhas)")
    print(f"[join_features] features_base -> {feats_out} ({len(feats)} linhas)")
    print(f"[join_features] OK — rodada={rodada}")

if __name__ == "__main__":
    main()
