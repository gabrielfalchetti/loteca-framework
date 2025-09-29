#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds de consenso a partir dos arquivos:
  data/out/{rodada}/odds_theoddsapi.csv
  data/out/{rodada}/odds_apifootball.csv

Regras:
- Pelo menos um provedor precisa ter retornado odds (fail-fast caso contrário)
- Concatena e (opcional) aplica desvig/peso/mediana (placeholder simples aqui)
- Salva em data/out/{rodada}/odds.csv
"""

import argparse
import os
import sys
import pandas as pd

def read_if_exists(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception as e:
            print(f"[consensus] ERRO ao ler {path}: {e}", flush=True)
            sys.exit(1)
    return pd.DataFrame()

def simple_consensus(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assume colunas mínimas:
      match_id, home_team, away_team, market (1X2), book, o1, ox, o2
    Se o seu esquema for diferente, adapte aqui.
    """
    if df.empty:
        return df

    # Remove linhas sem odds
    keep_cols = [c for c in df.columns if c.lower() in {"match_id","home_team","away_team","market","o1","ox","o2"}]
    missing = {"match_id","o1","ox","o2"} - set([c.lower() for c in df.columns])
    if missing:
        # Se quiser ser tolerante, só avisa:
        print(f"[consensus] AVISO: colunas esperadas ausentes: {missing}. Mantendo todas as colunas originais.", flush=True)
        return df

    # Desvig simplificado (placeholder): converter odds -> probs e renormalizar
    def odds_to_probs(row):
        try:
            p1 = 1.0/float(row["o1"])
            px = 1.0/float(row["ox"])
            p2 = 1.0/float(row["o2"])
            s = p1 + px + p2
            return pd.Series({"p1": p1/s, "px": px/s, "p2": p2/s})
        except Exception:
            return pd.Series({"p1": None, "px": None, "p2": None})

    probs = df.apply(odds_to_probs, axis=1)
    df_probs = pd.concat([df, probs], axis=1)
    # Agrega por match_id tirando média (ou mediana)
    agg = df_probs.groupby("match_id", as_index=False)[["p1","px","p2"]].mean()
    # Mantém nomes
    first = df.drop_duplicates("match_id")[["match_id","home_team","away_team"]] if "home_team" in df.columns and "away_team" in df.columns else agg[["match_id"]].copy()
    out = first.merge(agg, on="match_id", how="right")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    base = f"data/out/{args.rodada}"
    p_theodds = os.path.join(base, "odds_theoddsapi.csv")
    p_rapid   = os.path.join(base, "odds_apifootball.csv")

    df_list = []
    for name, p in [("theoddsapi", p_theodds), ("apifootball", p_rapid)]:
        df = read_if_exists(p)
        if not df.empty:
            df["provider"] = name
            df_list.append(df)

    if not df_list:
        print("[consensus] ERRO: nenhum provedor retornou odds. Aborte.", flush=True)
        sys.exit(1)

    df_all = pd.concat(df_list, ignore_index=True)
    consensus = simple_consensus(df_all)

    out_path = os.path.join(base, "odds.csv")
    consensus.to_csv(out_path, index=False)
    print(f"[consensus] odds de consenso -> {out_path} (n={len(consensus)})", flush=True)

    # Telemetria resumida
    used = ", ".join(sorted(set(df_all["provider"].unique())))
    print(f"[audit] Odds usadas: {used}", flush=True)

if __name__ == "__main__":
    main()
