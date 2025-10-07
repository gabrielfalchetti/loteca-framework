#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consenso de odds: mergeia API-Football (apifoot_odds.csv) + TheOddsAPI (odds_theoddsapi.csv) se existir.
Saída: odds_consensus.csv com (home, away, match_key, odds_home, odds_draw, odds_away)
"""
import os
import sys
import argparse
import pandas as pd

def die(msg):
    print(f"[consensus-safe] ERRO: {msg}", file=sys.stderr)
    sys.exit(1)

def load_csv(path):
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[consensus-safe] ERRO ao ler {path}: {e}")
        return pd.DataFrame()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rodada", required=True, help="OUT_DIR (ex: data/out/123456) ou ID (123456)")
    args = p.parse_args()

    out_dir = args.rodada
    # Se vier apenas o ID, monta o caminho
    if out_dir.isdigit():
        out_dir = os.path.join("data","out",out_dir)

    apifoot_path = os.path.join(out_dir, "apifoot_odds.csv")
    theodds_path = os.path.join(out_dir, "odds_theoddsapi.csv")
    fixtures_path = os.path.join(out_dir, "apifoot_fixtures.csv")

    df_a = load_csv(apifoot_path)
    df_t = load_csv(theodds_path)
    df_f = load_csv(fixtures_path)

    if df_a.empty and df_t.empty:
        die("nenhuma fonte de odds disponível.")

    # preparar odds de cada fonte para colunas padrão
    out_rows = []

    # (A) API-Football
    if not df_a.empty and not df_f.empty:
        # junta por fixture_id para obter nomes
        # df_a: fixture_id, bookmaker, odds_home, odds_draw, odds_away
        # df_f: fixture_id, home, away
        dfm = pd.merge(
            df_a[["fixture_id","bookmaker","odds_home","odds_draw","odds_away"]],
            df_f[["fixture_id","home","away"]],
            on="fixture_id", how="left"
        )
        for _, r in dfm.iterrows():
            home = str(r["home"]).strip()
            away = str(r["away"]).strip()
            mk = f"{home.lower()}__vs__{away.lower()}"
            out_rows.append({
                "provider": "apifoot",
                "match_key": mk,
                "team_home": home,
                "team_away": away,
                "odds_home": r.get("odds_home"),
                "odds_draw": r.get("odds_draw"),
                "odds_away": r.get("odds_away")
            })

    # (B) TheOddsAPI (assume já salva no formato padronizado do seu script)
    if not df_t.empty:
        # esperado: team_home, team_away, match_key, odds_home, odds_draw, odds_away
        # se não tiver match_key, cria
        if "match_key" not in df_t.columns:
            df_t["match_key"] = (df_t["team_home"].str.lower().str.strip() +
                                 "__vs__" +
                                 df_t["team_away"].str.lower().str.strip())
        for _, r in df_t.iterrows():
            out_rows.append({
                "provider": "theoddsapi",
                "match_key": r["match_key"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "odds_home": r.get("odds_home"),
                "odds_draw": r.get("odds_draw"),
                "odds_away": r.get("odds_away")
            })

    if not out_rows:
        die("falha ao consolidar odds (linhas vazias).")

    df = pd.DataFrame(out_rows)

    # agregação simples: média das odds por match_key
    agg = df.groupby("match_key").agg({
        "team_home":"first",
        "team_away":"first",
        "odds_home":"mean",
        "odds_draw":"mean",
        "odds_away":"mean"
    }).reset_index()

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "odds_consensus.csv")
    agg.to_csv(out_path, index=False)
    print(f"[consensus-safe] OK -> {out_path} ({len(agg)} linhas)")

if __name__ == "__main__":
    main()