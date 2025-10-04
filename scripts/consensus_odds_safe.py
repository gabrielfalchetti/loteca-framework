# -*- coding: utf-8 -*-
"""
Consenso de odds (SAFE)
- Lê quaisquer arquivos disponíveis em data/out/<rodada>:
  - odds_theoddsapi.csv
  - odds_apifootball.csv
- Se ambos existirem para o mesmo match_key, prioriza média simples (ou escolhe TheOddsAPI se quiser)
- Se só existir um provedor, usa o disponível.
- Exige apenas que a linha tenha pelo menos 2 odds > 1.0 (home/draw/away) para considerar válida.
- Saída: data/out/<rodada>/odds_consensus.csv
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd

REQUIRED_COLS = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]

def read_csv_safe(p: Path) -> pd.DataFrame:
    if not p.exists():
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {p}")
        return pd.DataFrame(columns=REQUIRED_COLS)
    df = pd.read_csv(p)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        print(f"[consensus-safe] AVISO: faltam colunas {missing} em {p.name}; ignorando.")
        return pd.DataFrame(columns=REQUIRED_COLS)
    return df[REQUIRED_COLS].copy()

def at_least_two_odds_valid(row) -> bool:
    vals = [row["odds_home"], row["odds_draw"], row["odds_away"]]
    try:
        cnt = sum(float(x) > 1.0 for x in vals if pd.notna(x))
        return cnt >= 2
    except Exception:
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)

    df_to = read_csv_safe(out_dir/"odds_theoddsapi.csv")
    df_af = read_csv_safe(out_dir/"odds_apifootball.csv")

    # junta por match_key; se só um existe, usa; se ambos, média simples de odds
    if df_to.empty and df_af.empty:
        print("[consensus-safe] ERRO: nenhuma fonte de odds disponível.")
        sys.exit(1)

    df_to["src"] = "theoddsapi"
    df_af["src"] = "apifootball"
    df = pd.concat([df_to, df_af], ignore_index=True)

    # mantém a 1ª ocorrência por (match_key, src) e depois agrega por match_key
    df = df.drop_duplicates(subset=["match_key","src"])

    # agrega: média das odds quando houver as duas fontes
    agg = (df.groupby("match_key")
             .agg({
                 "team_home":"first",
                 "team_away":"first",
                 "odds_home":"mean",
                 "odds_draw":"mean",
                 "odds_away":"mean"
             })
             .reset_index())

    # valida
    before = len(agg)
    agg["__valid"] = agg.apply(at_least_two_odds_valid, axis=1)
    agg = agg[agg["__valid"]].drop(columns="__valid")
    after = len(agg)

    if after == 0:
        print("[consensus-safe] consenso bruto: 0 (soma linhas válidas dos provedores); finais (>=2 odds > 1.0): 0")
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(1)

    agg.to_csv(out_dir/"odds_consensus.csv", index=False)
    print(f"[consensus-safe] OK -> {out_dir/'odds_consensus.csv'} ({after} linhas) | mapping theoddsapi/apifootball: team_home='team_home', team_away='team_away', match_key='match_key', odds_home='odds_home', odds_draw='odds_draw', odds_away='odds_away'")

if __name__ == "__main__":
    main()