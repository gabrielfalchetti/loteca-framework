#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monta o CSV de consenso de odds: data/out/<rodada>/odds.csv
Regra simples e robusta:
- lê todos os provedores disponíveis (hoje: TheOddsAPI, e futuramente outros);
- prioriza a linha mais “completa” (k1,kx,k2 válidos) por match_id;
- se nenhum provedor trouxe odds para NENHUM jogo, explica a causa provável e aborta (exit 1);
- se trouxe para alguns, segue com o que tem.

Colunas de saída: match_id, k1, kx, k2, provider, fetched_at
"""

import sys
import argparse
from pathlib import Path
import pandas as pd

PROVIDERS = [
    ("odds_theoddsapi.csv", "theoddsapi"),
    # futuramente: ("odds_apifootball.csv", "apifootball")
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "odds.csv"

    frames = []
    used = []
    for fname, pname in PROVIDERS:
        p = out_dir / fname
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if df.empty:
            continue
        # valida colunas
        need = ["match_id","k1","kx","k2"]
        if not all(c in df.columns for c in need):
            continue
        # filtra odds válidas
        df = df.dropna(subset=["match_id","k1","kx","k2"])
        if df.empty:
            continue
        frames.append(df.assign(provider=pname))
        used.append(pname)

    if not frames:
        print("[consensus] ERRO: nenhum provedor retornou odds. Aborte.", file=sys.stderr)
        print("Causas comuns:", file=sys.stderr)
        print("  - 'sport_key' inválido ou ausente no matches_source.csv;", file=sys.stderr)
        print("  - data do jogo muito distante (mercado ainda não aberto);", file=sys.stderr)
        print("  - nomes de times com variações que impediram o matching (ajuste aliases).", file=sys.stderr)
        sys.exit(1)

    df_all = pd.concat(frames, ignore_index=True)
    # regra: por match_id, preferir linha que tenha k1*kx*k2 maior cobertura (todas presentes) — aqui já filtramos nulos.
    # se houver duplicidade por provedor, fica a primeira.
    df_all = df_all.drop_duplicates(subset=["match_id"], keep="first")

    df_all.to_csv(out_path, index=False)
    print(f"[consensus] odds de consenso -> {out_path} (n={len(df_all)})")
    print("[audit] Odds usadas:", ", ".join(sorted(set(used))) if used else "nenhuma")

if __name__ == "__main__":
    main()
