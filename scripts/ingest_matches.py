from __future__ import annotations
import argparse
import pandas as pd
from pathlib import Path
from utils_team_aliases import load_aliases, normalize_team

def main():
    ap = argparse.ArgumentParser(description="Ingest de partidas para a rodada")
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    base_in = Path(f"data/in/{args.rodada}")
    base_out = Path(f"data/out/{args.rodada}")
    base_out.mkdir(parents=True, exist_ok=True)

    src = base_in / "matches_source.csv"
    if not src.exists() or src.stat().st_size == 0:
        raise RuntimeError(f"[ingest_matches] Arquivo-fonte ausente/vazio: {src}\nCrie-o com as colunas: match_id,home,away[,date]")

    df = pd.read_csv(src).rename(columns=str.lower)
    need = {"match_id", "home", "away"}
    if not need.issubset(df.columns):
        raise RuntimeError("[ingest_matches] matches_source.csv sem colunas necessárias (match_id, home, away).")

    alias_map = load_aliases()
    df["home"] = df["home"].astype(str).apply(lambda x: normalize_team(x, alias_map))
    df["away"] = df["away"].astype(str).apply(lambda x: normalize_team(x, alias_map))

    out = base_out / "matches.csv"
    df.to_csv(out, index=False)
    print(f"[ingest_matches] OK -> {out}")

if __name__ == "__main__":
    main()
