#!/usr/bin/env python3
import sys, json, argparse
import pandas as pd
from unidecode import unidecode
from pathlib import Path

def norm(s: str) -> str:
    return unidecode(str(s or "")).strip().lower()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--leaguemap", default="data/theoddsapi_league_map.json")
    args = ap.parse_args()

    rodada_dir = Path(f"data/in/{args.rodada}")
    ms_path = rodada_dir / "matches_source.csv"
    if not ms_path.exists():
        print(f"[validate] ERRO: {ms_path} não existe", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(ms_path)
    missing_cols = [c for c in ["match_id","home","away"] if c not in df.columns]
    if missing_cols:
        print(f"[validate] ERRO: matches_source sem colunas {missing_cols}", file=sys.stderr)
        sys.exit(2)

    # carrega league map
    try:
        leaguemap = json.loads(Path(args.leaguemap).read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[validate] AVISO: não consegui ler {args.leaguemap}: {e}")
        leaguemap = {}

    # index auxiliar por alias normalizado
    alias_to_key = {}
    for league_name, info in leaguemap.items():
        key = info.get("sport_key")
        for alias in set([league_name] + info.get("aliases", [])):
            alias_to_key[norm(alias)] = key

    problems = []
    for _, r in df.iterrows():
        sk = str(r.get("sport_key") or "").strip()
        lg = str(r.get("league") or "").strip()
        mid = r["match_id"]

        # já tem sport_key? ok
        if sk:
            continue

        # tenta resolver por league
        key = alias_to_key.get(norm(lg))
        if key:
            continue

        problems.append((mid, lg))

    if problems:
        print("[validate] ERRO: partidas sem sport_key e liga não mapeada:")
        for mid, lg in problems:
            print(f"  - match_id={mid} league='{lg}'")
        print("→ Soluções: (a) preencha 'sport_key' no CSV; ou (b) inclua a liga no data/theoddsapi_league_map.json")
        sys.exit(3)

    print("[validate] OK — sport_keys resolvidos para todas as partidas.")

if __name__ == "__main__":
    main()
