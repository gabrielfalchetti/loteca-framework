# -*- coding: utf-8 -*-
"""
Ingest TheOddsAPI (SAFE)
- Busca odds H2H em Campeonato e Série B (soccer_brazil_campeonato, soccer_brazil_serie_b)
- Casa eventos com matches_source.csv via normalização/aliases
- Gera:
  - data/out/<rodada>/odds_theoddsapi.csv (linhas válidas casadas)
  - data/out/<rodada>/unmatched_theoddsapi.csv (não casadas)
"""
from __future__ import annotations
import argparse, os, sys, json
from pathlib import Path
import requests
import pandas as pd
from scripts.text_normalizer import load_aliases, canonicalize_team, make_match_key, equals_team

SPORTS = ["soccer_brazil_campeonato", "soccer_brazil_serie_b"]

def get_events(api_key: str, regions: str, debug=False):
    base = "https://api.the-odds-api.com/v4/sports/{sport}/odds"
    headers = {}
    params = {"apiKey": api_key, "regions": regions, "markets":"h2h"}
    all_rows = []
    for sp in SPORTS:
        url = base.format(sport=sp)
        if debug: print(f"[theoddsapi-safe][DEBUG] GET {url} {params}")
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for ev in data:
            # structure: home_team, away_team, bookmakers -> markets -> outcomes
            home = ev.get("home_team","") or ""
            away = ev.get("away_team","") or ""
            # pegar melhor média simples do market h2h
            oh = od = oa = None
            for bk in ev.get("bookmakers", []):
                for mk in bk.get("markets", []):
                    if mk.get("key") == "h2h":
                        vals = {o["name"]: o.get("price") for o in mk.get("outcomes", [])}
                        # nomes podem ser "Home","Draw","Away" ou equivalentes ao time
                        oh = vals.get("Home", oh) or vals.get(home, oh)
                        od = vals.get("Draw", od)
                        oa = vals.get("Away", oa) or vals.get(away, oa)
            all_rows.append({"team_home": home, "team_away": away, "odds_home": oh, "odds_draw": od, "odds_away": oa})
    return pd.DataFrame(all_rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    api_key = os.getenv("THEODDS_API_KEY")
    if not api_key:
        print("[theoddsapi-safe] SKIP: THEODDS_API_KEY ausente.")
        return

    in_dir  = Path(f"data/in/{args.rodada}")
    out_dir = Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # fonte (matches)
    src_path = in_dir/"matches_source.csv"
    if not src_path.exists():
        print(f"[theoddsapi-safe] ERRO: {src_path} não encontrado")
        sys.exit(2)
    src_df = pd.read_csv(src_path)
    for col in ("team_home","team_away","match_key"):
        if col not in src_df.columns:
            print(f"[theoddsapi-safe] ERRO: coluna ausente em matches_source.csv: {col}")
            sys.exit(2)

    aliases = load_aliases("data/aliases_br.json")

    # coleta
    events_df = get_events(api_key, args.regions, debug=args.debug)
    if args.debug:
        print(f"[theoddsapi-safe][DEBUG] eventos coletados: {len(events_df)}")

    # normaliza & faz match_key para os eventos
    events_df["team_home_c"] = events_df["team_home"].map(lambda x: canonicalize_team(str(x), aliases))
    events_df["team_away_c"] = events_df["team_away"].map(lambda x: canonicalize_team(str(x), aliases))
    events_df["match_key"]   = [
        make_match_key(h, a, aliases=None)  # já canonizados acima
        for h, a in zip(events_df["team_home_c"], events_df["team_away_c"])
    ]

    # casa com source por match_key
    # nota: source já deve ter match_key (feito no seu CSV)
    # join
    merged = events_df.merge(
        src_df[["match_key","team_home","team_away"]],
        on="match_key",
        how="inner",
        suffixes=("_ev","")
    )

    # valida: precisa de ao menos 2 odds > 1.0
    def _ok(row):
        vals = [row["odds_home"], row["odds_draw"], row["odds_away"]]
        try:
            return sum(float(x) > 1.0 for x in vals if pd.notna(x)) >= 2
        except Exception:
            return False

    merged["__valid"] = merged.apply(_ok, axis=1)
    valid = merged[merged["__valid"]].copy()

    # saída
    out_cols = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]
    valid[out_cols].to_csv(out_dir/"odds_theoddsapi.csv", index=False)
    # não casados (no source)
    matched_keys = set(valid["match_key"])
    unmatched = src_df[~src_df["match_key"].isin(matched_keys)].copy()
    unmatched.to_csv(out_dir/"unmatched_theoddsapi.csv", index=False)

    print("9:Marcador requerido pelo workflow: \"theoddsapi-safe\"")
    print("[theoddsapi-safe] linhas -> " + json.dumps({
        "odds_theoddsapi.csv": int(len(valid)),
        "unmatched_theoddsapi.csv": int(len(unmatched))
    }))

if __name__ == "__main__":
    main()