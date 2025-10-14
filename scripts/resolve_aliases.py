# scripts/resolve_aliases.py
# resolve IDs/canônicos para matches_norm.csv usando catálogo + cache de aliases
from __future__ import annotations
import os
import sys
import json
import argparse
import pandas as pd
from typing import Dict, Any, Tuple, Optional
from _utils_norm import norm_name, token_key, best_match, load_json, dump_json

def load_catalog(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"catalog not found: {path}")
    df = pd.read_parquet(path)
    # chave de comparação
    df["canon"] = df["name"].fillna("").astype(str)
    df["canon_key"] = df["canon"].map(token_key)
    return df

def resolve_one(name: str, country: Optional[str], cat: pd.DataFrame) -> Tuple[Optional[int], float, Optional[str]]:
    if not name:
        return (None, 0.0, None)
    pool = cat
    # se informado país nos dados, filtra — ajuda muito BR
    if country and "country" in cat.columns:
        pool = pool[cat["country"].str.lower() == country.lower()]
        if pool.empty:
            pool = cat
    candidates = list(zip(pool["canon"], pool["team_id"]))
    return best_match(name, candidates, min_score=0.88)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True, help="matches_norm.csv")
    ap.add_argument("--catalog", default="data/ref/teams_catalog.parquet")
    ap.add_argument("--aliases_json", default="data/ref/aliases.json")
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--country", default="Brazil", help="país default p/ filtro de catálogo")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)

    df = pd.read_csv(args.in_csv)
    for col in ("home","away"):
        if col not in df.columns:
            print(f"::error::coluna ausente em source: {col}")
            sys.exit(1)

    cat = load_catalog(args.catalog)
    aliases = load_json(args.aliases_json)

    new_learn: Dict[str, Any] = {}
    home_id, away_id, home_canon, away_canon = [], [], [], []

    for _, row in df.iterrows():
        h, a = str(row["home"]), str(row["away"])
        # 1) tenta cache de aliases
        hid = aliases.get(norm_name(h), {}).get("team_id")
        aid = aliases.get(norm_name(a), {}).get("team_id")
        hc = None
        ac = None
        hs, as_ = 0.0, 0.0

        # 2) catálogo + fuzzy se cache não bater
        if not hid:
            hid, hs, hc = resolve_one(h, args.country, cat)
            if hid:
                key = norm_name(h)
                new_learn[key] = {"team_id": int(hid), "source": "resolver", "confidence": round(hs,3)}
        else:
            # encontra nome canônico no catálogo
            m = cat[cat["team_id"] == int(hid)]
            hc = m["canon"].iloc[0] if not m.empty else h

        if not aid:
            aid, as_, ac = resolve_one(a, args.country, cat)
            if aid:
                key = norm_name(a)
                new_learn[key] = {"team_id": int(aid), "source": "resolver", "confidence": round(as_,3)}
        else:
            m = cat[cat["team_id"] == int(aid)]
            ac = m["canon"].iloc[0] if not m.empty else a

        home_id.append(hid if hid else None)
        away_id.append(aid if aid else None)
        home_canon.append(hc if hc else h)
        away_canon.append(ac if ac else a)

    df["home_team_id"] = home_id
    df["away_team_id"] = away_id
    df["home_canon"] = home_canon
    df["away_canon"] = away_canon

    df.to_csv(args.out_csv, index=False)
    print(f"[resolve] OK — salvo {args.out_csv} com IDs/canônicos")

    # atualiza cache
    if new_learn:
        aliases.update(new_learn)
        dump_json(args.aliases_json, aliases)
        # também salva CSV de auditoria
        pd.DataFrame([
            {"observed": k, **v} for k, v in new_learn.items()
        ]).to_csv(os.path.join(os.path.dirname(args.out_csv), "aliases_new.csv"), index=False)
        print(f"[resolve] {len(new_learn)} aliases aprendidos → {args.aliases_json}")

if __name__ == "__main__":
    main()