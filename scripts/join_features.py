# scripts/join_features.py
from __future__ import annotations
import argparse, os
from pathlib import Path
import pandas as pd
from pandas.errors import EmptyDataError

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()

def padroniza(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.rename(columns={c: c.lower() for c in df.columns})
    maps = {
        "mandante":"home","visitante":"away",
        "time_casa":"home","time_fora":"away",
        "casa":"home","fora":"away",
        "home_team":"home","away_team":"away",
        "data_jogo":"date","data":"date","matchdate":"date",
        "id":"match_id"
    }
    df = df.rename(columns={k:v for k,v in maps.items() if k in df.columns})
    for col in ("home","away"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.replace(r"\s+"," ",regex=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "match_id" not in df.columns and len(df)>0:
        df = df.reset_index(drop=True)
        df["match_id"] = df.index + 1
    return df

def choose_keys(a: pd.DataFrame, b: pd.DataFrame):
    if "match_id" in a.columns and "match_id" in b.columns:
        return ["match_id"]
    for cand in (("home","away","date"), ("home","away")):
        if all(c in a.columns for c in cand) and all(c in b.columns for c in cand):
            return list(cand)
    return []

def main():
    ap = argparse.ArgumentParser(description="Join matches+odds+features -> joined.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--matches", default=None)
    ap.add_argument("--odds", default=None)
    ap.add_argument("--features", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--soft", action="store_true")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)
    matches_path  = Path(args.matches)  if args.matches  else base / "matches.csv"
    odds_path     = Path(args.odds)     if args.odds     else base / "odds.csv"
    features_path = Path(args.features) if args.features else base / "features.csv"
    out_path      = Path(args.out)      if args.out      else base / "joined.csv"

    m = padroniza(safe_read_csv(matches_path))
    o = padroniza(safe_read_csv(odds_path))
    f = padroniza(safe_read_csv(features_path))

    if m.empty:
        msg = f"[join_features] matches vazio/ausente: {matches_path}"
        if args.soft or os.getenv("JOIN_SOFT","0") == "1":
            print(msg + " (soft-mode: criando joined vazio)")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame().to_csv(out_path, index=False)
            return
        raise RuntimeError(msg)

    df = m.copy()

    if not o.empty:
        k = choose_keys(df, o)
        if k:
            df = df.merge(o, on=k, how="left", suffixes=("", "_odds"))
        else:
            common = [c for c in ("home","away") if c in df.columns and c in o.columns]
            if common:
                df = df.merge(o, on=common, how="left", suffixes=("", "_odds"))

    if not f.empty:
        k = choose_keys(df, f)
        if k:
            df = df.merge(f, on=k, how="left", suffixes=("", "_feat"))
        else:
            common = [c for c in ("match_id","home","away") if c in df.columns and c in f.columns]
            if common:
                df = df.merge(f, on=common, how="left", suffixes=("", "_feat"))
            else:
                raise RuntimeError("Sem chaves para juntar features (precisa 'match_id' ou 'home/away[/date]').")

    if df.empty:
        raise RuntimeError("joined vazio; verifique entradas.")
    df.to_csv(out_path, index=False)
    print(f"[join_features] OK: {len(df)} -> {out_path}")

if __name__ == "__main__":
    main()
