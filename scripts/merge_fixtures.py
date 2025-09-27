# scripts/merge_fixtures.py
# Une fixtures de múltiplas fontes para robustez do agendamento de jogos.
from __future__ import annotations
import argparse, csv, sys, json, time
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
import numpy as np

def _read_csv_safe(p: Path, lower=True) -> pd.DataFrame:
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    df = pd.read_csv(p)
    if lower: df = df.rename(columns=str.lower)
    return df

def _norm_team(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.strip().lower()
    reps = {
        "ã":"a","á":"a","â":"a","ä":"a","à":"a",
        "é":"e","ê":"e","è":"e","ë":"e",
        "í":"i","ì":"i","ï":"i",
        "ó":"o","õ":"o","ô":"o","ö":"o","ò":"o",
        "ú":"u","ü":"u","ù":"u",
        "ç":"c","/":" ","-":" ","  ":" ","   ":" ",
        " futebol clube":"", " fc":"", " afc":"", " cf":"", " de futebol":""
    }
    for k,v in reps.items(): s = s.replace(k,v)
    return " ".join(s.split())

def _pick_date(row: pd.Series) -> Optional[str]:
    for c in ["date","kickoff","datetime_iso","utc_date"]:
        if c in row and isinstance(row[c], str) and row[c].strip():
            return row[c].strip()
    return None

def main():
    ap = argparse.ArgumentParser(description="Merge de fixtures: data/in/<rodada>/matches_source.csv (+ fontes opcionais) -> data/out/<rodada>/fixtures_merged.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--alt-path", default=None, help="CSV alternativo com fixtures (opcional). Ex.: data/out/<rodada>/odds_apifootball.csv ou fixtures_alt.csv")
    args = ap.parse_args()

    base_in  = Path(f"data/in/{args.rodada}")
    base_out = Path(f"data/out/{args.rodada}")
    base_out.mkdir(parents=True, exist_ok=True)

    src_path = base_in / "matches_source.csv"
    if not src_path.exists() or src_path.stat().st_size==0:
        raise RuntimeError(f"[fixtures] Arquivo-fonte ausente/vazio: {src_path} (crie com match_id,home,away[,date])")

    src = _read_csv_safe(src_path)
    src = src.rename(columns={"home_team":"home","away_team":"away"})
    need = {"match_id","home","away"}
    if not need.issubset(src.columns):
        raise RuntimeError("[fixtures] matches_source.csv inválido; precisa de colunas: match_id,home,away[,date]")

    # Base principal
    base = src[["match_id","home","away"]].copy()
    base["home_n"] = base["home"].map(_norm_team)
    base["away_n"] = base["away"].map(_norm_team)
    if "date" in src.columns:
        base["date_src"] = src["date"]
    else:
        base["date_src"] = None

    # Fonte alternativa (opcional)
    alt = pd.DataFrame()
    if args.alt_path:
        p = Path(args.alt_path)
        alt = _read_csv_safe(p)
    else:
        # tenta usar odds_apifootball como pista de data se existir
        p1 = base_out / "odds_apifootball.csv"
        alt = _read_csv_safe(p1)
    # padroniza possíveis nomes
    if not alt.empty:
        ren = {}
        if "home_team" in alt.columns: ren["home_team"]="home"
        if "away_team" in alt.columns: ren["away_team"]="away"
        if "team_home" in alt.columns: ren["team_home"]="home"
        if "team_away" in alt.columns: ren["team_away"]="away"
        alt = alt.rename(columns=ren)
        # normaliza
        have = [c for c in ["home","away","date","kickoff","datetime_iso","utc_date","fixture_id"] if c in alt.columns]
        alt = alt[have].copy()
        if "home" in alt.columns: alt["home_n"] = alt["home"].map(_norm_team)
        if "away" in alt.columns: alt["away_n"] = alt["away"].map(_norm_team)

    # Merge fuzzy simples por normalização exata (já resolve muitos casos)
    merged = base.merge(alt, on=["home_n","away_n"], how="left", suffixes=("","_alt"))
    # escolhe data
    merged["date_best"] = merged.apply(lambda r: _pick_date(r) or _pick_date(r.filter(like="_alt")) or r.get("date_src"), axis=1)

    out_cols = ["match_id","home","away","date_best"]
    if "fixture_id" in merged.columns: out_cols.append("fixture_id")
    out = merged[out_cols].rename(columns={"date_best":"date"})
    out.to_csv(base_out/"fixtures_merged.csv", index=False)
    print(f"[fixtures] OK -> {base_out/'fixtures_merged.csv'}")

    # Atualiza/gera matches.csv com data se não houver
    matches_path = base_out/"matches.csv"
    if matches_path.exists() and matches_path.stat().st_size>0:
        m = _read_csv_safe(matches_path)
        m = m.rename(columns=str.lower)
        m = m.drop(columns=[c for c in m.columns if c not in {"match_id","home","away","date"}])
        mm = m.merge(out[["match_id","date"]], on="match_id", how="left", suffixes=("","_new"))
        mm["date"] = mm["date"].fillna(mm["date_new"])
        mm = mm.drop(columns=[c for c in ["date_new"] if c in mm.columns])
    else:
        mm = out.copy()
    mm.to_csv(matches_path, index=False)
    print(f"[fixtures] matches.csv atualizado -> {matches_path}")

if __name__ == "__main__":
    main()
