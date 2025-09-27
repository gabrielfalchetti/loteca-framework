# scripts/merge_fixtures.py
# Une fixtures de múltiplas fontes para robustez do agendamento de jogos.
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional
import pandas as pd
import numpy as np

def _read_csv_safe(p: Path, lower=True) -> pd.DataFrame:
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    df = pd.read_csv(p)
    if lower:
        df = df.rename(columns=str.lower)
    return df

def _norm_team(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    reps = {
        "ã": "a", "á": "a", "â": "a", "ä": "a", "à": "a",
        "é": "e", "ê": "e", "è": "e", "ë": "e",
        "í": "i", "ì": "i", "ï": "i",
        "ó": "o", "õ": "o", "ô": "o", "ö": "o", "ò": "o",
        "ú": "u", "ü": "u", "ù": "u",
        "ç": "c", "/": " ", "-": " ",
    }
    for k, v in reps.items():
        s = s.replace(k, v)
    s = s.replace("  ", " ").replace("   ", " ")
    # sufixos/complementos comuns
    for tail in (" futebol clube", " de futebol", " fc", " afc", " cf"):
        if s.endswith(tail):
            s = s[: -len(tail)]
    return " ".join(s.split())

def _pick_date(row: pd.Series) -> Optional[str]:
    # tenta nas colunas "normais"
    for c in ("date", "kickoff", "datetime_iso", "utc_date", "match_date"):
        if c in row and isinstance(row[c], str) and row[c].strip():
            return row[c].strip()
    return None

def _prepare_alt(alt: pd.DataFrame) -> pd.DataFrame:
    """Padroniza um possível CSV alternativo para ter ao menos home_n/away_n + alguma coluna de data."""
    if alt.empty:
        return pd.DataFrame({"home_n": [], "away_n": []})
    # Renomeia variantes de colunas de times
    ren = {}
    if "home_team" in alt.columns: ren["home_team"] = "home"
    if "away_team" in alt.columns: ren["away_team"] = "away"
    if "team_home" in alt.columns: ren["team_home"] = "home"
    if "team_away" in alt.columns: ren["team_away"] = "away"
    alt = alt.rename(columns=ren)

    # Se não houver colunas de time, devolve DF mínimo para não quebrar
    if not {"home", "away"}.issubset(alt.columns):
        return pd.DataFrame({"home_n": [], "away_n": []})

    # Mantém somente colunas potencialmente úteis
    keep = [c for c in ("home", "away", "date", "kickoff", "datetime_iso", "utc_date", "fixture_id", "match_date") if c in alt.columns]
    alt = alt[keep].copy()

    # Normaliza times
    alt["home_n"] = alt["home"].map(_norm_team)
    alt["away_n"] = alt["away"].map(_norm_team)

    # Deduplifica por par (home_n, away_n) mantendo a primeira linha (qualquer)
    alt = alt.sort_index().drop_duplicates(subset=["home_n", "away_n"], keep="first")
    return alt

def main():
    ap = argparse.ArgumentParser(description="Merge de fixtures: data/in/<rodada>/matches_source.csv (+ fonte opcional) -> data/out/<rodada>/fixtures_merged.csv")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--alt-path", default=None, help="CSV alternativo com fixtures (opcional). Ex.: data/out/<rodada>/odds_apifootball.csv ou fixtures_alt.csv")
    args = ap.parse_args()

    base_in = Path(f"data/in/{args.rodada}")
    base_out = Path(f"data/out/{args.rodada}")
    base_out.mkdir(parents=True, exist_ok=True)

    src_path = base_in / "matches_source.csv"
    if not src_path.exists() or src_path.stat().st_size == 0:
        raise RuntimeError(f"[fixtures] Arquivo-fonte ausente/vazio: {src_path} (crie com match_id,home,away[,date])")

    src = _read_csv_safe(src_path)
    # aceita variantes
    src = src.rename(columns={"home_team": "home", "away_team": "away"})
    need = {"match_id", "home", "away"}
    if not need.issubset(src.columns):
        raise RuntimeError("[fixtures] matches_source.csv inválido; precisa de colunas: match_id,home,away[,date]")

    # Base principal
    base = src[["match_id", "home", "away"]].copy()
    base["home_n"] = base["home"].map(_norm_team)
    base["away_n"] = base["away"].map(_norm_team)
    base["date_src"] = src["date"] if "date" in src.columns else None

    # Fonte alternativa (opcional)
    if args.alt_path:
        alt_raw = _read_csv_safe(Path(args.alt_path))
    else:
        # tenta odds_apifootball como pista (se existir)
        alt_raw = _read_csv_safe(base_out / "odds_apifootball.csv")

    alt = _prepare_alt(alt_raw)

    # Se alt vazio ou sem colunas, seguimos sem quebrar
    if alt.empty or not {"home_n", "away_n"}.issubset(alt.columns):
        # Sem dados externos — apenas promove date_src
        out = base[["match_id", "home", "away", "date_src"]].rename(columns={"date_src": "date"})
        # grava fixtures_merged.csv e matches.csv
        out.to_csv(base_out / "fixtures_merged.csv", index=False)
        print(f"[fixtures] OK (sem fonte alt) -> {base_out/'fixtures_merged.csv'}")
        matches_path = base_out / "matches.csv"
        # Atualiza/gera matches.csv com date
        if matches_path.exists() and matches_path.stat().st_size > 0:
            m = _read_csv_safe(matches_path)
            m = m.rename(columns=str.lower)
            m = m[["match_id", "home", "away"] + ([c for c in ["date"] if c in m.columns])]
            mm = m.merge(out[["match_id", "date"]], on="match_id", how="left", suffixes=("", "_new"))
            if "date" not in mm.columns:
                mm["date"] = mm["date_new"]
            else:
                mm["date"] = mm["date"].fillna(mm["date_new"])
            if "date_new" in mm.columns:
                mm = mm.drop(columns=["date_new"])
        else:
            mm = out.copy()
        mm.to_csv(matches_path, index=False)
        print(f"[fixtures] matches.csv atualizado -> {matches_path}")
        return

    # Merge quando ALT existe e está preparado
    merged = base.merge(alt, on=["home_n", "away_n"], how="left", suffixes=("", "_alt"))

    # Escolha da melhor data
    def _best_date(row: pd.Series) -> Optional[str]:
        # data vinda da base (source)
        d_src = _pick_date(row)
        if d_src:
            return d_src
        # tenta colunas com sufixo _alt (fonte alternativa)
        cols_alt = [c for c in merged.columns if c.endswith("_alt") or c in ("date", "kickoff", "datetime_iso", "utc_date", "match_date")]
        return _pick_date(row[cols_alt]) if cols_alt else None

    merged["date_best"] = merged.apply(_best_date, axis=1)

    # Saída
    out_cols = ["match_id", "home", "away", "date_best"]
    if "fixture_id" in merged.columns:
        out_cols.append("fixture_id")
    out = merged[out_cols].rename(columns={"date_best": "date"})
    out.to_csv(base_out / "fixtures_merged.csv", index=False)
    print(f"[fixtures] OK -> {base_out/'fixtures_merged.csv'}")

    # Atualiza/gera matches.csv com a melhor data
    matches_path = base_out / "matches.csv"
    if matches_path.exists() and matches_path.stat().st_size > 0:
        m = _read_csv_safe(matches_path).rename(columns=str.lower)
        m = m.drop(columns=[c for c in m.columns if c not in {"match_id", "home", "away", "date"}])
        mm = m.merge(out[["match_id", "date"]], on="match_id", how="left", suffixes=("", "_new"))
        mm["date"] = mm["date"].fillna(mm["date_new"])
        if "date_new" in mm.columns:
            mm = mm.drop(columns=["date_new"])
    else:
        mm = out.copy()
    mm.to_csv(matches_path, index=False)
    print(f"[fixtures] matches.csv atualizado -> {matches_path}")

if __name__ == "__main__":
    main()
