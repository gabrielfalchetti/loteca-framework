# scripts/join_features.py
# Versão simples e tolerante a CSV vazio

import argparse
from pathlib import Path
import pandas as pd
from pandas.errors import EmptyDataError

def safe_read_csv(path: str, **kwargs) -> pd.DataFrame:
    """
    Lê CSV com segurança:
    - Se não existir ou estiver vazio, retorna DataFrame() vazio.
    - Se der EmptyDataError, retorna DataFrame() vazio.
    """
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, **kwargs)
    except EmptyDataError:
        return pd.DataFrame()

def padroniza_colunas(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # tudo minúsculo
    df = df.rename(columns={c: c.lower() for c in df.columns})
    # mapeamentos comuns para facilitar merge
    mapa = {
        "mandante": "home",
        "visitante": "away",
        "time_casa": "home",
        "time_fora": "away",
        "casa": "home",
        "fora": "away",
        "home_team": "home",
        "away_team": "away",
        "data_jogo": "date",
        "data": "date",
        "matchdate": "date",
    }
    inter = {k: v for k, v in mapa.items() if k in df.columns}
    if inter:
        df = df.rename(columns=inter)

    # normaliza strings
    for col in ("home", "away"):
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
            )

    # tenta converter data
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df

def escolhe_chaves(a: pd.DataFrame, b: pd.DataFrame):
    # prioridade por match_id; depois (home, away, date) e (home, away)
    if "match_id" in a.columns and "match_id" in b.columns:
        return ["match_id"]
    cand = [("home", "away", "date"), ("home", "away")]
    for cols in cand:
        if all(c in a.columns for c in cols) and all(c in b.columns for c in cols):
            return list(cols)
    return []  # sem chave clara

def main():
    ap = argparse.ArgumentParser(description="Junta matches + odds + features de forma tolerante.")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-09-20_21")
    ap.add_argument("--matches", default=None)
    ap.add_argument("--odds", default=None)
    ap.add_argument("--features", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    base = f"data/out/{args.rodada}"
    matches_path  = args.matches  or f"{base}/matches.csv"
    odds_path     = args.odds     or f"{base}/odds.csv"
    features_path = args.features or f"{base}/features.csv"
    out_path      = args.out      or f"{base}/joined.csv"

    # leituras seguras (não quebram se arquivo estiver vazio)
    matches  = safe_read_csv(matches_path)
    odds     = safe_read_csv(odds_path)
    features = safe_read_csv(features_path)

    # padroniza nomes
    matches  = padroniza_colunas(matches)
    odds     = padroniza_colunas(odds)
    features = padroniza_colunas(features)

    # regra mínima: precisamos de matches com algo
    if matches.empty:
        raise RuntimeError(f"[join_features] matches vazio/ausente: {matches_path}")

    df = matches.copy()

    # junta ODDS (se houver)
    if not odds.empty:
        keys = escolhe_chaves(df, odds)
        if keys:
            df = df.merge(odds, on=keys, how="left", suffixes=("", "_odds"))
        else:
            # fallback simples: tenta por home/away
            comuns = [c for c in ("home", "away") if c in df.columns and c in odds.columns]
            if comuns:
                df = df.merge(odds, on=comuns, how="left", suffixes=("", "_odds"))
            # se não tiver como, segue sem odds

    # junta FEATURES (se houver)
    if not features.empty:
        keys = escolhe_chaves(df, features)
        if keys:
            df = df.merge(features, on=keys, how="left", suffixes=("", "_feat"))
        else:
            comuns = [c for c in ("match_id", "home", "away") if c in df.columns and c in features.columns]
            if comuns:
                df = df.merge(features, on=comuns, how="left", suffixes=("", "_feat"))
            else:
                # sem chave clara — melhor avisar cedo
                raise RuntimeError(
                    "[join_features] Não achei chaves para juntar features. "
                    "Garanta pelo menos match_id ou (home/away[/date])."
                )

    # salva resultado
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    if df is None or df.empty:
        raise RuntimeError(f"[join_features] Resultado final vazio — não vou salvar: {out_path}")
    df.to_csv(out_path, index=False)

    print(f"[join_features] OK: {len(df)} linhas -> {out_path}")
    print(f"[join_features] Origens: matches={len(matches)} | odds={len(odds)} | features={len(features)}")

if __name__ == "__main__":
    main()
