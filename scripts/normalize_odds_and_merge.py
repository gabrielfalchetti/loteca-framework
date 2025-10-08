#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Normalize team names (robust to unicode spaces/accents) and rebuild odds consensus for Loteca.

Usage (exemplo):
  python scripts/normalize_odds_and_merge.py \
    --theodds data/out/odds_theoddsapi.csv \
    --apifoot data/out/odds_apifootball.csv \
    --out data/out/odds_consensus.csv

Saída:
  - odds_consensus.csv com colunas:
      match_id, team_home, team_away, odds_home, odds_draw, odds_away, source

Notas:
  - Inclui logs para verificar casos específicos do teste:
      atletico-mg vs sport
      america-mg vs vila nova
"""

from __future__ import annotations
import argparse
import pandas as pd
import unicodedata as ud
import re
from pathlib import Path

# ----------------------------- Alias Map ------------------------------------
# Ajuste/expanda conforme necessidade (times BR e variações comuns)
ALIAS_MAP = {
    # Gerais
    "atletico mineiro": "atletico-mg",
    "atletico-mineiro": "atletico-mg",
    "atletico mg": "atletico-mg",
    "américa mineiro": "america-mg",
    "america mineiro": "america-mg",
    "america-mineiro": "america-mg",
    "america mg": "america-mg",
    "operario pr": "operario-pr",
    "operario-pr": "operario-pr",
    "operario": "operario-pr",
    "gremio novorizontino": "novorizontino",
    "grêmio novorizontino": "novorizontino",
    "novorizontino": "novorizontino",
    "cuiaba": "cuiaba",
    "cuiabá": "cuiaba",
    "avai": "avai",
    "avaí": "avai",
    "mirassol": "mirassol",
    "fluminense": "fluminense",
    "volta redonda": "volta redonda",
    "vila nova": "vila nova",
    "sport recife": "sport",
    "sport": "sport",
    "athletic club (mg)": "athletic club",
    "athletic club": "athletic club",
    # Outras variações que às vezes aparecem
    "atletico-mg": "atletico-mg",
    "america-mg": "america-mg",
}

# ---------------------------- Normalização ----------------------------------
def strip_accents(s: str) -> str:
    if pd.isna(s):
        return s
    return "".join(ch for ch in ud.normalize("NFKD", str(s)) if not ud.combining(ch))

# Conjunto de espaços unicode comuns que podem quebrar o matching
_UNICODE_SPACES = r"[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000]"

def canon(s: str) -> str:
    """
    Normaliza nomes: remove acentos, troca espaços unicode por espaço simples,
    mantém apenas [a-z0-9 -()], colapsa espaços, aplica aliases.
    """
    if pd.isna(s):
        return s
    s0 = strip_accents(s)
    s0 = re.sub(_UNICODE_SPACES, " ", s0)  # espaços invisíveis -> espaço normal
    s0 = s0.lower()
    s0 = re.sub(r"[^a-z0-9\s\-()]", " ", s0)  # mantém letras, números, espaço, hífen e parênteses
    s0 = re.sub(r"\s+", " ", s0).strip()
    # aplica alias exato
    if s0 in ALIAS_MAP:
        return ALIAS_MAP[s0]
    return s0

# --------------------------- Builders por fonte ------------------------------
def build_from_theodds(path: Path) -> pd.DataFrame:
    """
    Lê CSV do TheOddsAPI e padroniza:
      Esperado: cols >= [home, away, odds_home, odds_draw, odds_away, sport]
    Retorna DataFrame com:
      [team_home, team_away, odds_home, odds_draw, odds_away, source]
    """
    if not path or not path.exists():
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])

    # Normaliza nomes
    df["team_home"] = df["home"].apply(canon)
    df["team_away"] = df["away"].apply(canon)

    # Filtra futebol (defensivo)
    if "sport" in df.columns:
        df = df[df["sport"].astype(str).str.contains("soccer", case=False, na=False)]

    # Garante numéricos
    for c in ("odds_home","odds_draw","odds_away"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Agrega por confronto (média simples)
    agg = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    agg["source"] = "theoddsapi"
    return agg

def build_from_apifoot(path: Path) -> pd.DataFrame:
    """
    Lê CSV do APIFootball e padroniza:
      Esperado: cols >= [team_home, team_away, odds_home, odds_draw, odds_away]
    Retorna DataFrame com:
      [team_home, team_away, odds_home, odds_draw, odds_away, source]
    """
    if not path or not path.exists():
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])

    # Normaliza nomes
    df["team_home"] = df["team_home"].apply(canon)
    df["team_away"] = df["team_away"].apply(canon)

    # Garante numéricos
    for c in ("odds_home","odds_draw","odds_away"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Agrega por confronto (média simples)
    agg = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    agg["source"] = "apifootball"
    return agg

# ----------------------------- Consenso -------------------------------------
def merge_consensus(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Une fontes e gera consenso (média das odds) por (team_home, team_away).
    Cria match_id no formato "Home__Away" (Title Case).
    """
    if not df_list:
        return pd.DataFrame(columns=["match_id","team_home","team_away","odds_home","odds_draw","odds_away","source"])

    df = pd.concat([d for d in df_list if d is not None], ignore_index=True)
    if df.empty:
        return pd.DataFrame(columns=["match_id","team_home","team_away","odds_home","odds_draw","odds_away","source"])

    base = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    # Monta match_id (Title Case com espaços colapsados)
    base["match_id"] = (
        base["team_home"].str.replace(r"\s+", " ", regex=True).str.strip().str.title()
        + "__" +
        base["team_away"].str.replace(r"\s+", " ", regex=True).str.strip().str.title()
    )
    base["source"] = "consensus"
    return base[["match_id","team_home","team_away","odds_home","odds_draw","odds_away","source"]]

# ------------------------------- Main ---------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Normalize team names and rebuild odds consensus (Loteca fix)")
    ap.add_argument("--theodds", type=Path, required=True, help="CSV do TheOddsAPI")
    ap.add_argument("--apifoot", type=Path, required=False, default=None, help="CSV do APIFootball (opcional)")
    ap.add_argument("--out", type=Path, required=True, help="Arquivo CSV de saída do consenso")
    args = ap.parse_args()

    df1 = build_from_theodds(args.theodds)
    df2 = build_from_apifoot(args.apifoot) if args.apifoot else pd.DataFrame(columns=df1.columns)

    out = merge_consensus([df1, df2])
    out.to_csv(args.out, index=False)

    # Logs úteis (casos citados no teste)
    targets = [("atletico-mg","sport"), ("america-mg","vila nova")]
    for h,a in targets:
        hit = out[(out.team_home==h)&(out.team_away==a)]
        if hit.empty:
            print(f"[WARN] Sem odds no consenso para: {h} vs {a} (verifique fontes e nomes)")
        else:
            print(f"[OK] Consenso encontrado para: {h} vs {a}")

if __name__ == "__main__":
    main()
