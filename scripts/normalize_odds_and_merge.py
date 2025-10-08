from __future__ import annotations
import argparse
import pandas as pd
import unicodedata as ud
import re
from pathlib import Path

# Alias/normalização para clubes brasileiros (ajuste/expanda à vontade)
ALIAS_MAP = {
    # Brasileirão / Copas BR
    "atletico mineiro": "atletico-mg",
    "atletico-mineiro": "atletico-mg",
    "américa mineiro": "america-mg",
    "america mineiro": "america-mg",
    "america-mineiro": "america-mg",
    "operario pr": "operario-pr",
    "operario-pr": "operario-pr",
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
}

def strip_accents(s: str) -> str:
    if pd.isna(s):
        return s
    return "".join(ch for ch in ud.normalize("NFKD", s) if not ud.combining(ch))

def canon(s: str) -> str:
    """Normaliza nomes: remove acentos, minúsculas, poda símbolos e aplica aliases."""
    if pd.isna(s):
        return s
    s0 = strip_accents(str(s)).lower()
    s0 = re.sub(r"[^a-z0-9\s\-()]", " ", s0)   # mantém letras, números, espaço, hífen e parênteses
    s0 = re.sub(r"\s+", " ", s0).strip()
    if s0 in ALIAS_MAP:
        return ALIAS_MAP[s0]
    return s0

def build_from_theodds(path: Path) -> pd.DataFrame:
    """Lê odds do TheOddsAPI em CSV e padroniza (home/away/odds_*)."""
    if not path.exists():
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])
    # Espera colunas: home, away, odds_home, odds_draw, odds_away, sport (entre outras)
    df["team_home"] = df["home"].apply(canon)
    df["team_away"] = df["away"].apply(canon)
    if "sport" in df.columns:
        df = df[df["sport"].astype(str).str.contains("soccer", case=False, na=False)]
    # média por confronto (se houver regiões/linhas repetidas)
    agg = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    agg["source"] = "theoddsapi"
    return agg

def build_from_apifoot(path: Path) -> pd.DataFrame:
    """Lê odds do APIFootball em CSV e padroniza (team_home/team_away/odds_*)."""
    if not path.exists():
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","source"])
    # Espera colunas: team_home, team_away, odds_home, odds_draw, odds_away
    df["team_home"] = df["team_home"].apply(canon)
    df["team_away"] = df["team_away"].apply(canon)
    agg = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    agg["source"] = "apifootball"
    return agg

def merge_consensus(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    """Une fontes e gera consenso por (home, away) como média das odds."""
    df = pd.concat(df_list, ignore_index=True)
    if df.empty:
        return pd.DataFrame(columns=["match_id","team_home","team_away","odds_home","odds_draw","odds_away","source"])
    base = df.groupby(["team_home","team_away"], as_index=False)[["odds_home","odds_draw","odds_away"]].mean(numeric_only=True)
    base["match_id"] = (
        base["team_home"].str.replace(r"\s+", " ", regex=True).str.strip().str.title()
        + "__" +
        base["team_away"].str.replace(r"\s+", " ", regex=True).str.strip().str.title()
    )
    base["source"] = "consensus"
    base = base[["match_id","team_home","team_away","odds_home","odds_draw","odds_away","source"]]
    return base

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

    # Logs úteis para os casos citados no teste
    targets = [("atletico-mg","sport"), ("america-mg","vila nova")]
    for h,a in targets:
        hit = out[(out.team_home==h)&(out.team_away==a)]
        if hit.empty:
            print(f"[WARN] Sem odds no consenso para: {h} vs {a} (verifique fontes e nomes)")
        else:
            print(f"[OK] Consenso encontrado para: {h} vs {a}")

if __name__ == "__main__":
    main()
