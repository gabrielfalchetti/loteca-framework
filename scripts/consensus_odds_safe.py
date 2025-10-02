#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/consensus_odds_safe.py

Gera odds de consenso a partir dos provedores disponíveis no diretório da rodada.
Falha COM CÓDIGO 10 se nenhuma linha válida (>= 2 odds > 1.0) for encontrada — isto
impede etapas posteriores (ex.: publish_kelly) de rodarem sem odds reais.

Regras/Notas:
- Lê os arquivos de entrada (se existirem):
  - data/out/<RODADA>/odds_theoddsapi.csv
  - data/out/<RODADA>/odds_apifootball.csv
- Base de jogos: tenta casar por "match_key"; se ausente, casa por (team_home, team_away)
  normalizados.
- Normalização de colunas: aceita várias variantes e mapeia para
  [team_home, team_away, match_key, odds_home, odds_draw, odds_away].
- Coerção de odds:
  - Converte strings para float, troca vírgula por ponto, remove espaços/sinais estranhos.
  - Rejeita odds <= 1.0 (não são odds decimais válidas).
- Consenso:
  - Por jogo, computa a média simples das odds válidas entre provedores.
  - Mantém também o número de provedores válidos por mercado (prov_count_*).
- Saída:
  - data/out/<RODADA>/odds_consensus.csv
  - Se não houver NENHUMA linha com pelo menos 2 mercados válidos (>1.0), aborta (exit 10).

Uso:
  python -m scripts.consensus_odds_safe --rodada 2025-09-27_1213 [--debug]
"""

import argparse
import os
import sys
import math
import json
from typing import Dict, List, Optional, Tuple

import pandas as pd

# -----------------------------
# Helpers
# -----------------------------

def debug_print(enabled: bool, *args, **kwargs):
    if enabled:
        print(*args, **kwargs, flush=True)

def _clean_float(x) -> Optional[float]:
    """Converte odds em float decimal válido (> 1.0). Retorna None se inválido."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            fx = float(x)
            return fx if fx > 1.0 and math.isfinite(fx) else None
        except Exception:
            return None
    # strings
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    # troca vírgula por ponto e remove caracteres fora do padrão
    s = s.replace(",", ".")
    # remove qualquer coisa que não seja parte de um float simples
    # (mantém dígitos, ponto e eventualmente notação científica)
    try:
        fx = float(s)
        if fx > 1.0 and math.isfinite(fx):
            return fx
        return None
    except Exception:
        return None

def _norm_team(s: str) -> str:
    if s is None:
        return ""
    # normaliza de forma leve; não depende de libs externas
    return (
        str(s)
        .strip()
        .lower()
        .replace(" fc", "")
        .replace(" - ", " ")
        .replace("-", " ")
        .replace("  ", " ")
    )

def build_key(team_home: str, team_away: str) -> str:
    th = _norm_team(team_home)
    ta = _norm_team(team_away)
    return f"{th}__vs__{ta}"

def pick_first_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def normalize_provider_frame(df: pd.DataFrame, debug=False, provider_name="prov") -> pd.DataFrame:
    """Mapeia colunas para um layout padrão e retorna apenas as colunas de interesse."""
    # Primeiro normaliza header (lowercase)
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    map_candidates: Dict[str, List[str]] = {
        "team_home": ["team_home", "home_team", "home", "mandante", "time_mandante"],
        "team_away": ["team_away", "away_team", "away", "visitante", "time_visitante"],
        "match_key": ["match_key", "match_id", "game_id", "partida_id"],
        "odds_home": ["odds_home", "home_odds", "h2h_home", "odd_home", "homeprice"],
        "odds_draw": ["odds_draw", "draw_odds", "h2h_draw", "odd_draw", "drawprice", "x_odds", "empate"],
        "odds_away": ["odds_away", "away_odds", "h2h_away", "odd_away", "awayprice"],
    }

    detected = {}
    for target, cands in map_candidates.items():
        col = pick_first_column(df, cands)
        detected[target] = col

    # Se não houver teams mas houver 'teams' agrupado, tenta expandir
    if detected["team_home"] is None and detected["team_away"] is None:
        # tenta 'home_name' / 'away_name'
        for th_cand in ["home_name", "casa", "time_casa"]:
            if th_cand in df.columns:
                detected["team_home"] = th_cand
                break
        for ta_cand in ["away_name", "fora", "time_fora"]:
            if ta_cand in df.columns:
                detected["team_away"] = ta_cand
                break

    # Constrói DataFrame de saída
    out = pd.DataFrame()
    for tgt in ["team_home", "team_away", "match_key"]:
        src = detected.get(tgt)
        if src is not None and src in df.columns:
            out[tgt] = df[src]
        else:
            out[tgt] = None

    # Se não houver match_key, cria a partir dos times
    if out["match_key"].isna().all():
        out["__auto_key"] = out.apply(lambda r: build_key(r["team_home"], r["team_away"]), axis=1)
        out["match_key"] = out["__auto_key"]
        out.drop(columns=["__auto_key"], inplace=True)

    # Odds
    for tgt in ["odds_home", "odds_draw", "odds_away"]:
        src = detected.get(tgt)
        if src is not None and src in df.columns:
            out[tgt] = df[src].apply(_clean_float)
        else:
            out[tgt] = None

    # Remove linhas completamente sem odds
    out["valid_count"] = (
        out[["odds_home", "odds_draw", "odds_away"]]
        .apply(lambda r: sum(1 for v in r if (v is not None and v > 1.0)), axis=1)
    )
    out["provider"] = provider_name
    out = out[(out["odds_home"].notna()) | (out["odds_draw"].notna()) | (out["odds_away"].notna())].copy()

    debug_print(debug, f"[consensus-safe] {provider_name} normalizado: {len(out)} linhas (com alguma odd)")
    return out[["match_key", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "provider"]]

def read_provider_csv(path: str, provider_name: str, debug=False) -> pd.DataFrame:
    if not os.path.exists(path):
        debug_print(debug, f"[consensus-safe] AVISO: arquivo não encontrado: {path}")
        return pd.DataFrame(columns=["match_key","team_home","team_away","odds_home","odds_draw","odds_away","provider"])
    try:
        df = pd.read_csv(path)
    except Exception as e:
        debug_print(debug, f"[consensus-safe] AVISO: falha ao ler {path}: {e}")
        return pd.DataFrame(columns=["match_key","team_home","team_away","odds_home","odds_draw","odds_away","provider"])
    return normalize_provider_frame(df, debug=debug, provider_name=provider_name)

def load_matches_source(out_dir: str, debug=False) -> pd.DataFrame:
    # Base de jogos (opcional): útil para reforçar join/cobertura
    # data/in/<RODADA>/matches_source.csv
    rodada = os.path.basename(out_dir.rstrip("/"))
    ms_path = os.path.join("data", "in", rodada, "matches_source.csv")
    if not os.path.exists(ms_path):
        debug_print(debug, f"[consensus-safe] AVISO: matches_source ausente: {ms_path}")
        # retorna vazio — não é obrigatório
        return pd.DataFrame(columns=["match_key","team_home","team_away"])
    try:
        df = pd.read_csv(ms_path)
    except Exception as e:
        debug_print(debug, f"[consensus-safe] AVISO: falha ao ler matches_source: {e}")
        return pd.DataFrame(columns=["match_key","team_home","team_away"])

    # normaliza cabeçalhos básicos
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    # garante colunas
    for c in ["team_home","team_away","match_key"]:
        if c not in df.columns:
            df[c] = None

    if df["match_key"].isna().all():
        df["match_key"] = df.apply(lambda r: build_key(r["team_home"], r["team_away"]), axis=1)
    return df[["match_key","team_home","team_away"]]

def aggregate_consensus(per_provider: List[pd.DataFrame], base_matches: pd.DataFrame, debug=False) -> pd.DataFrame:
    if not per_provider:
        return pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away","prov_count_home","prov_count_draw","prov_count_away"])

    all_odds = pd.concat(per_provider, ignore_index=True) if len(per_provider) > 1 else per_provider[0].copy()
    if all_odds.empty:
        return pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away","prov_count_home","prov_count_draw","prov_count_away"])

    # Calcula consensos por match_key
    grp = all_odds.groupby("match_key", dropna=False)

    def safe_mean(series):
        vals = [v for v in series if (v is not None and not (isinstance(v, float) and (math.isnan(v) or v <= 1.0)) and v > 1.0)]
        if not vals:
            return None
        return float(sum(vals) / len(vals))

    def safe_count(series):
        return int(sum(1 for v in series if (v is not None and v > 1.0)))

    rows = []
    for mk, g in grp:
        th = g["team_home"].dropna()
        ta = g["team_away"].dropna()
        team_home = th.iloc[0] if not th.empty else None
        team_away = ta.iloc[0] if not ta.empty else None

        oh = safe_mean(g["odds_home"])
        od = safe_mean(g["odds_draw"])
        oa = safe_mean(g["odds_away"])

        ch = safe_count(g["odds_home"])
        cd = safe_count(g["odds_draw"])
        ca = safe_count(g["odds_away"])

        rows.append({
            "match_key": mk,
            "team_home": team_home,
            "team_away": team_away,
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
            "prov_count_home": ch,
            "prov_count_draw": cd,
            "prov_count_away": ca,
        })

    out = pd.DataFrame(rows)

    # Se base de jogos existe, faz um left-join para garantir times/ordem
    if not base_matches.empty:
        out = base_matches.merge(out, on="match_key", how="left", suffixes=("", "_calc"))
        # Preenche times a partir da base quando faltarem
        out["team_home"] = out["team_home"].fillna(out.get("team_home_calc"))
        out["team_away"] = out["team_away"].fillna(out.get("team_away_calc"))
        for c in ["team_home_calc","team_away_calc"]:
            if c in out.columns:
                out.drop(columns=[c], inplace=True)

    # Ordena pela presença de odds válidas (desc) e por match_key
    out["valid_markets"] = out[["odds_home", "odds_draw", "odds_away"]].apply(
        lambda r: sum(1 for v in r if (v is not None and v > 1.0)), axis=1
    )
    out.sort_values(by=["valid_markets", "match_key"], ascending=[False, True], inplace=True)

    # Filtra odds inválidas para NaN/None
    for c in ["odds_home", "odds_draw", "odds_away"]:
        out[c] = out[c].apply(lambda v: v if (v is not None and v > 1.0) else None)

    return out[[
        "team_home","team_away","match_key",
        "odds_home","odds_draw","odds_away",
        "prov_count_home","prov_count_draw","prov_count_away",
        "valid_markets"
    ]]

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Identificador da rodada (ex.: 2025-09-27_1213)")
    ap.add_argument("--debug", action="store_true", default=False)
    args = ap.parse_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    paths = {
        "theodds": os.path.join(out_dir, "odds_theoddsapi.csv"),
        "apifoot": os.path.join(out_dir, "odds_apifootball.csv"),
    }

    # Leitura e normalização por provedor
    df_theodds  = read_provider_csv(paths["theodds"], "theoddsapi", debug=args.debug)
    df_apifoot  = read_provider_csv(paths["apifoot"], "api-football", debug=args.debug)

    per_provider = []
    if not df_theodds.empty:
        per_provider.append(df_theodds)
    if not df_apifoot.empty:
        per_provider.append(df_apifoot)

    # Base de jogos (opcional)
    base_matches = load_matches_source(out_dir, debug=args.debug)

    # Consolida
    consensus = aggregate_consensus(per_provider, base_matches, debug=args.debug)

    total_rows = len(consensus)
    valid_rows = int(consensus["valid_markets"].fillna(0).astype(int).ge(2).sum()) if total_rows > 0 else 0

    if args.debug:
        print(f"[consensus-safe] consenso bruto: {total_rows} linhas; válidas (>=2 odds > 1.0): {valid_rows}", flush=True)

    # Salva SEM filtrar (mantém tudo), mas a etapa seguinte pode filtrar por valid_markets>=2
    out_path = os.path.join(out_dir, "odds_consensus.csv")
    consensus.to_csv(out_path, index=False)

    if valid_rows == 0:
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.", flush=True)
        print(f"[consensus-safe] consenso bruto: {total_rows} linhas; válidas (>=2 odds > 1.0): {valid_rows}", flush=True)
        # Mantém o arquivo escrito (útil para debug), porém retorna exit 10
        sys.exit(10)

    print(f"[consensus-safe] OK -> {out_path} ({total_rows} linhas; válidas >=2 odds: {valid_rows})", flush=True)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        # Evita stacktrace gigante e dá um erro controlado
        print(f"[consensus-safe] ERRO inesperado: {e}", flush=True)
        sys.exit(2)