#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera risk_report.csv consolidando probabilidades, odds (se houver) e metadados do jogo.

• Corrige automaticamente tipos de `match_id` (sempre string) e espaços.
• Evita erro de colunas duplicadas (ex.: 'home') ao fazer merge.
• Funciona mesmo sem odds.csv (marca reason='missing_odds').
• Exporta métricas no W&B se disponível (opcional).
"""

from __future__ import annotations
import argparse
import os
import sys
import pandas as pd

# -------- Utils --------

def _fix_match_id(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    if "match_id" in df.columns:
        df["match_id"] = (
            df["match_id"]
            .astype(str)
            .str.strip()
        )
    return df

def _read_csv_safe(path: str, required: bool = True, **kwargs) -> pd.DataFrame | None:
    if path and os.path.exists(path):
        df = pd.read_csv(path, **kwargs)
        return df
    if required:
        raise FileNotFoundError(f"Arquivo obrigatório não encontrado: {path}")
    return None

def _rename_dupes(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Se houver colunas repetidas (ex.: 'home'), renomeia preservando a primeira.
    Ex.: 'home', 'home' -> 'home', 'home__{prefix}'
    """
    cols = df.columns.tolist()
    seen = {}
    new_cols = []
    for c in cols:
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}__{prefix}")
    df.columns = new_cols
    return df

def _try_import_wandb():
    try:
        import wandb
        return wandb
    except Exception:
        return None

# -------- Lógica principal --------

def build_risk(rodada: str) -> pd.DataFrame:
    base_dir = f"data/out/{rodada}"

    # Entradas
    probs_path   = os.path.join(base_dir, "probabilities.csv")
    odds_path    = os.path.join(base_dir, "odds.csv")
    matches_path = os.path.join(base_dir, "matches.csv")

    probs   = _read_csv_safe(probs_path, required=True)
    matches = _read_csv_safe(matches_path, required=False)
    odds    = _read_csv_safe(odds_path, required=False)

    # Tipos & limpeza
    probs   = _rename_dupes(_fix_match_id(probs),   "probs")
    matches = _rename_dupes(_fix_match_id(matches), "matches") if matches is not None else None
    odds    = _rename_dupes(_fix_match_id(odds),    "odds")    if odds is not None else None

    # Normaliza colunas principais esperadas em probabilities.csv
    # Aceita tanto nomes padrão quanto variações comuns
    col_map_candidates = {
        "p1": ["p1", "prob_1", "home_win", "prob_home"],
        "px": ["px", "prob_x", "draw", "prob_draw"],
        "p2": ["p2", "prob_2", "away_win", "prob_away"],
    }
    def _pick(df, options, default):
        for c in options:
            if c in df.columns:
                return c
        return default

    p1_col = _pick(probs, col_map_candidates["p1"], "p1")
    px_col = _pick(probs, col_map_candidates["px"], "px")
    p2_col = _pick(probs, col_map_candidates["p2"], "p2")

    missing_core = [c for c in [p1_col, px_col, p2_col] if c not in probs.columns]
    if missing_core:
        raise ValueError(f"probabilities.csv sem colunas necessárias (p1/px/p2). Faltando: {missing_core}")

    # Seleciona e renomeia para padrão
    probs_std = probs[["match_id", p1_col, px_col, p2_col]].rename(
        columns={p1_col: "p1", px_col: "px", p2_col: "p2"}
    )

    # Metadados do jogo (se existirem)
    meta_cols = []
    if matches is not None:
        for c in ["home", "away", "home_team", "away_team", "league", "kickoff", "date", "ts"]:
            if c in matches.columns:
                meta_cols.append(c)
        matches_std = matches[["match_id"] + meta_cols].copy()
    else:
        matches_std = None

    # Merge probs + meta
    df = probs_std.copy()
    if matches_std is not None:
        df = df.merge(matches_std, on="match_id", how="left")

    # Se houver odds, calcula EV; senão marca motivo
    if odds is not None and not odds.empty:
        # Tenta detectar colunas de odds padrão
        k1 = "k1" if "k1" in odds.columns else None
        kx = "kx" if "kx" in odds.columns else None
        k2 = "k2" if "k2" in odds.columns else None

        # Também aceita nomes comuns
        alt_map = {
            "k1": ["k1", "odds_1", "home_odds"],
            "kx": ["kx", "odds_x", "draw_odds"],
            "k2": ["k2", "odds_2", "away_odds"],
        }
        def _find_col(df, preferred, alts):
            if preferred and preferred in df.columns:
                return preferred
            for c in alts:
                if c in df.columns:
                    return c
            return None

        k1 = _find_col(odds, k1, alt_map["k1"])
        kx = _find_col(odds, kx, alt_map["kx"])
        k2 = _find_col(odds, k2, alt_map["k2"])

        keep = ["match_id"] + [c for c in [k1, kx, k2] if c]
        odds_std = odds[keep].rename(columns={k1: "k1", kx: "kx", k2: "k2"})

        # Garantir tipo numérico nas odds
        for c in ["k1", "kx", "k2"]:
            if c in odds_std.columns:
                odds_std[c] = pd.to_numeric(odds_std[c], errors="coerce")

        df = df.merge(odds_std, on="match_id", how="left")

        # EV (expected value) por seleção quando odds presentes
        for sel, prob_col, odd_col in [("1", "p1", "k1"), ("X", "px", "kx"), ("2", "p2", "k2")]:
            if odd_col in df.columns:
                df[f"ev_{sel.lower()}"] = df[prob_col] * (df[odd_col] - 1)

        # Motivo de risco
        df["risk_reason"] = df.apply(
            lambda r: "ok" if (pd.notna(r.get("k1")) and pd.notna(r.get("kx")) and pd.notna(r.get("k2")))
            else "missing_some_odds", axis=1
        )
    else:
        df["risk_reason"] = "missing_odds"
        # coloca colunas de odds/ev vazias para manter schema
        for c in ["k1", "kx", "k2", "ev_1", "ev_x", "ev_2"]:
            if c not in df.columns:
                df[c] = pd.NA

    # Ordena por match_id (estável)
    df = df.sort_values("match_id", kind="stable").reset_index(drop=True)

    # Colunas de saída (organizadas)
    ordered = [c for c in [
        "match_id",
        # meta
        "home", "away", "home_team", "away_team", "league", "kickoff", "date", "ts",
        # probs
        "p1", "px", "p2",
        # odds
        "k1", "kx", "k2",
        # EV
        "ev_1", "ev_x", "ev_2",
        # risco
        "risk_reason",
    ] if c in df.columns] + [c for c in df.columns if c not in [
        "match_id","home","away","home_team","away_team","league","kickoff","date","ts",
        "p1","px","p2","k1","kx","k2","ev_1","ev_x","ev_2","risk_reason"
    ]]

    return df[ordered]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Identificador da rodada, ex: 2025-09-27_1213")
    args = parser.parse_args()

    rodada = args.rodada
    out_dir = f"data/out/{rodada}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "risk_report.csv")

    try:
        df = build_risk(rodada)
        df.to_csv(out_path, index=False)
        print(f"[risk] OK -> {out_path} ({len(df)} linhas)")

        # Métricas W&B (opcional)
        wandb = _try_import_wandb()
        if wandb:
            try:
                run = wandb.init(project="loteca", job_type="risk", name=f"risk_{rodada}", reinit=True)
                metrics = {
                    "risk_rows": len(df),
                    "risk_missing_odds": int((df["risk_reason"] == "missing_odds").sum()) if "risk_reason" in df.columns else 0,
                }
                wandb.log(metrics)
                run.finish()
            except Exception as e:
                print(f"[risk] WARN wandb: {e}")

    except Exception as e:
        print(f"[risk] ERRO não fatal: {e}", file=sys.stderr)
        # Garante ao menos um CSV vazio com header mínimo para não quebrar pipeline
        empty_cols = ["match_id", "p1", "px", "p2", "k1", "kx", "k2", "ev_1", "ev_x", "ev_2", "risk_reason"]
        pd.DataFrame(columns=empty_cols).to_csv(out_path, index=False)
        print(f"[risk] OK -> {out_path} (0 linhas)")

if __name__ == "__main__":
    main()
