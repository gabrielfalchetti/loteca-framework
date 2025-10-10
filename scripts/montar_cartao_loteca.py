#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera o cartão Loteca a partir dos artefatos da rodada.

Saídas:
  - {OUT_DIR}/cartao_loteca.csv
  - {OUT_DIR}/cartao_loteca.md

Ordem dos jogos preservada da whitelist. Seleção do palpite (1/X/2) a partir de:
  1) kelly_stakes.csv (se existir): usa o maior 'prob_*' da linha para pick,
     e aproveita 'stake' e 'pick' se presentes.
  2) calibrated_probs.csv (se existir)
  3) predictions_market.csv (se existir)
  4) odds_consensus.csv (se existir; infere probs via 1/odds e normaliza)

Parâmetros:
  --rodada OUT_DIR            (obrigatório)
  --max-jogos N               (padrão 14)
  --prefer-kelly              (se houver Kelly, respeita TOP_N por stake>0)
  --debug
"""

import argparse
import os
from pathlib import Path
import pandas as pd
import numpy as np

REQ_WL_COLS = ["match_id", "home", "away"]
PROB_COLS = ["prob_home", "prob_draw", "prob_away"]
ODDS_COLS = ["odds_home", "odds_draw", "odds_away"]

def read_csv_safe(path: Path) -> pd.DataFrame | None:
    try:
        if path.is_file() and path.stat().st_size > 0:
            return pd.read_csv(path)
    except Exception as e:
        print(f"[loteca][WARN] Não consegui ler {path}: {e}")
    return None

def infer_probs_from_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Se houver odds_* válidas, cria prob_* normalizadas."""
    if not set(ODDS_COLS).issubset(df.columns):
        return df
    d = df.copy()
    for c in ODDS_COLS:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    # odds válidas: >1 e não NaN
    mask = (d["odds_home"] > 1.0) & (d["odds_away"] > 1.0) & d["odds_home"].notna() & d["odds_away"].notna()
    # empates podem faltar em algumas ligas (usa 2-way se preciso)
    if "odds_draw" in d and d["odds_draw"].notna().any():
        mask = mask & (d["odds_draw"] > 1.0)
    d.loc[mask, "prob_home"] = 1.0 / d.loc[mask, "odds_home"]
    if "odds_draw" in d:
        d.loc[mask, "prob_draw"] = 1.0 / d.loc[mask, "odds_draw"]
    else:
        d.loc[mask, "prob_draw"] = np.nan
    d.loc[mask, "prob_away"] = 1.0 / d.loc[mask, "odds_away"]

    # normaliza por linha (se ao menos home e away existem)
    def _norm(row):
        vals = [row.get("prob_home", np.nan), row.get("prob_draw", np.nan), row.get("prob_away", np.nan)]
        arr = np.array([x if pd.notna(x) else 0.0 for x in vals], dtype=float)
        s = arr.sum()
        if s > 0:
            arr = arr / s
        # caso empates venham todos NaN, re-normaliza home/away
        if (np.isnan(vals[1]) or vals[1] == 0) and arr.sum() > 0:
            arr = arr / arr.sum()
        return pd.Series({"prob_home": arr[0], "prob_draw": arr[1], "prob_away": arr[2]})

    probs = d.apply(_norm, axis=1)
    for c in PROB_COLS:
        d[c] = probs[c]
    return d

def decide_pick(row: pd.Series) -> tuple[str, float]:
    """Retorna (pick_str, prob_escolhida). Regras:
       - se existir coluna 'pick' (1/X/2) válida, respeita
       - senão, usa maior prob de prob_home/prob_draw/prob_away
    """
    # 1) respeita pick existente
    if "pick" in row and isinstance(row["pick"], str):
        pk = row["pick"].strip().upper()
        if pk in ("1", "X", "2"):
            if pk == "1":
                return "1", row.get("prob_home", np.nan)
            elif pk == "X":
                return "X", row.get("prob_draw", np.nan)
            else:
                return "2", row.get("prob_away", np.nan)

    # 2) maior prob
    ph, pd_, pa = row.get("prob_home", np.nan), row.get("prob_draw", np.nan), row.get("prob_away", np.nan)
    vals = np.array([ph, pd_, pa], dtype=float)
    # trata NaNs como -inf pra não escolher
    vals_mask = np.where(np.isnan(vals), -1.0, vals)
    idx = int(np.argmax(vals_mask))
    return ("1", ph) if idx == 0 else ( "X", pd_ ) if idx == 1 else ( "2", pa )

def load_best_probs(out_dir: Path) -> pd.DataFrame:
    """Carrega o melhor conjunto de probabilidades disponível, com fallback."""
    # prioridade: calibrated_probs -> predictions_market -> odds_consensus (inferir)
    cand = [
        out_dir / "calibrated_probs.csv",
        out_dir / "predictions_market.csv",
        out_dir / "odds_consensus.csv",
    ]
    base = None
    for p in cand:
        df = read_csv_safe(p)
        if df is None:
            continue
        # padroniza nomes de colunas
        df = df.rename(columns={
            "team_home": "home", "team_away": "away",
            "p_home": "prob_home", "p_draw": "prob_draw", "p_away": "prob_away",
        })
        # se só tem odds, infere probs
        if not set(PROB_COLS).issubset(df.columns):
            df = infer_probs_from_odds(df)
        if set(REQ_WL_COLS).issubset(df.columns) and set(PROB_COLS).issubset(df.columns):
            base = df[[*REQ_WL_COLS, *PROB_COLS]].copy()
            break
    if base is None:
        raise FileNotFoundError("Nenhum arquivo de probabilidades válido encontrado (calibrated/predictions_market/odds_consensus).")
    return base

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Pasta da rodada (OUT_DIR)")
    ap.add_argument("--max-jogos", type=int, default=14, help="Qtd máx de jogos no cartão (padrão 14)")
    ap.add_argument("--prefer-kelly", action="store_true", help="Se houver Kelly, respeita ordem por stake>0 (TOP_N)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.rodada)
    wl = read_csv_safe(out_dir / "matches_whitelist.csv")
    if wl is None:
        raise SystemExit(f"[loteca][ERRO] Whitelist ausente: {out_dir/'matches_whitelist.csv'}")
    for c in REQ_WL_COLS:
        if c not in wl.columns:
            raise SystemExit(f"[loteca][ERRO] Whitelist sem coluna obrigatória '{c}'")
    # preserva ordem original
    wl = wl[[*REQ_WL_COLS]].copy()

    # tenta carregar probabilidades
    base = load_best_probs(out_dir)

    # merge base + whitelist (inner para manter só jogos da wl)
    df = wl.merge(base, on=REQ_WL_COLS, how="left")

    # tenta anexar Kelly (se existir)
    kelly = read_csv_safe(out_dir / "kelly_stakes.csv")
    if kelly is not None:
        kelly = kelly.rename(columns={"team_home":"home","team_away":"away"})
        cols_keep = [c for c in ["match_id","home","away","stake","pick","edge","prob_home","prob_draw","prob_away"] if c in kelly.columns]
        kelly = kelly[cols_keep].copy()
        # Kelly pode trazer prob_* melhores (calibradas). Completa onde estiver NaN.
        for tgt in PROB_COLS:
            if tgt in kelly.columns and tgt in df.columns:
                df[tgt] = df[tgt].fillna(kelly[tgt])
        # junta stake/pick
        df = df.merge(kelly.drop(columns=[c for c in PROB_COLS if c in kelly.columns]), on=["match_id","home","away"], how="left")

    # decide pick por linha
    picks = []
    probs_escolha = []
    for _, row in df.iterrows():
        p, pv = decide_pick(row)
        picks.append(p)
        probs_escolha.append(pv)
    df["pick"] = picks
    df["pick_prob"] = probs_escolha

    # se prefer_kelly: ordena por stake desc (>0), mantendo whitelist order como desempate
    df["_ord"] = np.arange(len(df))
    if args.prefer_kelly and "stake" in df.columns:
        df["_stake_ok"] = df["stake"].fillna(0.0)
        df = df.sort_values(by=["_stake_ok","_ord"], ascending=[False,True])
    else:
        df = df.sort_values(by=["_ord"])

    # limita a N jogos
    df = df.head(args.max_jogos).copy()
    df["jogo"] = np.arange(1, len(df)+1)

    # rationale curta
    def mk_rat(row):
        parts = []
        if "stake" in row and pd.notna(row["stake"]) and row["stake"] > 0:
            parts.append(f"Kelly {row['stake']:.2f}")
        ph, pd_, pa = row.get("prob_home", np.nan), row.get("prob_draw", np.nan), row.get("prob_away", np.nan)
        if pd.notna(ph) and pd.notna(pa):
            parts.append(f"P(H/D/A)={ph:.2f}/{(pd_ if pd.notna(pd_) else 0):.2f}/{pa:.2f}")
        return " | ".join(parts) if parts else ""
    df["rationale"] = df.apply(mk_rat, axis=1)

    # seleciona colunas finais
    out_cols = ["jogo","match_id","home","away","pick","prob_home","prob_draw","prob_away","pick_prob"]
    if "stake" in df.columns:
        out_cols.insert(5, "stake")
    out_cols.append("rationale")
    out_csv = df[out_cols].copy()

    # salva CSV
    out_csv_path = out_dir / "cartao_loteca.csv"
    out_csv.to_csv(out_csv_path, index=False, encoding="utf-8")
    print(f"[loteca] cartao_loteca.csv salvo em: {out_csv_path}")

    # salva Markdown amigável
    lines = ["# Cartão Loteca", ""]
    for _, r in out_csv.iterrows():
        stake_txt = f" | Stake: {r['stake']:.2f}" if "stake" in out_csv.columns and pd.notna(r["stake"]) else ""
        lines.append(
            f"**Jogo {int(r['jogo'])}** — {r['home']} x {r['away']} | Palpite: **{r['pick']}**{stake_txt}  \n"
            f"_Prob(H/D/A): {r['prob_home']:.2f}/{(r['prob_draw'] if pd.notna(r['prob_draw']) else 0):.2f}/{r['prob_away']:.2f}_"
            + (f"  \n_{r['rationale']}_" if isinstance(r['rationale'], str) and r['rationale'] else "")
        )
        lines.append("")
    (out_dir / "cartao_loteca.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"[loteca] cartao_loteca.md salvo em: {out_dir/'cartao_loteca.md'}")

    if args.debug:
        print(out_csv.head(20).to_string(index=False))

if __name__ == "__main__":
    main()