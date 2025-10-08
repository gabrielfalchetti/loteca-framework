#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_cartao.py (atualizado)
Usa predictions_final.csv se disponível; fallback para blend, depois market.
"""

from __future__ import annotations
import argparse
import pandas as pd
from pathlib import Path
import math

def _load_probs(rodada: Path) -> pd.DataFrame:
    for fname in ("predictions_final.csv", "predictions_blend.csv", "predictions_market.csv"):
        p = rodada / fname
        if p.exists() and p.stat().st_size > 0:
            df = pd.read_csv(p)
            # uniformiza nomes
            df = df.rename(columns={"team_home":"home","team_away":"away"})
            if not {"home","away","p_home","p_draw","p_away"}.issubset(df.columns):
                # market tem columns diferentes — tentar mapear
                if {"home","away","m_home","m_draw","m_away"}.issubset(df.columns):
                    df["p_home"] = df["m_home"]; df["p_draw"] = df["m_draw"]; df["p_away"] = df["m_away"]
                else:
                    raise RuntimeError(f"Arquivo {fname} não contém colunas de probabilidade esperadas.")
            return df
    raise FileNotFoundError("Nenhum arquivo de probabilidades encontrado.")

def _pick_1x2(r):
    v = {"1": r["p_home"], "X": r["p_draw"], "2": r["p_away"]}
    pick = max(v, key=v.get)
    conf = v[pick]
    return pick, conf

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()
    rodada = Path(args.rodada)

    wl = pd.read_csv(rodada / "matches_whitelist.csv")
    wl = wl.rename(columns={"team_home":"home","team_away":"away"})
    probs = _load_probs(rodada)

    # join por nomes (title-cased no pipeline)
    card = wl.merge(probs, on=["home","away"], how="left")

    lines = []
    miss = []
    for i, row in card.iterrows():
        jn = int(row["match_id"]) if "match_id" in card.columns and pd.notna(row.get("match_id")) else (i+1)
        if pd.isna(row.get("p_home")) or pd.isna(row.get("p_draw")) or pd.isna(row.get("p_away")):
            lines.append(f"Jogo {jn:02d} - {row['home']} x {row['away']}: ? (stake=0.0) [nan%]")
            miss.append(jn)
            continue
        pick, conf = _pick_1x2(row)
        pct = f"{conf*100:.1f}%"
        # stake: se existir kelly_stakes, lemos; senão 0
        stake = 0.0
        try:
            ks = pd.read_csv(rodada / "kelly_stakes.csv")
            ks = ks.rename(columns={"team_home":"home","team_away":"away"})
            krow = ks[(ks["home"]==row["home"]) & (ks["away"]==row["away"])]
            if len(krow):
                stake = float(krow["stake"].iloc[0] or 0.0)
        except Exception:
            pass
        lines.append(f"Jogo {jn:02d} - {row['home']} x {row['away']}: {pick} (stake={stake:.1f}) [{pct}]")

    outp = rodada / "loteca_cartao.txt"
    text = "==== CARTÃO LOTECA ====\n" + "\n".join(lines) + "\n=======================\n"
    outp.write_text(text, encoding="utf-8")
    print(text)
    print(f"[cartao] OK -> {outp}")

if __name__ == "__main__":
    main()
