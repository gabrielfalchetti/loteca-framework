#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Blend entre probabilidades do modelo (probabilities.csv) e do mercado (odds.csv),
com remoção de vigorish simples. Funciona mesmo sem odds (faz fallback para o modelo).

Entradas (em data/out/<rodada>/):
- probabilities.csv  (colunas: match_id, p1, px, p2, ...)
- odds.csv           (colunas esperadas: match_id, k1, kx, k2)  [opcional]

Saídas:
- probabilities_blended.csv
- (opcional) sobrescreve probabilities.csv com o blend, para manter compat com pipeline

Exemplo:
  python scripts/blend_probs.py --rodada 2025-09-27_1213 --alpha 0.75
"""

from __future__ import annotations
import argparse
import os
import sys
import math
import pandas as pd

def _read_probs(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # normalizar nomes
    lower = {c: c.lower() for c in df.columns}
    df.rename(columns=lower, inplace=True)
    need = {"match_id","p1","px","p2"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"probabilities.csv sem colunas necessárias: {miss}")
    return df

def _read_odds(path: str) -> pd.DataFrame | None:
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    lower = {c: c.lower() for c in df.columns}
    df.rename(columns=lower, inplace=True)

    # suportar nomes k1/kx/k2 (padrão do seu pipeline)
    expected = {"match_id","k1","kx","k2"}
    if not expected.issubset(df.columns):
        # tentar variantes
        alt = {"home_odds":"k1", "draw_odds":"kx", "away_odds":"k2"}
        for a,b in alt.items():
            if a in df.columns and b not in df.columns:
                df[b] = df[a]
        if not expected.issubset(df.columns):
            # se ainda não bater, ignorar odds
            return None
    return df[["match_id","k1","kx","k2"]].copy()

def _desvig(row) -> tuple[float,float,float] | None:
    """Remove vigorish por normalização simples. Retorna (p1,pX,p2) do mercado ou None se inválido."""
    k1, kx, k2 = row["k1"], row["kx"], row["k2"]
    if not all(isinstance(v, (int,float)) for v in [k1,kx,k2]):
        return None
    if any((v is None) or (not math.isfinite(v)) or (v <= 1e-9) for v in [k1,kx,k2]):
        return None
    imp1 = 1.0 / k1
    impx = 1.0 / kx
    imp2 = 1.0 / k2
    s = imp1 + impx + imp2
    if s <= 0:
        return None
    return imp1/s, impx/s, imp2/s

def main():
    ap = argparse.ArgumentParser(description="Blending Bayesiano (modelo ⨉ mercado) com fallback.")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--alpha", type=float, default=0.75, help="peso do MODELO no blend (0..1). Mercado pesa (1-alpha).")
    ap.add_argument("--overwrite_probabilities", action="store_true", help="se setado, sobrescreve probabilities.csv com o blend")
    args = ap.parse_args()

    out_dir = os.path.join("data","out",args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    probs_path = os.path.join(out_dir, "probabilities.csv")
    odds_path  = os.path.join(out_dir, "odds.csv")
    out_blend  = os.path.join(out_dir, "probabilities_blended.csv")

    if not os.path.exists(probs_path):
        print(f"[blend] ERRO: não achei {probs_path}", file=sys.stderr)
        sys.exit(1)

    model = _read_probs(probs_path)
    market = _read_odds(odds_path)

    if market is None or market.empty:
        print("[blend] AVISO: odds indisponíveis — usando somente o modelo.")
        blended = model.copy()
        blended["blend_source"] = "model_only"
    else:
        # calcular probs de mercado (desvig)
        mk = market.copy()
        mk[["p1_mkt","px_mkt","p2_mkt"]] = mk.apply(
            lambda r: pd.Series(_desvig(r) or (float("nan"),)*3), axis=1
        )
        # junta
        df = model.merge(mk[["match_id","p1_mkt","px_mkt","p2_mkt"]], on="match_id", how="left")

        # blend seguro linha a linha
        rows = []
        for _, r in df.iterrows():
            p1, px, p2 = float(r["p1"]), float(r["px"]), float(r["p2"])
            m1, mx, m2 = r.get("p1_mkt"), r.get("px_mkt"), r.get("p2_mkt")

            if pd.notna(m1) and pd.notna(mx) and pd.notna(m2):
                b1 = args.alpha * p1 + (1 - args.alpha) * float(m1)
                bx = args.alpha * px + (1 - args.alpha) * float(mx)
                b2 = args.alpha * p2 + (1 - args.alpha) * float(m2)
                s = b1 + bx + b2
                if s > 0:
                    b1, bx, b2 = b1/s, bx/s, b2/s
                src = "blend_model_market"
            else:
                b1, bx, b2 = p1, px, p2
                src = "model_only"

            row = dict(r)
            row.update({"p1": b1, "px": bx, "p2": b2, "blend_source": src})
            rows.append(row)

        blended = pd.DataFrame(rows)
        blended = blended[model.columns.tolist() + ["blend_source"]]

    blended.to_csv(out_blend, index=False, encoding="utf-8")
    print(f"[blend] OK -> {out_blend} ({len(blended)} linhas)")

    if args.overwrite_probabilities:
        blended.drop(columns=["blend_source"], errors="ignore").to_csv(probs_path, index=False, encoding="utf-8")
        print(f"[blend] probabilities.csv sobrescrito com o blend.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[blend] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
