#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Constrói probabilities.csv a partir de:
- preds_bivar.csv (se existir), ou
- xg.csv / features_base.csv (gerando preds_bivar na hora).

Saídas:
- data/out/<rodada>/probabilities.csv
- (se necessário) data/out/<rodada>/preds_bivar.csv
"""

from __future__ import annotations
import argparse
import os
import sys
import pandas as pd

# W&B é opcional; mantemos o mesmo comportamento dos seus logs
def _try_wandb_init(run_name: str, rodada: str):
    try:
        import wandb
        wandb.init(project="loteca", name=f"{run_name}_{rodada}", reinit=True)
        return wandb
    except Exception:
        return None

def _ensure_preds_bivar(rodada: str, max_goals: int = None) -> str:
    """
    Garante a existência de preds_bivar.csv. Se não existir, chama o gerador interno.
    Retorna o caminho do arquivo.
    """
    out_dir = os.path.join("data", "out", rodada)
    pred_path = os.path.join(out_dir, "preds_bivar.csv")
    if os.path.exists(pred_path):
        return pred_path

    # gera localmente (importando a função do script irmão sem depender de CLI)
    # para evitar circularidade, copiamos uma implementação mínima aqui:
    import math

    def _poisson_pmf(k: int, lam: float) -> float:
        if lam <= 0:
            return 1.0 if k == 0 else 0.0
        return math.exp(-lam) * (lam ** k) / math.factorial(k)

    def _grid_probs(lh: float, la: float, max_g: int | None) -> tuple[float,float,float]:
        if max_g is None:
            max_g = int(min(18, max(10, math.ceil(lh + la + 8))))
        ph = [_poisson_pmf(i, lh) for i in range(max_g + 1)]
        pa = [_poisson_pmf(j, la) for j in range(max_g + 1)]
        p1 = px = p2 = 0.0
        for i, p_i in enumerate(ph):
            for j, p_j in enumerate(pa):
                pij = p_i * p_j
                if i > j:
                    p1 += pij
                elif i == j:
                    px += pij
                else:
                    p2 += pij
        s = p1 + px + p2
        if s > 0:
            p1 /= s; px /= s; p2 /= s
        return p1, px, p2

    # fonte: xg.csv preferido; fallback features_base.csv
    xg_path = os.path.join(out_dir, "xg.csv")
    fb_path = os.path.join(out_dir, "features_base.csv")
    if os.path.exists(xg_path):
        df = pd.read_csv(xg_path)
        df["_source"] = "xg"
    elif os.path.exists(fb_path):
        df = pd.read_csv(fb_path)
        df["_source"] = "features_base"
    else:
        raise FileNotFoundError("Nem xg.csv nem features_base.csv encontrados para gerar preds_bivar.")

    # detectar colunas
    mid = None
    for c in df.columns:
        if c.lower() in ("match_id","id_partida","id","partida_id"):
            mid = c; break
    cand_home = [c for c in df.columns if c.lower() in ("xg_home","xh","home_xg","xgh","lambda_home","lambda_h","lh")]
    cand_away = [c for c in df.columns if c.lower() in ("xg_away","xa","away_xg","xga","lambda_away","lambda_a","la")]
    if mid is None or not cand_home or not cand_away:
        raise ValueError("Não consegui identificar match_id e xG home/away para gerar preds_bivar.")
    ch, ca = cand_home[0], cand_away[0]

    # tentar nomes dos times
    home_col = away_col = None
    try:
        matches = pd.read_csv(os.path.join(out_dir, "matches.csv"))
        m_mid = [c for c in matches.columns if c.lower() == "match_id"]
        m_home = [c for c in matches.columns if c.lower() in ("home","mandante","time_casa")]
        m_away = [c for c in matches.columns if c.lower() in ("away","visitante","time_fora")]
        if m_mid and m_home and m_away:
            matches = matches[[m_mid[0], m_home[0], m_away[0]]].rename(
                columns={m_mid[0]:"match_id", m_home[0]:"home", m_away[0]:"away"}
            )
            df = df.merge(matches, left_on=mid, right_on="match_id", how="left")
            home_col, away_col = "home", "away"
    except Exception:
        pass

    rows = []
    for _, r in df.iterrows():
        lh = float(max(0.0, r[ch]))
        la = float(max(0.0, r[ca]))
        p1, px, p2 = _grid_probs(lh, la, max_goals)
        row = {
            "match_id": r[mid],
            "lambda_home": lh,
            "lambda_away": la,
            "p1": p1, "px": px, "p2": p2
        }
        if home_col and away_col:
            row["home"] = r.get(home_col, None)
            row["away"] = r.get(away_col, None)
        rows.append(row)

    out = pd.DataFrame(rows)
    out.to_csv(pred_path, index=False, encoding="utf-8")
    print(f"[build_probs] preds_bivar gerado -> {pred_path} ({len(out)} linhas)")
    return pred_path

def main():
    parser = argparse.ArgumentParser(description="Constrói probabilities.csv a partir de preds_bivar/xg/features.")
    parser.add_argument("--rodada", required=True, help="Rodada ex.: 2025-09-27_1213")
    # manter compat com chamadas antigas:
    parser.add_argument("--source", required=False, help="(LEGADO) Ignorado; lógica agora é automática.")
    parser.add_argument("--max_goals", type=int, default=None, help="Grade máxima de gols ao gerar preds_bivar (se necessário).")
    args, _unknown = parser.parse_known_args()

    out_dir = os.path.join("data", "out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    if args.source:
        print(f"[build_probs] AVISO: argumento legado --source='{args.source}' ignorado. Usando lógica automática.")

    # 1) garantir preds_bivar.csv
    pred_path = os.path.join(out_dir, "preds_bivar.csv")
    if not os.path.exists(pred_path):
        pred_path = _ensure_preds_bivar(args.rodada, args.max_goals)

    # 2) carregar e montar probabilities.csv (aqui já está praticamente pronto)
    preds = pd.read_csv(pred_path)

    # Padronizar colunas esperadas
    required = {"match_id","lambda_home","lambda_away","p1","px","p2"}
    miss = required - set(map(str.lower, preds.columns))
    # se os nomes vierem com case diferente, normalizar
    cols_map = {c: c.lower() for c in preds.columns}
    preds.rename(columns=cols_map, inplace=True)

    # garantir ordenação de colunas
    base_cols = ["match_id","lambda_home","lambda_away","p1","px","p2"]
    extra_cols = [c for c in preds.columns if c not in base_cols]
    probs = preds[base_cols + extra_cols]

    # 3) salvar probabilities.csv
    out_probs = os.path.join(out_dir, "probabilities.csv")
    probs.to_csv(out_probs, index=False, encoding="utf-8")
    print(f"[build_probs] Fonte='preds_bivar' -> {out_probs} ({len(probs)} linhas)")

    # 4) wandb (opcional, como nos seus logs)
    wandb = _try_wandb_init("build_probs", args.rodada)
    if wandb:
        try:
            wandb.log({"probs_rows": len(probs), "probs_source": "preds_bivar"})
            wandb.finish()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[build_probs] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
