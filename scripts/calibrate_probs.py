#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
calibrate_probs.py
------------------
Gera probabilidades calibradas por jogo.

Entradas esperadas em data/out/<rodada>/:
- predictions_market.csv (opcional, preferencial)
    colunas ALTERNATIVAS aceitas:
      A) match_id, prob_home, prob_draw, prob_away
      B) match_key, p_home, p_draw, p_away, home, away  (aceito como backup)
- odds_consensus.csv (fallback SE predictions_market.csv não tiver prob_*)
    colunas mínimas: team_home, team_away, odds_home, odds_draw, odds_away
    (match_id será criado se não houver)

Saída:
- calibrated_probs.csv
    colunas: match_id, calib_method, calib_home, calib_draw, calib_away

Notas:
- Implementa sinalizador --debug (log detalhado).
- Método padrão: “Dirichlet” simples (amaciamento beta-like) para manter soma=1.
- Se predictions_market.csv não tiver prob_*, converte odds → probs (implied).

Uso:
python scripts/calibrate_probs.py --rodada <path|id> [--history HIST] [--model_path PATH] [--debug]
"""

import argparse
import os
import csv
import pandas as pd
from typing import Tuple


def log(msg: str, debug: bool = False):
    if debug:
        print(msg, flush=True)


def resolve_out_dir(rodada: str) -> str:
    if os.path.isdir(rodada):
        return rodada
    path = os.path.join("data", "out", str(rodada))
    os.makedirs(path, exist_ok=True)
    return path


def implied_from_odds(oh: float, od: float, oa: float) -> Tuple[float, float, float]:
    ih = 0.0 if oh is None or oh <= 0 else 1.0 / oh
    id_ = 0.0 if od is None or od <= 0 else 1.0 / od
    ia = 0.0 if oa is None or oa <= 0 else 1.0 / oa
    s = ih + id_ + ia
    if s <= 0:
        return (0.0, 0.0, 0.0)
    return (ih / s, id_ / s, ia / s)


def dirichlet_smooth(ph: float, pd: float, pa: float, alpha: float = 1.0) -> Tuple[float, float, float]:
    """
    Suavização simples tipo Dirichlet (equivalente a adicionar alpha pseudo-contagens uniformes).
    Mantém soma = 1.
    """
    ph = max(ph, 0.0)
    pd = max(pd, 0.0)
    pa = max(pa, 0.0)
    s = ph + pd + pa
    if s <= 0:
        # uniforme
        return (1/3, 1/3, 1/3)
    ph, pd, pa = ph / s, pd / s, pa / s
    k = 3.0
    ph2 = (ph + alpha/k) / (1 + alpha)
    pd2 = (pd + alpha/k) / (1 + alpha)
    pa2 = (pa + alpha/k) / (1 + alpha)
    # normaliza por segurança
    s2 = ph2 + pd2 + pa2
    return (ph2/s2, pd2/s2, pa2/s2) if s2 > 0 else (1/3, 1/3, 1/3)


def load_market_probs(out_dir: str, debug: bool = False) -> pd.DataFrame | None:
    """
    Tenta ler predictions_market.csv em formato com prob_*
    Retornos possíveis:
      - DF com colunas ['match_id','prob_home','prob_draw','prob_away']
      - None se arquivo inexistente
      - Se existir mas faltar prob_*, retorna DF vazio (shape[0]==0) sinalizando fallback
    """
    fp = os.path.join(out_dir, "predictions_market.csv")
    if not os.path.isfile(fp):
        return None

    df = pd.read_csv(fp)
    col_a = all(c in df.columns for c in ["match_id", "prob_home", "prob_draw", "prob_away"])
    if col_a:
        # formata / tipa
        for c in ["prob_home", "prob_draw", "prob_away"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        return df[["match_id", "prob_home", "prob_draw", "prob_away"]].copy()

    # aceitar alternativa (p_home/p_draw/p_away + match_key)
    col_b = all(c in df.columns for c in ["match_key", "p_home", "p_draw", "p_away", "home", "away"])
    if col_b:
        df2 = df.rename(columns={
            "p_home": "prob_home",
            "p_draw": "prob_draw",
            "p_away": "prob_away",
        }).copy()
        # cria match_id se não existir
        if "match_id" not in df2.columns:
            df2["match_id"] = df2["home"].astype(str).str.strip() + "__" + df2["away"].astype(str).str.strip()
        for c in ["prob_home", "prob_draw", "prob_away"]:
            df2[c] = pd.to_numeric(df2[c], errors="coerce").fillna(0.0)
        return df2[["match_id", "prob_home", "prob_draw", "prob_away"]].copy()

    # arquivo presente, mas sem prob_* => indicar fallback
    return pd.DataFrame(columns=["match_id", "prob_home", "prob_draw", "prob_away"])


def fallback_from_odds(out_dir: str, debug: bool = False) -> pd.DataFrame:
    """
    Constrói probs a partir de odds_consensus.csv.
    Saída: ['match_id','prob_home','prob_draw','prob_away']
    """
    fp = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.isfile(fp):
        raise FileNotFoundError("[calibrate] odds_consensus.csv não encontrado para fallback.")

    df = pd.read_csv(fp)
    need = ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"[calibrate] odds_consensus.csv está sem colunas obrigatórias: {miss}")

    # cria match_id se não houver
    if "match_id" not in df.columns:
        df["match_id"] = df["team_home"].astype(str).str.strip() + "__" + df["team_away"].astype(str).str.strip()

    for c in ["odds_home", "odds_draw", "odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    probs = df.apply(lambda r: implied_from_odds(r["odds_home"], r["odds_draw"], r["odds_away"]), axis=1)
    tmp = pd.DataFrame(probs.tolist(), columns=["prob_home", "prob_draw", "prob_away"], index=df.index)
    out = pd.concat([df[["match_id"]], tmp], axis=1)
    if debug:
        print(f"[calibrate] Fallback probs (odds→probs) gerado para {len(out)} jogos.")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ID da rodada ou caminho data/out/<id>")
    ap.add_argument("--history", default="", help="(reservado) arquivo histórico para calibração supervisionada")
    ap.add_argument("--model_path", default="", help="(reservado) caminho de modelo de calibração")
    ap.add_argument("--debug", action="store_true", help="exibe logs detalhados")
    args = ap.parse_args()

    out_dir = resolve_out_dir(args.rodada)

    print("=" * 51)
    print("[calibrate] INICIANDO CALIBRAÇÃO DE PROBABILIDADES")
    print(f"[calibrate] Diretório de rodada : {out_dir}")
    print("=" * 51)

    # 1) tenta usar predictions_market.csv
    df_m = load_market_probs(out_dir, args.debug)

    if df_m is None:
        # não existe predictions_market.csv → cair no fallback por odds
        if args.debug:
            print("[calibrate] predictions_market.csv ausente. Usando fallback por odds.")
        df_probs = fallback_from_odds(out_dir, args.debug)
    else:
        # existe, mas tem prob_*?
        if df_m.shape[0] == 0:
            print("[calibrate] predictions_market.csv sem colunas ['match_id', 'prob_home', 'prob_draw', 'prob_away']. Usando fallback por odds.")
            df_probs = fallback_from_odds(out_dir, args.debug)
        else:
            df_probs = df_m

    # 2) aplica suavização “Dirichlet”
    method = "Dirichlet"
    ch, cd, ca = [], [], []
    for _, r in df_probs.iterrows():
        ph, pd, pa = float(r["prob_home"]), float(r["prob_draw"]), float(r["prob_away"])
        sh, sd, sa = dirichlet_smooth(ph, pd, pa, alpha=0.5)
        ch.append(sh); cd.append(sd); ca.append(sa)

    out = pd.DataFrame({
        "match_id": df_probs["match_id"].astype(str),
        "calib_method": method,
        "calib_home": ch,
        "calib_draw": cd,
        "calib_away": ca
    })

    # 3) salva
    out_fp = os.path.join(out_dir, "calibrated_probs.csv")
    out.to_csv(out_fp, index=False, quoting=csv.QUOTE_MINIMAL)
    if args.debug:
        print(f"[calibrate] Método usado: {method}")
        print(f"[calibrate] Salvo em: {out_fp}")
        try:
            print(out.head().to_string(index=False))
        except Exception:
            print(out.head())
        print("[ok] Calibração concluída com sucesso.")
    else:
        print("[ok] Calibração concluída.")


if __name__ == "__main__":
    main()