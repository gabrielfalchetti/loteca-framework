#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera stakes via Kelly a partir de probabilidades/odds disponíveis.

Ordem de fontes (a primeira que atender aos requisitos é usada):
  1) calibrated_probs.csv
  2) final_probs.csv
  3) predictions_market.csv
  4) odds_consensus.csv  (converte odds -> probs)

Requisitos mínimos para seguir com cálculo (3-way H/D/A):
  - Colunas obrigatórias após normalização:
      match_id, team_home, team_away
  - E UM dos conjuntos abaixo:
      a) prob_home, prob_draw, prob_away
      b) odds_home, odds_draw, odds_away  (serão convertidas em probs)

Saída:
  {OUT_DIR}/kelly_stakes.csv  com colunas:
    match_id,team_home,team_away,pick,prob,odds,edge,kelly_raw,stake
"""

import os
import sys
import math
import argparse
import pandas as pd

EXIT_CODE = 25

def eprint(*a, **k):
    print(*a, file=sys.stderr, **k)

def read_csv_if_exists(path):
    if os.path.isfile(path) and os.path.getsize(path) > 0:
        try:
            return pd.read_csv(path)
        except Exception as ex:
            eprint(f"[kelly][WARN] Falha ao ler {path}: {ex}")
            return None
    return None

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols_map = {c.lower(): c for c in df.columns}
    # normaliza match_id
    if "match_id" not in df.columns:
        for c in list(df.columns):
            if c.lower() == "match_id":
                df = df.rename(columns={c: "match_id"})
                break
    # normaliza home/away
    # aceita: home/away ou team_home/team_away
    if "team_home" not in df.columns:
        for c in list(df.columns):
            if c.lower() == "home":
                df = df.rename(columns={c: "team_home"})
                break
    if "team_away" not in df.columns:
        for c in list(df.columns):
            if c.lower() == "away":
                df = df.rename(columns={c: "team_away"})
                break
    return df

def has_probs(df: pd.DataFrame) -> bool:
    need = {"prob_home", "prob_draw", "prob_away"}
    return need.issubset(set(df.columns))

def has_odds(df: pd.DataFrame) -> bool:
    need = {"odds_home", "odds_draw", "odds_away"}
    return need.issubset(set(df.columns))

def decimal_odds_to_probs(df: pd.DataFrame) -> pd.DataFrame:
    """Converte odds decimais para probs normalizadas (remove overround)."""
    if not has_odds(df):
        raise ValueError("decimal_odds_to_probs chamado sem odds_* completas.")
    for c in ["odds_home","odds_draw","odds_away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # valida odds > 1.0
    mask_ok = (df["odds_home"] > 1.0) & (df["odds_draw"] > 1.0) & (df["odds_away"] > 1.0)
    if not mask_ok.any():
        raise ValueError("Todas as odds válidas ( > 1.0 ) estão ausentes/inválidas.")
    df = df.loc[mask_ok].copy()

    inv = pd.DataFrame({
        "ih": 1.0 / df["odds_home"],
        "id": 1.0 / df["odds_draw"],
        "ia": 1.0 / df["odds_away"],
    })
    s = inv.sum(axis=1)
    # normaliza
    df["prob_home"] = inv["ih"] / s
    df["prob_draw"] = inv["id"] / s
    df["prob_away"] = inv["ia"] / s
    return df

def choose_pick_row(row):
    """Define o pick = outcome com maior valor de Kelly (ou maior prob se odds iguais)."""
    # odds e probs
    probs = {
        "H": row["prob_home"],
        "D": row["prob_draw"],
        "A": row["prob_away"],
    }
    odds = {
        "H": row.get("odds_home", float("nan")),
        "D": row.get("odds_draw", float("nan")),
        "A": row.get("odds_away", float("nan")),
    }

    # calcula Kelly bruto outcome a outcome (assumindo aposta decimal; b = odds-1)
    kelly_vals = {}
    for k in ["H","D","A"]:
        p = probs[k]
        o = odds[k]
        if not (isinstance(o, (int,float)) and o > 1.0):
            # se não houver odds confiável para o outcome, aproxima com fair odds = 1/p
            if isinstance(p, (int,float)) and p > 0:
                o = 1.0 / p
            else:
                o = float("nan")
        b = o - 1.0
        q = 1.0 - p
        # fórmula clássica: k = (b*p - q) / b
        try:
            k = (b * p - q) / b
        except Exception:
            k = float("nan")
        kelly_vals[k] = k

    # escolhe maior Kelly; se todos NaN, escolhe maior probabilidade
    best_outcome = max(kelly_vals, key=lambda k: (kelly_vals[k] if isinstance(kelly_vals[k], (int,float)) else -1e9))
    best_k = kelly_vals[best_outcome]
    if not (isinstance(best_k, (int,float)) and math.isfinite(best_k)):
        # fallback: maior prob
        best_outcome = max(probs, key=lambda k: probs[k])

    return best_outcome

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (OUT_DIR)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    # parâmetros do ambiente
    bankroll = float(os.environ.get("BANKROLL", "1000"))
    kelly_fraction = float(os.environ.get("KELLY_FRACTION", "0.5"))
    kelly_cap = float(os.environ.get("KELLY_CAP", "0.1"))  # fração máxima por aposta (ex: 0.1 = 10%)
    round_to = float(os.environ.get("ROUND_TO", "1"))
    top_n = int(float(os.environ.get("KELLY_TOP_N", "0"))) if os.environ.get("KELLY_TOP_N") else 0

    # tenta carregar fontes na ordem de prioridade
    cand = [
        ("calibrated_probs.csv", True),
        ("final_probs.csv", True),
        ("predictions_market.csv", False),
        ("odds_consensus.csv", False),
    ]
    df = None
    picked_source = None
    for fname, expect_probs in cand:
        path = os.path.join(out_dir, fname)
        tmp = read_csv_if_exists(path)
        if tmp is None:
            continue
        tmp = normalize_columns(tmp)
        need_base = {"match_id", "team_home", "team_away"}
        if not need_base.issubset(set(tmp.columns)):
            # tenta renomeações oportunistas
            pass
        if not need_base.issubset(set(tmp.columns)):
            eprint(f"[kelly][WARN] {fname} sem chaves mínimas {need_base}; ignorando.")
            continue

        # verifica se já tem prob_*; senão tenta odds_* -> probs
        has_p = has_probs(tmp)
        has_o = has_odds(tmp)

        if expect_probs and not (has_p or has_o):
            eprint(f"[kelly][WARN] {fname} sem prob_* ou odds_*; tentando próxima fonte.")
            continue

        # se não tem prob mas tem odds, cria prob
        if not has_p and has_o:
            try:
                tmp = decimal_odds_to_probs(tmp)
                has_p = True
            except Exception as ex:
                eprint(f"[kelly][WARN] Falha ao converter odds em {fname}: {ex}")
                continue

        if has_p:
            df = tmp.copy()
            picked_source = fname
            break

    if df is None:
        eprint("::error::Nenhum arquivo de probabilidades disponível (calibrated/final/market/consensus).")
        sys.exit(EXIT_CODE)

    # Garante tipos numéricos
    for c in ["prob_home","prob_draw","prob_away","odds_home","odds_draw","odds_away"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Filtra linhas com probs válidas (3-way)
    mask_prob = df[["prob_home","prob_draw","prob_away"]].apply(lambda s: s.notna() & (s >= 0) & (s <= 1)).all(axis=1)
    df = df.loc[mask_prob].copy()
    if df.empty:
        eprint("::error::Após normalização, não há probabilidades válidas (0..1) para calcular Kelly.")
        sys.exit(EXIT_CODE)

    # Se odds ausentes, aproxima por fair odds (1/p)
    for side in ["home","draw","away"]:
        oc = f"odds_{side}"
        pc = f"prob_{side}"
        if oc not in df.columns:
            df[oc] = 1.0 / df[pc]
        else:
            # completa NaN com fair odds
            df[oc] = df[oc].where(df[oc].notna() & (df[oc] > 1.0), 1.0 / df[pc])

    # Escolhe pick por linha
    picks = []
    for _, r in df.iterrows():
        pick = choose_pick_row(r)
        if pick == "H":
            prob = r["prob_home"]; odds = r["odds_home"]
        elif pick == "D":
            prob = r["prob_draw"]; odds = r["odds_draw"]
        else:
            prob = r["prob_away"]; odds = r["odds_away"]

        b = max(odds - 1.0, 1e-12)
        q = 1.0 - prob
        k_raw = (b * prob - q) / b  # Kelly teórico
        # edge (esperado) = b*p - q
        edge = b * prob - q

        picks.append({
            "match_id": r["match_id"],
            "team_home": r["team_home"],
            "team_away": r["team_away"],
            "pick": pick,
            "prob": prob,
            "odds": odds,
            "edge": edge,
            "kelly_raw": k_raw
        })

    out = pd.DataFrame(picks)

    # aplica fração, cap e bankroll
    out["kelly_raw"] = pd.to_numeric(out["kelly_raw"], errors="coerce").fillna(0.0)
    # valores negativos -> 0 (não apostar)
    out.loc[out["kelly_raw"] < 0, "kelly_raw"] = 0.0

    # cap por aposta
    if kelly_cap > 0:
        out["kelly_raw"] = out["kelly_raw"].clip(upper=kelly_cap)

    # stake $$ = bankroll * kelly_fraction * kelly_raw
    out["stake"] = bankroll * kelly_fraction * out["kelly_raw"]

    # arredonda
    if round_to and round_to > 0:
        out["stake"] = (out["stake"] / round_to).round() * round_to

    # ordena por stake desc e aplica TOP_N
    out = out.sort_values(["stake","edge","prob"], ascending=[False, False, False]).reset_index(drop=True)
    if top_n and top_n > 0:
        out = out.head(top_n)

    out_path = os.path.join(out_dir, "kelly_stakes.csv")
    out.to_csv(out_path, index=False)

    if not os.path.isfile(out_path) or os.path.getsize(out_path) == 0:
        eprint("::error::kelly_stakes.csv não gerado")
        sys.exit(EXIT_CODE)

    if args.debug:
        eprint(f"[kelly] Fonte usada: {picked_source}")
        eprint(f"[kelly] Gerado: {out_path}  linhas={len(out)}")

if __name__ == "__main__":
    main()