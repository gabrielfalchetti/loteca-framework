#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera apostas pelo critério de Kelly com dados 100% reais (sem placeholders).

Fontes aceitas (em ordem de prioridade), todas dentro de <RODADA>:
1) calibrated_probs.csv            -> precisa de p_home,p_draw,p_away
2) predictions_final.csv           -> p_* OU odds_*
3) predictions_blend.csv           -> p_* OU odds_*   (opcional, se sua pipeline gera)
4) predictions_market.csv          -> odds_*
5) odds_consensus.csv              -> odds_*

Saída:
- <RODADA>/kelly_stakes.csv

Parâmetros (flags ou variáveis de ambiente):
--bankroll          (BANKROLL, default 1000)
--kelly_fraction    (KELLY_FRACTION, default 0.5)
--kelly_cap         (KELLY_CAP, default 0.1)
--top_n             (KELLY_TOP_N, default 14)
--round_to          (ROUND_TO, default 1)

Falha dura se:
- Nenhuma fonte válida encontrada
- Odds inválidas (NaN ou <= 1.0) quando necessárias
- Probabilidades inconsistentes

Este script NÃO inventa dados.
"""

import argparse
import os
import sys
import math
import pandas as pd

REQUIRED_TEAM_COLS = ["match_id", "team_home", "team_away"]
P_COLS = ["p_home", "p_draw", "p_away"]
O_COLS = ["odds_home", "odds_draw", "odds_away"]

def die(msg: str, code: int = 25):
    print(f"[kelly][ERRO] {msg}", file=sys.stderr)
    sys.exit(code)

def read_csv_safe(path: str) -> pd.DataFrame | None:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    try:
        df = pd.read_csv(path)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None

def has_cols(df: pd.DataFrame, cols: list[str]) -> bool:
    return set(cols).issubset(df.columns)

def validate_team_cols(df: pd.DataFrame, src: str):
    # Normaliza possíveis nomes home/away
    if "home" in df.columns and "away" in df.columns and "team_home" not in df.columns:
        df = df.rename(columns={"home": "team_home", "away": "team_away"})
    missing = set(REQUIRED_TEAM_COLS) - set(df.columns)
    if missing:
        die(f"{src} sem colunas mínimas {REQUIRED_TEAM_COLS}. Faltando: {sorted(missing)}")
    return df

def odds_to_probs(df: pd.DataFrame, src: str) -> pd.DataFrame:
    if not has_cols(df, O_COLS):
        die(f"{src} não possui odds_* completas para inferir probabilidades.")
    # Valida odds
    bad_mask = (
        df["odds_home"].isna() | df["odds_draw"].isna() | df["odds_away"].isna() |
        (df["odds_home"] <= 1.0) | (df["odds_draw"] <= 1.0) | (df["odds_away"] <= 1.0)
    )
    if bad_mask.any():
        bad = df.loc[bad_mask, ["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]].head(10)
        die(f"Odds inválidas detectadas em {src} (NaN ou <=1.0). Amostra:\n{bad.to_string(index=False)}", code=98)

    # Implied probs (sem overround fix por padrão; pode-se normalizar)
    p_h = 1.0 / df["odds_home"].astype(float)
    p_d = 1.0 / df["odds_draw"].astype(float)
    p_a = 1.0 / df["odds_away"].astype(float)
    total = p_h + p_d + p_a
    # normaliza para somar 1
    df["p_home"] = p_h / total
    df["p_draw"] = p_d / total
    df["p_away"] = p_a / total
    return df

def ensure_probs(df: pd.DataFrame, src: str) -> pd.DataFrame:
    # Aceita p_* ou converte de odds_*
    if has_cols(df, P_COLS):
        # valida [0,1] e soma
        for c in P_COLS:
            if df[c].isna().any():
                die(f"{src} contém NaN em {c}.", code=98)
            if (df[c] < 0).any() or (df[c] > 1).any():
                die(f"{src} contém {c} fora do intervalo [0,1].", code=98)
        s = (df["p_home"] + df["p_draw"] + df["p_away"]).round(6)
        if (s <= 0).any():
            die(f"{src} com soma de probabilidades <= 0.", code=98)
        # não força soma==1; mas normaliza levemente para robustez
        df["p_home"] = df["p_home"] / s
        df["p_draw"] = df["p_draw"] / s
        df["p_away"] = df["p_away"] / s
        return df
    # caso contrário, espera odds_*
    return odds_to_probs(df, src)

def load_best_source(rodada: str) -> tuple[pd.DataFrame, str]:
    candidates = [
        ("calibrated_probs.csv", True),
        ("predictions_final.csv", False),
        ("predictions_blend.csv", False),
        ("predictions_market.csv", False),
        ("odds_consensus.csv", False),
    ]
    for fname, expect_probs in candidates:
        path = os.path.join(rodada, fname)
        df = read_csv_safe(path)
        if df is None:
            continue
        df = validate_team_cols(df, fname)
        # Padroniza nomes odds* se vierem com outra grafia em alguns pipelines
        # (não cria dados do nada; apenas renomeia colunas comuns)
        rename_map = {}
        for col in df.columns:
            lc = col.lower().strip()
            if lc.startswith("prob_home"): rename_map[col] = "p_home"
            if lc.startswith("prob_draw"): rename_map[col] = "p_draw"
            if lc.startswith("prob_away"): rename_map[col] = "p_away"
        if rename_map:
            df = df.rename(columns=rename_map)

        if expect_probs and not has_cols(df, P_COLS):
            # Mesmo em calibrated_probs.csv, se não houver p_*, não usamos
            continue

        # Garante probs (converte de odds_* quando preciso)
        df = ensure_probs(df, fname)

        # Mantém também odds_* caso já existam (senão, gera "fair odds" depois)
        src = fname
        return df, src

    die("Nenhum arquivo de probabilidades disponível (final/blend/market/consensus/calibrated).")

def pick_best_kelly_for_match(row: pd.Series, bankroll: float, frac: float, cap: float) -> dict | None:
    """
    Calcula Kelly para cada desfecho usando odds de mercado se presentes; caso não haja,
    usa odds justas (1/p) apenas para ranqueamento — mas **sem stake** se não houver odds reais.
    Política aqui: stake só é permitido se houver odds_* reais na linha; caso contrário, stake=0.
    """
    results = []
    has_market_odds = all([(c in row and isinstance(row[c], (int, float))) for c in O_COLS])

    options = [
        ("home", row["p_home"], row.get("odds_home", float("nan"))),
        ("draw", row["p_draw"], row.get("odds_draw", float("nan"))),
        ("away", row["p_away"], row.get("odds_away", float("nan"))),
    ]

    for sel, p, o in options:
        if p is None or not (0 <= p <= 1) or math.isnan(p):
            continue

        if has_market_odds and (o is not None) and (not math.isnan(o)) and o > 1.0:
            # Kelly com odds de mercado (decimal)
            b = o - 1.0
            q = 1.0 - p
            k = (p * o - 1.0) / b  # fração de Kelly
            edge = p * o - 1.0
            stake = 0.0
            k_eff = max(0.0, k)
            if k_eff > 0:
                k_eff = min(k_eff, cap)
                stake = bankroll * frac * k_eff
            results.append({
                "selection": sel,
                "prob": p,
                "odds": o,
                "kelly_fraction": k_eff,
                "stake": stake,
                "edge": edge,
            })
        else:
            # Sem odds reais -> não apostar; apenas registrar fair odds p/ debug
            fair_o = float("inf") if p == 0 else (1.0 / p)
            results.append({
                "selection": sel,
                "prob": p,
                "odds": float("nan"),
                "kelly_fraction": 0.0,
                "stake": 0.0,
                "edge": float("nan"),
            })

    if not results:
        return None

    # Escolhe a melhor por kelly_fraction (ou por edge se empatar)
    results.sort(key=lambda d: (d["kelly_fraction"], (d["edge"] if not math.isnan(d["edge"]) else -1e9)), reverse=True)
    best = results[0]
    # Se não tiver stake possível (sem odds reais), retorna mesmo assim (stake=0)
    return best

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada")
    ap.add_argument("--bankroll", type=float, default=float(os.getenv("BANKROLL", "1000")))
    ap.add_argument("--kelly_fraction", type=float, default=float(os.getenv("KELLY_FRACTION", "0.5")))
    ap.add_argument("--kelly_cap", type=float, default=float(os.getenv("KELLY_CAP", "0.1")))
    ap.add_argument("--top_n", type=int, default=int(os.getenv("KELLY_TOP_N", "14")))
    ap.add_argument("--round_to", type=float, default=float(os.getenv("ROUND_TO", "1")))
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    bankroll = args.bankroll
    frac = args.kelly_fraction
    cap = args.kelly_cap
    top_n = args.top_n
    rnd = args.round_to

    df, src = load_best_source(rodada)
    if args.debug:
        print(f"[kelly] Fonte usada: {src} ({len(df)} linhas)")

    # Garante colunas team_home/team_away
    df = validate_team_cols(df, src)

    # Mantém odds_* apenas se vieram juntos; NÃO inventamos odds
    have_market_odds = has_cols(df, O_COLS)

    # Calcula melhor seleção por jogo
    picks = []
    for _, row in df.iterrows():
        best = pick_best_kelly_for_match(row, bankroll, frac, cap)
        if best is None:
            continue
        rec = {
            "match_id": row["match_id"],
            "team_home": row["team_home"],
            "team_away": row["team_away"],
            "selection": best["selection"],
            "prob": best["prob"],
            "odds": best["odds"],
            "kelly_fraction": best["kelly_fraction"],
            "stake": best["stake"],
            "edge": best["edge"],
            "source": src,
        }
        picks.append(rec)

    if not picks:
        die("Nenhuma aposta elegível (todas sem odds reais ou probabilidades inválidas).")

    out = pd.DataFrame(picks)

    # Se todas stakes forem 0, ainda assim salvamos (mas avisamos)
    if (out["stake"] <= 0).all():
        print("[kelly][AVISO] Nenhuma stake positiva calculada (provável ausência de odds_* reais na fonte usada).", file=sys.stderr)

    # Ordena por stake desc (se todas 0, ordena por prob desc como fallback)
    if (out["stake"] > 0).any():
        out = out.sort_values(["stake", "edge"], ascending=[False, False])
    else:
        out = out.sort_values(["prob"], ascending=False)

    # top N
    if top_n and top_n > 0:
        out = out.head(top_n).copy()

    # arredondamento de stake
    if rnd and rnd > 0:
        out["stake"] = out["stake"].apply(lambda x: (math.floor(x / rnd + 0.5) * rnd) if x > 0 else 0)

    out_path = os.path.join(rodada, "kelly_stakes.csv")
    out.to_csv(out_path, index=False)

    if args.debug:
        print(f"[kelly] config: bankroll={bankroll}, frac={frac}, cap={cap}, top_n={top_n}, round_to={rnd}")
        print(f"[kelly] gravado: {out_path} ({len(out)} linhas)")
        print(out.head(20).to_string(index=False))


if __name__ == "__main__":
    main()