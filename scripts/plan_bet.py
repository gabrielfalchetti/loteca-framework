# scripts/plan_bet.py
# Gera um cartão da Loteca a partir de joined.csv respeitando limites:
#   --max-duplos (default 4) e --max-triplos (default 2).
# Saída: data/out/<rodada>/cartao.csv

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

REQUIRED_COLS = ["match_id", "home", "away", "odd_home", "odd_draw", "odd_away"]

LABELS = np.array(["1", "X", "2"])

def softmax_from_odds(odd_home: float, odd_draw: float, odd_away: float) -> np.ndarray:
    """
    Converte odds decimais (home, draw, away) em probabilidades normalizadas.
    Usamos 1/odd e normalizamos (equivale a "desvigar" em primeira ordem).
    """
    arr = np.array([odd_home, odd_draw, odd_away], dtype=float)
    # protege contra zeros/NaN
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0 / arr
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s <= 0:
        return np.array([1/3, 1/3, 1/3], dtype=float)
    return inv / s

def entropy(p: np.ndarray) -> float:
    p = np.clip(p, 1e-12, 1.0)
    return float(-np.sum(p * np.log(p)))

def choose_duplo(probs: np.ndarray) -> str:
    """Retorna duplo cobrindo os dois resultados mais prováveis (ex.: '1X', '12' ou 'X2')."""
    idx_sorted = np.argsort(probs)  # crescente
    top2 = idx_sorted[-2:]          # dois maiores
    # ordenar pelo rótulo preferencial: manter ordem natural 1,X,2? Melhor pela probabilidade decrescente:
    top2 = top2[np.argsort(probs[top2])[::-1]]
    return "".join(LABELS[top2])

def main():
    ap = argparse.ArgumentParser(description="Planeja cartão (até N duplos, M triplos) a partir de joined.csv")
    ap.add_argument("--rodada", required=True, help="Identificador da rodada (ex.: 2025-10-05_14)")
    ap.add_argument("--max-duplos", type=int, default=4, dest="max_duplos")
    ap.add_argument("--max-triplos", type=int, default=2, dest="max_triplos")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    joined_path = base / "joined.csv"
    if not joined_path.exists() or joined_path.stat().st_size == 0:
        raise RuntimeError(f"[plan_bet] joined.csv ausente/vazio: {joined_path}")

    df = pd.read_csv(joined_path)
    # Validação de colunas
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"[plan_bet] joined.csv sem colunas requeridas: {missing}")

    # Filtra somente jogos com odds válidas
    df_ok = df.dropna(subset=["odd_home", "odd_draw", "odd_away"]).copy()
    if df_ok.empty:
        raise RuntimeError("[plan_bet] Nenhum jogo com odds válidas em joined.csv")

    # Calcula probabilidades e entropia
    probs_list = []
    ent_list = []
    for i, r in df_ok.iterrows():
        p = softmax_from_odds(r["odd_home"], r["odd_draw"], r["odd_away"])
        probs_list.append(p)
        ent_list.append(entropy(p))
    df_ok["p_home"] = [p[0] for p in probs_list]
    df_ok["p_draw"] = [p[1] for p in probs_list]
    df_ok["p_away"] = [p[2] for p in probs_list]
    df_ok["entropy"] = ent_list

    # Ordena por imprevisibilidade (entropia) decrescente
    df_sorted = df_ok.sort_values("entropy", ascending=False).reset_index(drop=True)

    # Ajusta limites para o número de jogos disponíveis
    n_games = len(df_sorted)
    max_triplos = max(0, min(args.max_triplos, n_games))
    max_duplos = max(0, min(args.max_duplos, max(0, n_games - max_triplos)))

    # Seleciona TRIPLOS primeiro (mais incertos)
    triplo_ids = set(df_sorted.head(max_triplos)["match_id"].astype(int).tolist())

    # Seleciona DUPLOS nos próximos mais incertos (excluindo os já triplos)
    rem = df_sorted[~df_sorted["match_id"].isin(triplo_ids)]
    duplo_ids = set(rem.head(max_duplos)["match_id"].astype(int).tolist())

    # Define os picks
    picks = {}
    types = {}
    probs_fmt = {}
    for _, r in df_ok.iterrows():
        mid = int(r["match_id"])
        p = np.array([r["p_home"], r["p_draw"], r["p_away"]], dtype=float)
        # formata probs como 0.000
        probs_fmt[mid] = f"{p[0]:.3f}|{p[1]:.3f}|{p[2]:.3f}"
        if mid in triplo_ids:
            picks[mid] = "123"   # cobre 1, X e 2
            types[mid] = "TRIPLO"
        elif mid in duplo_ids:
            picks[mid] = choose_duplo(p)
            types[mid] = "DUPLO"
        else:
            idx = int(np.argmax(p))
            picks[mid] = LABELS[idx]
            types[mid] = "SECO"

    # Monta saída preservando a ordem original do joined.csv
    out_rows = []
    for _, r in df.iterrows():
        mid = int(r["match_id"])
        # Se o jogo não tinha odds (foi filtrado), marcamos como '?' para você decidir manualmente
        if mid not in picks:
            pick = "?"
            typ = "SEM_ODDS"
            pfm = ""
        else:
            pick = picks[mid]
            typ = types[mid]
            pfm = probs_fmt[mid]
        out_rows.append({
            "match_id": mid,
            "home": r.get("home", ""),
            "away": r.get("away", ""),
            "pick": pick,
            "tipo": typ,
            "probs_h|x|a": pfm
        })

    out = pd.DataFrame(out_rows)
    out_path = base / "cartao.csv"
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[plan_bet] Cartão salvo em {out_path}")
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
