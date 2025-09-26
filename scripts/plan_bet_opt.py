# scripts/plan_bet_opt.py
# Otimizador de cartão Loteca:
# - lê data/out/<rodada>/joined.csv (com odd_home, odd_draw, odd_away)
# - escolhe SECO/ DUPLO / TRIPLO por jogo para maximizar P(14/14)
#   sob restrições: --max-duplos, --max-triplos
# - saída: data/out/<rodada>/cartao.csv (match_id, home, away, pick, tipo, probs_h|x|a)
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

LABELS = np.array(["1","X","2"], dtype=object)

def probs_from_odds(oh, od, oa):
    arr = np.array([float(oh), float(od), float(oa)], dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0/arr
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s <= 0:
        return np.array([1/3,1/3,1/3], dtype=float)
    return inv/s

def solve_opt(p_matrix: np.ndarray, max_duplos: int, max_triplos: int):
    """
    p_matrix: shape (N,3) com probabilidades [home,draw,away] para cada jogo.
    retorna arrays tipo (N,), picks (string) e tipo (SECO/DUPLO/TRIPLO/SEM_ODDS)
    Estratégia ótima "quase-exata":
      1) compute ganho marginal de cobrir 2º e 3º resultados por jogo:
         - prob_seco = max(p)
         - prob_duplo = prob_seco + segundo_maior(p)
         - prob_triplo = 1.0
         ganhos: g2 = prob_duplo - prob_seco ; g3 = prob_triplo - prob_duplo
      2) escolha TRIPLOS nos maiores g3 (até max_triplos)
      3) entre os restantes, escolha DUPLOS nos maiores g2 (até max_duplos)
      4) demais ficam SECOS
    Objetivo verdadeiro é produto das probabilidades cobertas.
    Essa heurística de ganhos marginais se aproxima muito do ótimo e é estável.
    """
    N = p_matrix.shape[0]
    probs = p_matrix.copy()
    # lida com linhas inválidas (sem odds): marca SEM_ODDS
    mask_valid = np.isfinite(probs).all(axis=1) & (probs.sum(axis=1) > 0)
    picks = np.array(["?"]*N, dtype=object)
    tipos = np.array(["SEM_ODDS"]*N, dtype=object)

    if not mask_valid.any():
        return picks, tipos  # tudo sem odds

    # para válidos: calcula estatísticas
    p_valid = probs[mask_valid]
    # ordena probabilidades por jogo
    idx_sorted = np.argsort(p_valid, axis=1)  # crescente
    top1_idx = idx_sorted[:, -1]
    top2_idx = idx_sorted[:, -2]

    prob_seco = p_valid[np.arange(p_valid.shape[0]), top1_idx]
    prob_duplo = prob_seco + p_valid[np.arange(p_valid.shape[0]), top2_idx]
    # triplo cobre 100%
    gain_duplo = prob_duplo - prob_seco
    gain_triplo = 1.0 - prob_duplo

    # ranks globais
    order_triplo = np.argsort(gain_triplo)[::-1]
    choose_triplo_local = np.zeros(p_valid.shape[0], dtype=bool)
    if max_triplos > 0:
        choose_triplo_local[order_triplo[:max_triplos]] = True

    # para duplos, não usar os já triplos
    gain_duplo_masked = gain_duplo.copy()
    gain_duplo_masked[choose_triplo_local] = -1.0
    order_duplo = np.argsort(gain_duplo_masked)[::-1]
    choose_duplo_local = np.zeros(p_valid.shape[0], dtype=bool)
    if max_duplos > 0:
        choose_duplo_local[order_duplo[:max_duplos]] = True

    # construir picks/tipos
    # mapeia de "válidos" de volta para indices globais
    valid_idx = np.where(mask_valid)[0]
    for j_local, j_global in enumerate(valid_idx):
        p = p_valid[j_local]
        i1 = int(top1_idx[j_local])
        i2 = int(top2_idx[j_local])
        if choose_triplo_local[j_local]:
            pick = "123"; tipo = "TRIPLO"
        elif choose_duplo_local[j_local]:
            # mantemos ordem decrescente de prob: ex: "1X", "12" ou "X2"
            pair = [i1, i2]
            # ordenar pelo valor p descrescente
            pair = sorted(pair, key=lambda k: p[k], reverse=True)
            pick = "".join(LABELS[pair])
            tipo = "DUPLO"
        else:
            pick = LABELS[i1]
            tipo = "SECO"
        picks[j_global] = pick
        tipos[j_global] = tipo

    return picks, tipos

def main():
    ap = argparse.ArgumentParser(description="Otimizador de cartão Loteca (max P(14/14) com limite de duplos/triplos)")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-10-05_14")
    ap.add_argument("--max-duplos", type=int, default=4, dest="max_duplos")
    ap.add_argument("--max-triplos", type=int, default=2, dest="max_triplos")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    joined_path = base / "joined.csv"
    if not joined_path.exists() or joined_path.stat().st_size == 0:
        raise RuntimeError(f"[plan_bet_opt] joined.csv ausente/vazio: {joined_path}")

    df = pd.read_csv(joined_path)
    for c in ["match_id","home","away","odd_home","odd_draw","odd_away"]:
        if c not in df.columns:
            raise RuntimeError(f"[plan_bet_opt] joined.csv faltando coluna: {c}")

    # monta matriz de probabilidades
    probs = []
    for _, r in df.iterrows():
        if pd.isna(r["odd_home"]) or pd.isna(r["odd_draw"]) or pd.isna(r["odd_away"]):
            probs.append([np.nan, np.nan, np.nan])
        else:
            probs.append(probs_from_odds(r["odd_home"], r["odd_draw"], r["odd_away"]))
    P = np.array(probs, dtype=float)

    picks, tipos = solve_opt(P, args.max_duplos, args.max_triplos)

    # formatar saída
    def fmt(p):
        if not np.isfinite(p).all(): return ""
        return f"{p[0]:.3f}|{p[1]:.3f}|{p[2]:.3f}"

    out_rows=[]
    for i, r in df.iterrows():
        pr = P[i]
        out_rows.append({
            "match_id": int(r["match_id"]),
            "home": r["home"],
            "away": r["away"],
            "pick": picks[i],
            "tipo": tipos[i],
            "probs_h|x|a": fmt(pr)
        })
    out = pd.DataFrame(out_rows)
    out_path = base / "cartao.csv"
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[plan_bet_opt] Cartão salvo em {out_path}")
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
