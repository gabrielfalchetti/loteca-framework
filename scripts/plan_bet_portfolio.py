# scripts/plan_bet_portfolio.py
# Portfólio de cartões: gera N tickets complementares, respeitando limites de duplos/triplos,
# maximizando prob(≥1 acerta 14/14) por ganho marginal via simulação.
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)

# ----------------- util de probabilidades -----------------
def _from_odds(arr_odds: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0 / arr_odds.astype(float)
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum(axis=1, keepdims=True)
    P = np.divide(inv, np.where(s > 0, s, 1.0))
    return P

def _ensure_probs(df: pd.DataFrame) -> tuple[np.ndarray, list[int]]:
    """
    Retorna P (n,3) garantidamente sem NaN e normalizado.
    Tenta nesta ordem:
      1) p_home/p_draw/p_away
      2) odd_home/odd_draw/odd_away -> 1/odd e normaliza
      3) fallback uniforme (1/3,1/3,1/3)
    Também retorna a lista de match_id que precisaram de fallback.
    """
    fallback_ids = []

    has_p = {"p_home","p_draw","p_away"}.issubset(df.columns)
    has_o = {"odd_home","odd_draw","odd_away"}.issubset(df.columns)

    if has_p:
        P = df[["p_home","p_draw","p_away"]].values.astype(float)
    elif has_o:
        P = _from_odds(df[["odd_home","odd_draw","odd_away"]].values)
        fallback_ids = list(df["match_id"].astype(int).values)
    else:
        # nada disponível: tudo uniforme
        n = len(df)
        P = np.full((n,3), 1/3.0, dtype=float)
        fallback_ids = list(df["match_id"].astype(int).values)

    # limpa NaN/infs por linha
    rows_to_fix = np.unique(
        np.where(~np.isfinite(P))[0]
    ).tolist()

    if rows_to_fix and has_o:
        # tenta usar odds para essas linhas específicas
        odds = df.loc[rows_to_fix, ["odd_home","odd_draw","odd_away"]].values.astype(float) if has_o else None
        if odds is not None:
            P_fix = _from_odds(odds)
            P[rows_to_fix,:] = P_fix
            fallback_ids += list(df.loc[rows_to_fix, "match_id"].astype(int).values)

    # ainda restou NaN? aplica uniforme na(s) linha(s)
    rows_to_fix = np.unique(
        np.where(~np.isfinite(P))[0]
    ).tolist()
    if rows_to_fix:
        P[rows_to_fix,:] = np.array([1/3.0,1/3.0,1/3.0], dtype=float)
        fallback_ids += list(df.loc[rows_to_fix, "match_id"].astype(int).values)

    # clip & renormaliza
    P = np.clip(P, 1e-9, 1.0)
    s = P.sum(axis=1, keepdims=True)
    P = P / np.where(s > 0, s, 1.0)

    # sanidade final
    if not np.isfinite(P).all():
        raise RuntimeError("[portfolio] probabilidades inválidas mesmo após saneamento.")

    # dedup ids na ordem de aparição
    seen = set(); clean_fallback = []
    for mid in fallback_ids:
        if mid not in seen:
            seen.add(mid); clean_fallback.append(mid)

    return P, clean_fallback

# ----------------- seleção e simulação -----------------
def _pick_joined(base: Path) -> Path:
    for name in ["joined_referee.csv","joined_weather.csv","joined_enriched.csv","joined.csv","odds.csv"]:
        p = base / name
        if p.exists() and p.stat().st_size > 0:
            return p
    raise RuntimeError("[portfolio] nenhum joined*/odds.csv encontrado.")

def _simulate_hits(P: np.ndarray, picks: list[set[str]], n_sims=20000) -> float:
    """
    P: (n_matches, 3) probas [1,X,2]
    picks: lista de sets com outcomes permitidos por jogo {"1"}, {"1","X"}, {"1","X","2"}
    Retorna prob de 14/14 sob simulação.
    """
    n = len(picks)
    draws = np.zeros((n_sims, n), dtype=np.int8)
    for i in range(n):
        draws[:, i] = RNG.choice(3, size=n_sims, p=P[i])
    idx_map = {"1":0,"X":1,"2":2}
    ok = np.ones(n_sims, dtype=bool)
    for j in range(n):
        allow_idx = {idx_map[a] for a in picks[j]}
        ok &= np.isin(draws[:,j], list(allow_idx))
        if not ok.any():
            return 0.0
    return float(ok.mean())

def _entropy_row(p3: np.ndarray) -> float:
    p = np.clip(p3, 1e-9, 1.0)
    p = p / p.sum()
    return float(-(p*np.log(p)).sum())

def _baseline_ticket(P: np.ndarray) -> list[set[str]]:
    labels = np.argmax(P, axis=1)
    map_out = {0:"1",1:"X",2:"2"}
    return [{map_out[i]} for i in labels]

def _expand_ticket(P: np.ndarray, ticket: list[set[str]], max_duplos: int, max_triplos: int) -> list[set[str]]:
    n = len(ticket)
    ent = np.array([_entropy_row(P[i]) for i in range(n)])
    order = np.argsort(-ent)  # maior entropia primeiro
    d_used = 0; t_used = 0
    for idx in order:
        if d_used < max_duplos and len(ticket[idx]) == 1:
            sorted_idx = np.argsort(-P[idx])  # 0,1,2 (desc)
            best, second = sorted_idx[0], sorted_idx[1]
            map_out = {0:"1",1:"X",2:"2"}
            ticket[idx] = {map_out[best], map_out[second]}
            d_used += 1
            continue
        if t_used < max_triplos and len(ticket[idx]) < 3:
            ticket[idx] = {"1","X","2"}
            t_used += 1
    return ticket

def _marginal_gain(P: np.ndarray, portfolio: list[list[set[str]]], candidate: list[set[str]], sims: int):
    def p14(ticket):
        return _simulate_hits(P, ticket, n_sims=sims)
    existing = [p14(t) for t in portfolio]
    base = 1.0 - np.prod([1.0 - p for p in existing]) if existing else 0.0
    cand = p14(candidate)
    new = 1.0 - np.prod([1.0 - p for p in (existing + [cand])])
    return (new - base), cand

# ----------------- main -----------------
def main():
    ap = argparse.ArgumentParser(description="Gerador de portfólio de cartões")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--n-tickets", required=False, default="5")  # pode vir como string do workflow
    ap.add_argument("--max-duplos", type=int, default=4)
    ap.add_argument("--max-triplos", type=int, default=2)
    ap.add_argument("--sims-eval", type=int, default=25000)
    args = ap.parse_args()

    # n-tickets pode chegar como string
    try:
        n_tickets = int(str(args.n_tickets).strip().replace('"','').replace("'", ""))
    except Exception:
        n_tickets = 5

    base = Path(f"data/out/{args.rodada}")
    src = _pick_joined(base)
    df = pd.read_csv(src)

    need_id = {"match_id"}
    if not need_id.issubset(df.columns):
        raise RuntimeError("[portfolio] joined/odds.csv precisa da coluna match_id.")

    # garante probabilidades válidas
    P, fixed_ids = _ensure_probs(df)
    if fixed_ids:
        print("[portfolio] Aviso: probabilidades reconstruídas para match_id:", fixed_ids)

    # ticket baseline + expansão + ganho marginal
    portfolio = []
    summary = []
    for k in range(n_tickets):
        t0 = _baseline_ticket(P)
        tk = _expand_ticket(P, t0, args.max_duplos, args.max_triplos)
        gain, p14 = _marginal_gain(P, portfolio, tk, sims=args.sims_eval)
        portfolio.append(tk)
        summary.append({"ticket": k+1, "p14": round(p14, 8), "marginal_gain": round(gain, 8)})

    # salvar
    outdir = base / "portfolio"
    outdir.mkdir(parents=True, exist_ok=True)

    for i, t in enumerate(portfolio, start=1):
        rows = []
        for j, allow in enumerate(t, start=1):
            picks = "".join(sorted(list(allow), key=lambda x: {"1":0,"X":1,"2":2}[x]))
            ph, px, pa = P[j-1, 0], P[j-1, 1], P[j-1, 2]
            row = {
                "match_id": int(df.loc[j-1, "match_id"]),
                "home": str(df.loc[j-1, "home"]) if "home" in df.columns else "",
                "away": str(df.loc[j-1, "away"]) if "away" in df.columns else "",
                "pick": picks,
                "tipo": {1:"SECO",2:"DUPLO",3:"TRIPLO"}[len(allow)],
                "probs_h|x|a": f"{ph:.3f}|{px:.3f}|{pa:.3f}"
            }
            rows.append(row)
        pd.DataFrame(rows).to_csv(outdir / f"cartao_{i:03d}.csv", index=False)

    pd.DataFrame(summary).to_csv(outdir / "portfolio_summary.csv", index=False)
    print(f"[portfolio] {len(portfolio)} cartões salvos em {outdir}")
    for s in summary:
        print(f"  - ticket {s['ticket']:03d}: p14={s['p14']} (marginal +{s['marginal_gain']})")

if __name__ == "__main__":
    main()
