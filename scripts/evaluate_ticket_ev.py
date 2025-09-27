# scripts/evaluate_ticket_ev.py
# EV + Kelly para cartões do portfólio (ou qualquer diretório de cartao_*.csv)
from __future__ import annotations
import argparse, yaml
from pathlib import Path
import numpy as np
import pandas as pd

RNG = np.random.default_rng(123)

DEFAULT_PRIZE = {
    "cost_per_ticket": 1.5,     # custo do cartão (ajuste p/ seu valor real)
    "payout_14": 500000.0,      # prêmio esperado 14 acertos (ajuste conforme histórico)
    "payout_13": 1200.0,        # prêmio médio 13 acertos
    "kelly_fraction": 0.25      # fração de Kelly (25%)
}

# ---------------- Probabilidades: saneamento robusto ----------------
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
    Ordem de tentativa:
      1) p_home/p_draw/p_away
      2) odd_home/odd_draw/odd_away -> 1/odd e normaliza
      3) fallback uniforme (1/3,1/3,1/3) por linha problemática
    Também retorna lista de match_id que precisaram de fallback.
    """
    fallback_ids: list[int] = []
    has_p = {"p_home","p_draw","p_away"}.issubset(df.columns)
    has_o = {"odd_home","odd_draw","odd_away"}.issubset(df.columns)

    if has_p:
        P = df[["p_home","p_draw","p_away"]].values.astype(float)
    elif has_o:
        P = _from_odds(df[["odd_home","odd_draw","odd_away"]].values)
        fallback_ids = list(df["match_id"].astype(int).values)
    else:
        n = len(df)
        P = np.full((n,3), 1/3.0, dtype=float)
        fallback_ids = list(df["match_id"].astype(int).values)

    # tenta corrigir linhas com NaN/inf usando odds (se houver)
    rows_bad = np.unique(np.where(~np.isfinite(P))[0]).tolist()
    if rows_bad and has_o:
        odds = df.loc[rows_bad, ["odd_home","odd_draw","odd_away"]].values.astype(float)
        P_fix = _from_odds(odds)
        P[rows_bad,:] = P_fix
        fallback_ids += list(df.loc[rows_bad, "match_id"].astype(int).values)

    # se ainda houver NaN/inf, aplica uniforme na(s) linha(s)
    rows_bad = np.unique(np.where(~np.isfinite(P))[0]).tolist()
    if rows_bad:
        P[rows_bad,:] = np.array([1/3.0,1/3.0,1/3.0], dtype=float)
        fallback_ids += list(df.loc[rows_bad, "match_id"].astype(int).values)

    # clamp + normalização
    P = np.clip(P, 1e-9, 1.0)
    s = P.sum(axis=1, keepdims=True)
    P = P / np.where(s > 0, s, 1.0)

    # dedup da lista de fallback
    seen=set(); clean=[]
    for mid in fallback_ids:
        if mid not in seen:
            seen.add(mid); clean.append(mid)
    return P, clean

# ---------------- Leitura de base & simulação ----------------
def load_joined(base: Path) -> tuple[pd.DataFrame, np.ndarray, list[int]]:
    # procura joined enriquecidos, depois odds.csv
    for name in ["joined_referee.csv","joined_weather.csv","joined_enriched.csv","joined.csv","odds.csv"]:
        p = base/name
        if p.exists() and p.stat().st_size>0:
            df = pd.read_csv(p)
            if "match_id" not in df.columns:
                raise RuntimeError(f"[ev] arquivo {name} não possui match_id.")
            P, fixed = _ensure_probs(df)
            return df, P, fixed
    raise RuntimeError("[ev] nenhum joined*/odds.csv encontrado.")

def simulate_outcomes(P: np.ndarray, n_sims=60000) -> np.ndarray:
    n = P.shape[0]
    out = np.zeros((n_sims, n), dtype=np.int8)
    for i in range(n):
        out[:,i] = RNG.choice(3, size=n_sims, p=P[i])
    return out

def eval_ticket(outcomes: np.ndarray, ticket_picks: list[set[str]]) -> tuple[float,float]:
    """
    outcomes: (n_sims, n) com valores 0/1/2
    ticket_picks: lista de sets {"1","X","2"} por jogo
    retorna: (p14, p13)
    """
    idx_map = {"1":0,"X":1,"2":2}
    allow = [ set(idx_map[a] for a in s) for s in ticket_picks ]

    # 14 acertos
    ok = np.ones(outcomes.shape[0], dtype=bool)
    for j, al in enumerate(allow):
        ok &= np.isin(outcomes[:,j], list(al))
        if not ok.any():  # curto-circuito
            return 0.0, 0.0
    p14 = float(ok.mean())

    # 13 acertos (erra exatamente 1 jogo)
    n = outcomes.shape[1]
    p13 = 0.0
    for miss in range(n):
        ok13 = np.ones(outcomes.shape[0], dtype=bool)
        for j, al in enumerate(allow):
            if j == miss:
                ok13 &= ~np.isin(outcomes[:,j], list(al))
            else:
                ok13 &=  np.isin(outcomes[:,j], list(al))
            if not ok13.any(): break
        p13 += ok13.mean()
    return p14, p13

def parse_ticket_csv(path: Path) -> list[set[str]]:
    df = pd.read_csv(path)
    picks=[]
    for _, r in df.iterrows():
        s = str(r["pick"]).upper().strip()
        picks.append(set(list(s)))
    return picks

def load_prize_model(path="config/prize_model.yml") -> dict:
    p = Path(path)
    if p.exists() and p.stat().st_size>0:
        with open(p, "r") as f:
            return yaml.safe_load(f)
    return DEFAULT_PRIZE.copy()

# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser(description="Avaliação de EV e Kelly para cartões")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--portfolio-dir", default=None, help="diretório com cartao_*.csv; se vazio, usa data/out/<rodada>/portfolio")
    ap.add_argument("--n-sims", type=int, default=60000)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    joined_df, P, fixed_ids = load_joined(base)
    if fixed_ids:
        print("[ev] Aviso: probabilidades reconstruídas para match_id:", fixed_ids)

    outcomes = simulate_outcomes(P, n_sims=args.n_sims)

    prize = load_prize_model()
    cpt   = float(prize.get("cost_per_ticket", DEFAULT_PRIZE["cost_per_ticket"]))
    pay14 = float(prize.get("payout_14",      DEFAULT_PRIZE["payout_14"]))
    pay13 = float(prize.get("payout_13",      DEFAULT_PRIZE["payout_13"]))
    kfrac = float(prize.get("kelly_fraction", DEFAULT_PRIZE["kelly_fraction"]))

    port_dir = Path(args.portfolio_dir) if args.portfolio_dir else (base/"portfolio")
    if not port_dir.exists():
        raise RuntimeError(f"[ev] diretório de portfólio não existe: {port_dir}")

    rows=[]
    for f in sorted(port_dir.glob("cartao_*.csv")):
        picks = parse_ticket_csv(f)
        p14, p13 = eval_ticket(outcomes, picks)
        ev = p14*pay14 + p13*pay13 - cpt
        # Kelly fracionado (proxy simples): stake_relativo = k * max(0, EV/custo) (limitado a 1)
        edge = ev / cpt
        stake = kfrac * max(0.0, edge)
        stake = float(min(1.0, stake))
        rows.append({
            "file": f.name,
            "p14": round(p14,8),
            "p13": round(p13,8),
            "EV": round(ev,2),
            "kelly_frac": kfrac,
            "suggested_stake_per_ticket": round(stake,4)
        })

    out = pd.DataFrame(rows)
    out_path = port_dir/"portfolio_ev.csv"
    out.to_csv(out_path, index=False)
    print(f"[ev] EV e Kelly calculados -> {out_path}")
    if not out.empty:
        print(out.to_string(index=False))

if __name__ == "__main__":
    main()
