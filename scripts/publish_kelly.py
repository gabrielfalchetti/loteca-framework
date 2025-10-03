#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Publica apostas (Kelly) a partir de odds e previsões.
- Usa TheOddsAPI (odds_consensus.csv preferencial; fallback para odds_theoddsapi.csv).
- Procura em data/in/<RODADA>/ e depois data/out/<RODADA>/.
- Se não houver predictions_*.csv, usa probabilidades implícitas das odds (sem overround).
- Gera data/out/<RODADA>/kelly_stakes.csv e um resumo em stdout.
- NUNCA falha por falta de predictions; só falha se não houver NENHUMA odd válida > 1.0.
"""

from __future__ import annotations
import os
import sys
import math
import json
from pathlib import Path
from typing import Optional, List, Tuple, Dict

import numpy as np
import pandas as pd


# ========================= utils ==========================

def log(msg: str) -> None:
    print(f"[kelly] {msg}")

def die(code: int, msg: str) -> None:
    log(f"ERRO: {msg}")
    sys.exit(code)

def norm(s: str) -> str:
    import unicodedata
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

def match_key(h: str, a: str) -> str:
    return f"{norm(h)}__vs__{norm(a)}"

def read_csv_if_exists(p: Path) -> Optional[pd.DataFrame]:
    if p.exists():
        try:
            return pd.read_csv(p)
        except Exception as e:
            log(f"AVISO: falha ao ler {p}: {e}")
    return None

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def american_to_decimal(x) -> float:
    try:
        v = float(x)
    except Exception:
        return np.nan
    if v > 0:
        return 1.0 + v/100.0
    if v < 0:
        return 1.0 + 100.0/abs(v)
    return np.nan

def coerce_odds(s: pd.Series) -> pd.Series:
    out = pd.Series(index=s.index, dtype="float64")
    # numéricos diretos
    num = s.apply(lambda v: isinstance(v, (int,float))) & s.notna()
    out.loc[num] = pd.to_numeric(s[num], errors="coerce")

    # strings
    is_str = s.apply(lambda v: isinstance(v, str))
    if is_str.any():
        ss = s[is_str].fillna("").str.strip()
        # vazios e '[]'
        mask_empty = ss.eq("") | ss.eq("[]") | ss.eq("[ ]")
        out.loc[mask_empty] = np.nan
        # remove colchetes
        ss2 = ss.str.replace("[", "", regex=False).str.replace("]", "", regex=False)
        # americanos
        is_am = ss2.str.match(r"^[\+\-]\d+$")
        out.loc[is_am] = ss2[is_am].map(american_to_decimal)
        # decimais
        rem = ~is_am & ~mask_empty
        out.loc[rem] = pd.to_numeric(ss2[rem], errors="coerce")

    # outros tipos -> NaN
    other = ~(num | is_str)
    out.loc[other] = np.nan

    # odds <= 1.0 não são válidas
    out = out.where(out > 1.0, np.nan)
    return out


# ========================= leitura de odds ==========================

def load_odds(rodada: str) -> pd.DataFrame:
    """
    Tenta (nesta ordem):
      - data/in/<rodada>/odds_consensus.csv
      - data/out/<rodada>/odds_consensus.csv
      - data/in/<rodada>/odds_theoddsapi.csv
      - data/out/<rodada>/odds_theoddsapi.csv
    Retorna DF com colunas: team_home, team_away, match_key, odds_home, odds_draw, odds_away
    """
    bases = [Path("data/in")/rodada, Path("data/out")/rodada]
    candidates = [
        "odds_consensus.csv",
        "odds_theoddsapi.csv",
    ]
    df = None
    used = None
    for base in bases:
        for name in candidates:
            p = base / name
            d = read_csv_if_exists(p)
            if d is not None and len(d) > 0:
                df, used = d.copy(), p
                break
        if df is not None:
            break
    if df is None:
        die(10, "nenhum arquivo de odds encontrado (consensus ou theoddsapi)")
    log(f"odds carregadas de: {used}")

    # normalizar nomes de colunas
    df.columns = [c.lower() for c in df.columns]
    # garantir colunas básicas
    # tentar detectar aliases comuns
    def first(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    hcol = first("team_home", "home", "home_team", "mandante")
    acol = first("team_away", "away", "away_team", "visitante")
    mcol = first("match_key", "match", "match_id", "partida")

    if mcol is None:
        if hcol and acol:
            df["match_key"] = df.apply(lambda r: match_key(str(r[hcol]), str(r[acol])), axis=1)
            mcol = "match_key"
        else:
            die(10, "colunas básicas ausentes (team_home/team_away) para gerar match_key")

    oh = first("odds_home","home_odds","price_home","home_price","h2h_home")
    od = first("odds_draw","draw_odds","price_draw","draw_price","h2h_draw")
    oa = first("odds_away","away_odds","price_away","away_price","h2h_away")

    # criar colunas destino
    out = pd.DataFrame({
        "team_home": df[hcol] if hcol else np.nan,
        "team_away": df[acol] if acol else np.nan,
        "match_key": df[mcol],
        "odds_home": coerce_odds(df[oh]) if oh else np.nan,
        "odds_draw": coerce_odds(df[od]) if od else np.nan,
        "odds_away": coerce_odds(df[oa]) if oa else np.nan,
    })

    # pelo menos UMA odd > 1.0
    valid = out[["odds_home","odds_draw","odds_away"]].gt(1.0).sum(axis=1) >= 1
    out = out.loc[valid].reset_index(drop=True)
    if len(out)==0:
        die(10, "nenhuma linha de odds válida (tudo vazio ou <= 1.0)")

    log(f"odds carregadas: {len(out)}")
    return out


# ========================= previsões ==========================

def load_predictions(rodada: str) -> Optional[pd.DataFrame]:
    """
    Procura qualquer predictions_*.csv (in/out). Retorna DF com
    match_key, prob_home, prob_draw, prob_away — ou None se não achar.
    """
    bases = [Path("data/in")/rodada, Path("data/out")/rodada]
    preds = None
    used = None
    for base in bases:
        for p in sorted(base.glob("predictions_*.csv")):
            d = read_csv_if_exists(p)
            if d is not None and len(d)>0:
                preds, used = d.copy(), p
                break
        if preds is not None:
            break
    if preds is None:
        return None

    preds.columns = [c.lower() for c in preds.columns]
    # tentar normalizar
    need = {"match_key","prob_home","prob_draw","prob_away"}
    have = set(preds.columns)
    if not need.issubset(have):
        # tentar reconstruir match_key
        hcol = "team_home" if "team_home" in preds.columns else None
        acol = "team_away" if "team_away" in preds.columns else None
        if "match_key" not in preds.columns and hcol and acol:
            preds["match_key"] = preds.apply(lambda r: match_key(str(r[hcol]), str(r[acol])), axis=1)
        # renomear prováveis nomes
        rename = {}
        for c in preds.columns:
            if c in ("p_home","ph","prob_h"): rename[c]="prob_home"
            if c in ("p_draw","pd","prob_d"): rename[c]="prob_draw"
            if c in ("p_away","pa","prob_a"): rename[c]="prob_away"
        if rename:
            preds = preds.rename(columns=rename)
    # filtro final
    if not {"match_key","prob_home","prob_draw","prob_away"}.issubset(set(preds.columns)):
        return None

    # coerção numérica e normalização (garante soma ~1)
    for c in ("prob_home","prob_draw","prob_away"):
        preds[c] = pd.to_numeric(preds[c], errors="coerce")
    row_sum = preds[["prob_home","prob_draw","prob_away"]].sum(axis=1)
    ok = row_sum.gt(0)
    preds.loc[ok, ["prob_home","prob_draw","prob_away"]] = preds.loc[ok, ["prob_home","prob_draw","prob_away"]].div(row_sum[ok], axis=0)

    log(f"previsões carregadas de: {used} ({ok.sum()} linhas utilizáveis)")
    return preds[["match_key","prob_home","prob_draw","prob_away"]]


def implied_probs_from_odds(odds_row: pd.Series) -> Tuple[float,float,float]:
    """Converte odds decimais em probabilidades implícitas (removendo overround)."""
    h, d, a = odds_row["odds_home"], odds_row["odds_draw"], odds_row["odds_away"]
    inv = []
    for v in (h,d,a):
        inv.append(0.0 if (pd.isna(v) or v<=1.0) else 1.0/float(v))
    s = sum(inv)
    if s <= 0:
        return (np.nan, np.nan, np.nan)
    return (inv[0]/s, inv[1]/s, inv[2]/s)


# ========================= Kelly ==========================

class KellyCfg:
    def __init__(self):
        self.bankroll = float(os.environ.get("BANKROLL", "1000"))
        self.kelly_fraction = float(os.environ.get("KELLY_FRACTION", "0.5"))
        self.kelly_cap = float(os.environ.get("KELLY_CAP", "0.10"))
        self.min_stake = float(os.environ.get("MIN_STAKE", "0"))
        self.max_stake = float(os.environ.get("MAX_STAKE", "0"))
        self.round_to = float(os.environ.get("ROUND_TO", "1"))
        self.top_n = int(os.environ.get("KELLY_TOP_N", "14"))

    def as_dict(self):
        return {
            "bankroll": self.bankroll,
            "kelly_fraction": self.kelly_fraction,
            "kelly_cap": self.kelly_cap,
            "min_stake": self.min_stake,
            "max_stake": self.max_stake,
            "round_to": self.round_to,
            "top_n": self.top_n,
        }

def kelly_fraction(p: float, dec_odds: float) -> float:
    """
    Kelly para odds decimais.
    f* = (b*p - q)/b, com b = odds-1, q = 1-p.
    Retorna fração do bankroll (pode ser <0).
    """
    if pd.isna(p) or pd.isna(dec_odds) or dec_odds <= 1.0:
        return 0.0
    b = dec_odds - 1.0
    q = 1.0 - p
    f = (b*p - q) / b
    return float(f)

def clamp_stake(stake: float, cfg: KellyCfg) -> float:
    if cfg.max_stake > 0:
        stake = min(stake, cfg.max_stake)
    if cfg.min_stake > 0:
        # se >0, eleva para mínimo apenas se stake > 0
        if stake > 0:
            stake = max(stake, cfg.min_stake)
    # arredondamento
    if cfg.round_to > 0:
        stake = math.floor(stake / cfg.round_to + 1e-9) * cfg.round_to
    return max(0.0, stake)

def compute_kelly_rows(df: pd.DataFrame, cfg: KellyCfg, use_implied: bool) -> List[Dict]:
    rows = []
    for _, r in df.iterrows():
        mk = r["match_key"]
        th, ta = r["team_home"], r["team_away"]
        oh, od, oa = r["odds_home"], r["odds_draw"], r["odds_away"]

        if use_implied:
            ph, pd_, pa = implied_probs_from_odds(r)
        else:
            ph, pd_, pa = r.get("prob_home", np.nan), r.get("prob_draw", np.nan), r.get("prob_away", np.nan)

        # Kelly por mercado
        bets = []
        for label, p, o in (
            ("HOME", ph, oh),
            ("DRAW", pd_, od),
            ("AWAY", pa, oa),
        ):
            k = kelly_fraction(p, o)
            k = k * cfg.kelly_fraction  # fração de Kelly
            # cap na fração (ex.: 10% do bankroll)
            k = min(k, cfg.kelly_cap)
            stake = clamp_stake(k * cfg.bankroll, cfg)
            bets.append((label, p, o, k, stake))

        # escolher a maior stake positiva
        bets_pos = [b for b in bets if b[4] > 0]
        best = max(bets_pos, key=lambda x: x[4]) if bets_pos else max(bets, key=lambda x: x[3])  # senão, a maior fração (pode ser <=0)
        label, p, o, k, stake = best

        rows.append({
            "match_key": mk,
            "team_home": th,
            "team_away": ta,
            "pick": label,
            "prob": p,
            "odds": o,
            "kelly_fraction": k,
            "stake": stake,
        })
    return rows


# ========================= main ==========================

def main():
    # RODADA
    if "--rodada" in sys.argv:
        rodada = sys.argv[sys.argv.index("--rodada")+1]
    else:
        rodada = os.environ.get("RODADA")
    if not rodada:
        die(2, "use --rodada <YYYY-MM-DD_HHMM> ou defina env RODADA")

    cfg = KellyCfg()
    log(f"config: {json.dumps(cfg.as_dict())}")
    out_dir = Path("data/out") / rodada
    ensure_dir(out_dir)
    log(f"out_dir: {out_dir}")

    # 1) odds
    odds = load_odds(rodada)

    # 2) previsões (ou implied)
    preds = load_predictions(rodada)
    use_implied = False
    if preds is None:
        log("AVISO: nenhum arquivo de previsões encontrado.")
        log("       Caindo para probabilidades implícitas de mercado (sem overround).")
        use_implied = True
        work = odds.copy()
        work["prob_home"], work["prob_draw"], work["prob_away"] = zip(
            *work.apply(implied_probs_from_odds, axis=1)
        )
    else:
        work = odds.merge(preds, on="match_key", how="left")

    log(f"AMOSTRA pós-join (top 5): {work.head(5).to_dict(orient='records')}")

    # 3) calcula Kelly
    picks = compute_kelly_rows(work, cfg, use_implied=use_implied)
    dfp = pd.DataFrame(picks)

    # 4) ordena por stake desc e aplica top_n se fizer sentido
    dfp = dfp.sort_values(["stake","kelly_fraction"], ascending=[False, False]).reset_index(drop=True)
    if cfg.top_n > 0 and len(dfp) > cfg.top_n:
        dfp = dfp.head(cfg.top_n)

    # 5) salva
    out_csv = out_dir / "kelly_stakes.csv"
    dfp.to_csv(out_csv, index=False)
    log(f"OK -> {out_csv} ({len(dfp)} linhas)")
    # imprime um pequeno resumo
    if len(dfp) > 0:
        log("TOP picks:")
        for i, r in dfp.head(10).iterrows():
            log(f"  #{i+1}: {r['team_home']} x {r['team_away']} | {r['pick']} "
                f"| prob={None if pd.isna(r['prob']) else round(float(r['prob']),4)} "
                f"| odds={None if pd.isna(r['odds']) else round(float(r['odds']),3)} "
                f"| kelly={round(float(r['kelly_fraction']),4)} | stake={round(float(r['stake']),2)}")

if __name__ == "__main__":
    main()