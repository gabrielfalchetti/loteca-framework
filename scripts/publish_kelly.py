#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/publish_kelly.py

- Lê configurações de env (BANKROLL, KELLY_FRACTION, KELLY_CAP, MIN/MAX_STAKE, ROUND_TO, KELLY_TOP_N).
- Prefere odds de data/out/<RODADA>/odds_consensus.csv.
- Se odds_consensus não existir ou estiver vazio, tenta data/out/<RODADA>/odds_theoddsapi.csv;
  se não existir, tenta data/in/<RODADA>/odds_theoddsapi.csv (e copia para /out).
- Normaliza colunas e filtra odds > 1.0.
- Busca probabilidades em qualquer arquivo predictions_*.csv encontrado no out_dir
  (ordem: predictions_stacked, predictions_calibrated, predictions_xg_bi, predictions_xg_uni).
- Só calcula Kelly quando houver (prob_* e odds_*) para o mesmo outcome.
- Gera data/out/<RODADA>/kelly_stakes.csv. Se nenhum par elegível, sai com código 10 com mensagem clara.
"""

import argparse, os, sys, re, json, math, shutil
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
import pandas as pd
import numpy as np


# --------------------------- util e config ---------------------------

@dataclass
class KellyConfig:
    bankroll: float = 1000.0
    kelly_fraction: float = 0.5
    kelly_cap: float = 0.10
    min_stake: float = 0.0
    max_stake: float = 0.0  # 0 = sem teto
    round_to: float = 1.0
    top_n: int = 14

def getenv_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default

def getenv_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def load_config() -> KellyConfig:
    return KellyConfig(
        bankroll=getenv_float("BANKROLL", 1000.0),
        kelly_fraction=getenv_float("KELLY_FRACTION", 0.5),
        kelly_cap=getenv_float("KELLY_CAP", 0.10),
        min_stake=getenv_float("MIN_STAKE", 0.0),
        max_stake=getenv_float("MAX_STAKE", 0.0),
        round_to=getenv_float("ROUND_TO", 1.0),
        top_n=getenv_int("KELLY_TOP_N", 14),
    )

def ensure_out_dir(rodada: str) -> str:
    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def copy_if_exists(src: str, dst: str) -> Optional[str]:
    if os.path.isfile(src):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        try:
            shutil.copy2(src, dst)
            return dst
        except Exception:
            return src
    return None

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    def norm(c: str) -> str:
        c = str(c).strip().lower()
        c = re.sub(r"[ \t\.\-/]+", "_", c)
        c = re.sub(r"[\(\)\[\]\{\}]+", "", c)
        return c
    out = df.copy()
    out.columns = [norm(c) for c in df.columns]
    return out

def build_match_key(df: pd.DataFrame, th: str, ta: str) -> pd.Series:
    return (
        df[th].astype(str).str.strip().str.lower()
        + "__vs__" +
        df[ta].astype(str).str.strip().str.lower()
    )

# --------------------------- leitura de odds ---------------------------

HOME_ALS = [
    "odds_home","home_odds","price_home","home_price","home_decimal",
    "price1","h2h_home","m1","h","home"
]
DRAW_ALS = [
    "odds_draw","draw_odds","price_draw","draw_price","draw_decimal",
    "pricex","h2h_draw","mx","x","tie","draw"
]
AWAY_ALS = [
    "odds_away","away_odds","price_away","away_price","away_decimal",
    "price2","h2h_away","m2","a","away"
]

TH_ALS = ["team_home","home_team","mandante","time_casa","time_home","equipa_casa"]
TA_ALS = ["team_away","away_team","visitante","time_fora","time_away","equipa_fora"]
MK_ALS = ["match_key","game_key","fixture_key","key","match","partida","id_partida"]

def pick(df: pd.DataFrame, cands: List[str]) -> str:
    for c in cands:
        if c in df.columns:
            return c
    for want in cands:
        for col in df.columns:
            if want in col:
                return col
    return ""

def to_number(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    raw = series.astype("object").astype(str).str.strip()
    # americano?
    is_am = raw.str.match(r"^[+\-]\d+(\.\d+)?$").fillna(False).to_numpy(bool)

    dec = pd.to_numeric(raw.str.replace(",", ".", regex=False), errors="coerce").astype("float64")

    if is_am.any():
        def am2dec(tok: str) -> float:
            v = float(tok)
            return 1.0 + (v/100.0) if v > 0 else 1.0 + (100.0/abs(v))
        am_vals = raw[is_am].map(am2dec).astype("float64").to_numpy()
        dec_np = dec.to_numpy(copy=True)
        dec_np[is_am] = am_vals
        dec = pd.Series(dec_np, index=series.index, dtype="float64")

    dec = dec.mask(dec <= 1.0)
    return pd.to_numeric(dec, errors="coerce")

def read_any_odds(rodada: str, debug: bool=False) -> pd.DataFrame:
    """
    Tenta na ordem:
      1) data/out/<rodada>/odds_consensus.csv
      2) data/out/<rodada>/odds_theoddsapi.csv
      3) data/in/<rodada>/odds_theoddsapi.csv (copia pra /out)
    Normaliza para colunas: team_home, team_away, match_key, odds_home, odds_draw, odds_away
    Filtra linhas onde pelo menos UMA odds > 1.0.
    """
    out_dir = ensure_out_dir(rodada)
    p_cons = os.path.join(out_dir, "odds_consensus.csv")
    if os.path.isfile(p_cons):
        df = pd.read_csv(p_cons)
        df = clean_columns(df)
        # garante tipos/filtro
        ch, cd, ca = pick(df, HOME_ALS), pick(df, DRAW_ALS), pick(df, AWAY_ALS)
        df["odds_home"] = to_number(df.get(ch))
        df["odds_draw"] = to_number(df.get(cd))
        df["odds_away"] = to_number(df.get(ca))
        th, ta, mk = pick(df, TH_ALS), pick(df, TA_ALS), pick(df, MK_ALS)
        if not mk and th and ta:
            df["match_key"] = build_match_key(df, th, ta)
        if debug:
            print(f"[kelly] consensus lido: {len(df)} linhas")
        df = df[(df[["odds_home","odds_draw","odds_away"]] > 1.0).any(axis=1)]
        cols = ["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]
        cols = [c for c in cols if c in df.columns]
        return df[cols]

    p_theo_out = os.path.join(out_dir, "odds_theoddsapi.csv")
    if os.path.isfile(p_theo_out):
        base = p_theo_out
    else:
        p_theo_in = os.path.join("data","in",rodada,"odds_theoddsapi.csv")
        base = copy_if_exists(p_theo_in, p_theo_out) or p_theo_in

    if not base or not os.path.isfile(base):
        if debug:
            print("[kelly] AVISO: nenhum arquivo de odds encontrado em /out nem /in.")
        return pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"])

    raw = pd.read_csv(base)
    df = clean_columns(raw)

    th, ta = pick(df, TH_ALS), pick(df, TA_ALS)
    mk = pick(df, MK_ALS)
    if not mk and th and ta:
        df["match_key"] = build_match_key(df, th, ta)
        mk = "match_key"

    ch, cd, ca = pick(df, HOME_ALS), pick(df, DRAW_ALS), pick(df, AWAY_ALS)
    work = pd.DataFrame({
        "team_home": df[th] if th else "",
        "team_away": df[ta] if ta else "",
        "match_key": df[mk].astype(str) if mk else "",
        "odds_home": to_number(df.get(ch)) if ch else pd.NA,
        "odds_draw": to_number(df.get(cd)) if cd else pd.NA,
        "odds_away": to_number(df.get(ca)) if ca else pd.NA,
    })

    # agrega por match_key pegando máximos de cada coluna de odds
    firsts = work.groupby("match_key")[["team_home","team_away"]].first()
    agg = work.groupby("match_key")[["odds_home","odds_draw","odds_away"]].max(min_count=1)
    out = firsts.join(agg, how="outer").reset_index()

    out = out[(out[["odds_home","odds_draw","odds_away"]] > 1.0).any(axis=1)]
    if debug:
        print(f"[kelly] odds_theoddsapi normalizado: {len(out)} linhas elegíveis (>=1 odd > 1.0)")
    return out[["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]]

# --------------------------- leitura de previsões ---------------------------

PREF_PRED = [
    "predictions_stacked.csv",
    "predictions_calibrated.csv",
    "predictions_xg_bi.csv",
    "predictions_xg_uni.csv",
]

PROB_HOME_ALS = ["prob_home","p_home","prob1","home_prob","prob_casa"]
PROB_DRAW_ALS = ["prob_draw","p_draw","probx","draw_prob","prob_empate"]
PROB_AWAY_ALS = ["prob_away","p_away","prob2","away_prob","prob_fora"]

def read_predictions(out_dir: str, debug: bool=False) -> pd.DataFrame:
    for fname in PREF_PRED:
        p = os.path.join(out_dir, fname)
        if os.path.isfile(p):
            try:
                df = pd.read_csv(p)
                df = clean_columns(df)
                mk = pick(df, MK_ALS)
                th, ta = pick(df, TH_ALS), pick(df, TA_ALS)
                if not mk and th and ta:
                    df["match_key"] = build_match_key(df, th, ta)
                elif mk:
                    df["match_key"] = df[mk].astype(str)

                ph = pick(df, PROB_HOME_ALS)
                pdw = pick(df, PROB_DRAW_ALS)
                pa = pick(df, PROB_AWAY_ALS)

                def to_prob(s: pd.Series) -> pd.Series:
                    if s is None:
                        return pd.Series(dtype="float64")
                    ser = pd.to_numeric(s, errors="coerce").astype("float64")
                    if ser.max(skipna=True) > 1.0001:
                        ser = ser / 100.0
                    return ser.clip(lower=0.0, upper=1.0)

                out = pd.DataFrame({"match_key": df["match_key"]})
                out["prob_home"] = to_prob(df.get(ph))
                out["prob_draw"] = to_prob(df.get(pdw))
                out["prob_away"] = to_prob(df.get(pa))
                if debug:
                    print(f"[kelly] predictions de {fname}: {len(out)} linhas")
                return out[["match_key","prob_home","prob_draw","prob_away"]]
            except Exception as e:
                if debug:
                    print(f"[kelly] AVISO: falha ao ler {fname}: {e}")
                continue
    if debug:
        print("[kelly] AVISO: nenhum arquivo de previsões encontrado.")
    return pd.DataFrame(columns=["match_key","prob_home","prob_draw","prob_away"])

# --------------------------- Kelly ---------------------------

def kelly_fraction(prob: float, odds: float) -> float:
    """Kelly fracionária: f* = (p*(b+1) - 1) / b  com b = odds-1. Se <0 -> 0."""
    if not (prob is not None and odds is not None):
        return 0.0
    if not (0.0 <= prob <= 1.0) or not (odds > 1.0):
        return 0.0
    b = odds - 1.0
    fstar = (prob*(b+1.0) - 1.0) / b
    return max(0.0, fstar)

def stake_from_kelly(prob: float, odds: float, cfg: KellyConfig) -> Tuple[float, float, float]:
    """Retorna (stake, kelly_full, edge)."""
    if pd.isna(prob) or pd.isna(odds) or odds <= 1.0 or prob < 0 or prob > 1:
        return 0.0, 0.0, float("nan")

    b = odds - 1.0
    k_full = (prob*(b+1.0) - 1.0) / b
    if k_full <= 0:
        return 0.0, k_full, prob - (1.0/(b+1.0))

    k_use = min(cfg.kelly_fraction * k_full, cfg.kelly_cap)
    stake = cfg.bankroll * k_use

    if cfg.max_stake and cfg.max_stake > 0:
        stake = min(stake, cfg.max_stake)
    if cfg.min_stake and cfg.min_stake > 0 and stake < cfg.min_stake:
        stake = 0.0

    if cfg.round_to and cfg.round_to > 0:
        stake = math.floor(stake / cfg.round_to + 1e-9) * cfg.round_to

    edge = prob - (1.0/(b+1.0))
    return float(stake), float(k_full), float(edge)

# --------------------------- pipeline ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    cfg = load_config()
    out_dir = ensure_out_dir(args.rodada)

    print(f"[kelly] config: {json.dumps(cfg.__dict__)}")
    print(f"[kelly] out_dir: {out_dir}")

    # 1) odds
    odds = read_any_odds(args.rodada, debug=args.debug)
    if args.debug:
        print(f"[kelly] odds carregadas: {len(odds)}")

    # 2) predictions (opcional)
    preds = read_predictions(out_dir, debug=args.debug)

    # 3) join (mantém jogos com odds; Kelly só onde tiver prob)
    df = odds.merge(preds, on="match_key", how="left")

    if args.debug:
        amostra = df.head(5).to_dict(orient="records")
        print(f"[kelly] AMOSTRA pós-join (top 5): {amostra}")

    # 4) calcula Kelly por outcome
    records = []
    for _, r in df.iterrows():
        row_base = {"match_key": r["match_key"], "team_home": r["team_home"], "team_away": r["team_away"]}
        for side, pcol, ocol in [("home","prob_home","odds_home"),("draw","prob_draw","odds_draw"),("away","prob_away","odds_away")]:
            p = r.get(pcol, np.nan)
            o = r.get(ocol, np.nan)
            stake, kfull, edge = stake_from_kelly(p, o, cfg)
            rec = dict(row_base)
            rec.update({
                "side": side,
                "prob": float(p) if pd.notna(p) else np.nan,
                "odds": float(o) if pd.notna(o) else np.nan,
                "kelly_full": kfull,
                "edge": edge,
                "stake": stake,
            })
            records.append(rec)

    picks = pd.DataFrame.from_records(records)

    # 5) mantém somente odds > 1.0 e stake > 0
    picks = picks[(picks["odds"] > 1.0) & (picks["stake"] > 0.0)]

    # 6) ordena e limita
    if not picks.empty:
        picks = picks.sort_values(["stake","edge"], ascending=[False, False]).head(cfg.top_n)

    # 7) saída
    out_path = os.path.join(out_dir, "kelly_stakes.csv")
    if picks.empty:
        picks.to_csv(out_path, index=False)
        if len(odds) == 0:
            print("[kelly] ERRO: nenhuma linha de odds válida (odds_* > 1.0).")
        else:
            print("[kelly] AVISO: odds existem, mas não há probabilidades compatíveis para calcular Kelly (stake=0 em todos).")
            print("        Verifique se predictions_*.csv foi gerado e contém prob_home/prob_draw/prob_away.")
        sys.exit(10)

    picks.to_csv(out_path, index=False)
    if args.debug:
        print(f"[kelly] AMOSTRA PICKS (top 5): {picks.head(5).to_dict(orient='records')}")
    print(f"[kelly] OK -> {out_path} ({len(picks)} linhas)")

if __name__ == "__main__":
    main()