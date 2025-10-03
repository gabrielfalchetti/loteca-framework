#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/consensus_odds_safe.py (TheOddsAPI only)

- Lê odds de data/out/<RODADA>/odds_theoddsapi.csv; se não houver, tenta data/in/<RODADA>/ e copia para /out.
- Não depende de RapidAPI/API-Football.
- Suporta CSV “longo” (uma seleção por linha) e “colunas”.
- Agrega por match_key pegando o melhor (máximo) odds_home/draw/away quando houver múltiplas linhas.
- Considera válido jogo com >= 1 odd > 1.0 (antes eram 2).
- Gera data/out/<RODADA>/odds_consensus.csv.
- Exit 10 quando não houver nenhum jogo válido (para manter contrato do pipeline).
"""
import argparse, os, sys, re, shutil
from typing import List, Tuple, Dict
import numpy as np
import pandas as pd

# -------------------- paths --------------------
def ensure_out_dir(rodada: str) -> str:
    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def find_theodds_path(rodada: str) -> str:
    # prefere OUT; cai pra IN e copia
    fname = "odds_theoddsapi.csv"
    p_out = os.path.join("data", "out", rodada, fname)
    if os.path.isfile(p_out):
        return p_out
    p_in = os.path.join("data", "in", rodada, fname)
    if os.path.isfile(p_in):
        out_dir = ensure_out_dir(rodada)
        dst = os.path.join(out_dir, fname)
        try:
            shutil.copy2(p_in, dst)
            print("[consensus-safe] INFO: odds_theoddsapi.csv encontrado em /in -> copiado para /out.")
            return dst
        except Exception as e:
            print(f"[consensus-safe] AVISO: falha ao copiar /in -> /out ({e}). Usarei diretamente /in.")
            return p_in
    return ""  # não encontrado

# -------------------- normalização --------------------
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    def norm(c: str) -> str:
        c = str(c).strip().lower()
        c = re.sub(r"[ \t\.\-/]+", "_", c)
        c = re.sub(r"[\(\)\[\]\{\}]+", "", c)
        return c
    out = df.copy()
    out.columns = [norm(c) for c in out.columns]
    return out

def is_american_token(s: str) -> bool:
    s = s.strip()
    return bool(re.fullmatch(r"[+\-]\d+(\.\d+)?", s))

def american_to_decimal(tok: str) -> float:
    v = float(tok)
    return 1.0 + (v/100.0) if v > 0 else 1.0 + (100.0/abs(v))

def to_number(series: pd.Series) -> pd.Series:
    """Converte odds em string para decimal, aceitando '.', ',', e formato americano (+120/-150)."""
    if series is None:
        return pd.Series(dtype="float64")
    raw = series.astype("object").astype(str).str.strip()

    # máscara de odds americanas
    is_am = raw.map(is_american_token).to_numpy(dtype=bool)

    # primeiro tenta decimal com vírgula/ponto
    dec = pd.to_numeric(raw.str.replace(",", ".", regex=False), errors="coerce")
    dec = dec.astype("float64")  # garante dtype float64

    # converte apenas as posições americanas usando numpy (evita FutureWarning de setitem incompatível)
    if is_am.any():
        am_vals = raw[is_am].map(american_to_decimal).astype("float64").to_numpy()
        dec_np = dec.to_numpy(copy=True)
        dec_np[is_am] = am_vals
        dec = pd.Series(dec_np, index=series.index, dtype="float64")

    # odds inválidas ou placeholders (<= 1.0) -> NaN
    dec = dec.mask(dec <= 1.0)

    return pd.to_numeric(dec, errors="coerce")

# aliases
THOME = ["team_home","home_team","mandante","time_casa","time_home","equipa_casa"]
TAWAY = ["team_away","away_team","visitante","time_fora","time_away","equipa_fora"]
MKEY  = ["match_key","game_key","fixture_key","key","match","partida","id_partida"]

HOME  = ["odds_home","home_odds","price_home","home_price","home_decimal","price1",
         "h2h_home","m1","selection_home","market_home","h","home"]
DRAW  = ["odds_draw","draw_odds","price_draw","draw_price","pricex","draw_decimal",
         "h2h_draw","mx","selection_draw","market_draw","x","tie","draw"]
AWAY  = ["odds_away","away_odds","price_away","away_price","away_decimal","price2",
         "h2h_away","m2","selection_away","market_away","a","away"]

SEL_NAME = ["selection","outcome","side","result","pick","market","bet"]
PRICE    = ["odds","price","decimal","price_decimal","odds_decimal","h2h_price","value"]

def pick(df: pd.DataFrame, cands: List[str]) -> str:
    for c in cands:
        if c in df.columns: return c
    for want in cands:
        for col in df.columns:
            if want in col:
                return col
    return ""

def build_match_key(df: pd.DataFrame, th: str, ta: str) -> pd.Series:
    return (
        df[th].astype(str).str.strip().str.lower()
        + "__vs__" +
        df[ta].astype(str).str.strip().str.lower()
    )

def valid_row(r) -> bool:
    vals = [r.get("odds_home"), r.get("odds_draw"), r.get("odds_away")]
    vals = [float(x) for x in vals if pd.notna(x)]
    # >>> regra relaxada: basta pelo menos UMA odd válida > 1.0
    return sum(v > 1.0 for v in vals) >= 1

def normalize_theodds(raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str,int]]:
    reasons = {"sem_odd_valida": 0}
    if raw is None or raw.empty:
        cols = ["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]
        return pd.DataFrame(columns=cols), reasons

    df = clean_columns(raw)
    th = pick(df, THOME) or "team_home"
    ta = pick(df, TAWAY) or "team_away"
    mk = pick(df, MKEY)

    sel_col   = pick(df, SEL_NAME)
    price_col = pick(df, PRICE)
    has_spread = (sel_col != "") and (price_col != "")
    has_direct = any(c in df.columns for c in HOME + DRAW + AWAY)

    # ----- modo “longo” (uma seleção por linha) -----
    if has_spread and not has_direct:
        if not mk:
            mk = "match_key"
            df[mk] = build_match_key(df, th, ta)

        sel_norm = df[sel_col].astype(str).str.lower().str.strip()
        home_mask = sel_norm.str.contains(r"\b(home|casa|mandante|1)\b")
        draw_mask = sel_norm.str.contains(r"\b(draw|empate|x|tie)\b")
        away_mask = sel_norm.str.contains(r"\b(away|fora|visitante|2)\b")

        tmp = pd.DataFrame({
            "match_key": df[mk].astype(str),
            "team_home": df[th],
            "team_away": df[ta],
            "sel_home": home_mask,
            "sel_draw": draw_mask,
            "sel_away": away_mask,
            "price": to_number(df[price_col]),
        })

        def best_for(mask):
            t = tmp[mask][["match_key","price"]]
            if t.empty: return pd.Series(dtype=float)
            return t.groupby("match_key")["price"].max()

        oh = best_for(tmp["sel_home"])
        od = best_for(tmp["sel_draw"])
        oa = best_for(tmp["sel_away"])

        idx = tmp.drop_duplicates("match_key")[["match_key","team_home","team_away"]]
        out = pd.DataFrame({
            "match_key": idx["match_key"],
            "team_home": idx["team_home"],
            "team_away": idx["team_away"],
            "odds_home": idx["match_key"].map(oh),
            "odds_draw": idx["match_key"].map(od),
            "odds_away": idx["match_key"].map(oa),
        })

    # ----- modo “colunas” -> AGREGA por match_key (pega máximos) -----
    else:
        ch = pick(df, HOME)
        cd = pick(df, DRAW)
        ca = pick(df, AWAY)

        work = pd.DataFrame({
            "team_home": df.get(th),
            "team_away": df.get(ta),
        })
        if not mk:
            work["match_key"] = build_match_key(df, th, ta)
        else:
            work["match_key"] = df[mk].astype(str)

        work["odds_home"] = to_number(df[ch]) if ch else pd.NA
        work["odds_draw"] = to_number(df[cd]) if cd else pd.NA
        work["odds_away"] = to_number(df[ca]) if ca else pd.NA

        firsts = work.groupby("match_key")[["team_home","team_away"]].first()
        agg = work.groupby("match_key")[["odds_home","odds_draw","odds_away"]].max(min_count=1)
        out = firsts.join(agg, how="outer").reset_index()

    out["__valid"] = out.apply(valid_row, axis=1)
    reasons["sem_odd_valida"] = int((~out["__valid"]).sum())
    out = out[out["__valid"]].drop(columns="__valid")
    return out, reasons

# -------------------- main --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = ensure_out_dir(args.rodada)
    p_out = os.path.join(out_dir, "odds_consensus.csv")

    p_theo = find_theodds_path(args.rodada)
    if not p_theo:
        print("[consensus-safe] ERRO: odds_theoddsapi.csv não encontrado em /out nem /in.", file=sys.stderr)
        pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]).to_csv(p_out, index=False)
        sys.exit(10)

    try:
        raw = pd.read_csv(p_theo)
    except Exception as e:
        print(f"[consensus-safe] ERRO ao ler {p_theo}: {e}", file=sys.stderr)
        pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]).to_csv(p_out, index=False)
        sys.exit(10)

    norm, reasons = normalize_theodds(raw)
    print(f"[consensus-safe] lido odds_theoddsapi.csv -> {len(raw)} linhas; válidas (>=1 odd): {len(norm)}")
    if len(norm) < len(raw):
        print(f"[consensus-safe] motivos inválidos theoddsapi: {reasons}")

    total = len(norm)
    if total == 0:
        pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]).to_csv(p_out, index=False)
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    print(f"[consensus-safe] AMOSTRA (top 5): {norm.head(5).to_dict(orient='records')}")
    norm.to_csv(p_out, index=False)
    print(f"[consensus-safe] OK -> {p_out} ({total} linhas)")

if __name__ == "__main__":
    main()