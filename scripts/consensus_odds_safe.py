#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/consensus_odds_safe.py

- Procura odds em data/out/<RODADA>/ e, se não houver, em data/in/<RODADA>/ (e copia p/ out).
- Aceita 1 ou 2 provedores (theoddsapi / apifootball). NÃO exige ambos.
- Normaliza cabeçalhos e numéricos (decimal e american).
- Suporta CSV “longo” (uma seleção por linha) e CSV “por colunas”.
- ***NOVO***: no modo “colunas”, agrega por match_key e pega o MAX de cada odds_*.
- Gera data/out/<RODADA>/odds_consensus.csv com (team_home, team_away, match_key, odds_home, odds_draw, odds_away).
- Sai com exit 10 se não houver jogos com >= 2 odds > 1.0.
"""
import argparse, os, sys, re, shutil
from typing import List, Tuple, Dict
import pandas as pd

# -------------------- utils de path --------------------
def _ensure_out_dir(rodada: str) -> str:
    out_dir = os.path.join("data", "out", rodada)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def _probe(provider_fname: str, rodada: str) -> str:
    """Prefere /out; cai para /in e copia para /out se possível."""
    p_out = os.path.join("data", "out", rodada, provider_fname)
    if os.path.isfile(p_out):
        return p_out
    p_in = os.path.join("data", "in", rodada, provider_fname)
    if os.path.isfile(p_in):
        out_dir = _ensure_out_dir(rodada)
        dst = os.path.join(out_dir, provider_fname)
        try:
            shutil.copy2(p_in, dst)
            print(f"[consensus-safe] INFO: {provider_fname} encontrado em /in -> copiado para /out.")
            return dst
        except Exception as e:
            print(f"[consensus-safe] AVISO: falha ao copiar de /in para /out ({e}). Usarei direto de /in.")
            return p_in
    return ""  # não encontrado

# -------------------- limpeza / cabeçalhos --------------------
def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    def norm(c: str) -> str:
        c = str(c).strip().lower()
        c = re.sub(r"[ \t\.\-/]+", "_", c)
        c = re.sub(r"[\(\)\[\]\{\}]+", "", c)
        return c
    out = df.copy()
    out.columns = [norm(c) for c in out.columns]
    return out

# -------------------- parsing numérico --------------------
def _is_american_token(s: str) -> bool:
    s = s.strip()
    # +150  -120  (+150.0)  etc.
    return bool(re.fullmatch(r"[+\-]\d+(\.\d+)?", s))

def _american_to_decimal_token(tok: str) -> float:
    v = float(tok)
    if v > 0:
        return 1.0 + (v / 100.0)
    else:
        return 1.0 + (100.0 / abs(v))

def _to_number(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    raw = series.astype("object").astype(str).str.strip()
    # detecta american
    is_am = raw.map(_is_american_token)
    # troca vírgula decimal por ponto
    dec = pd.to_numeric(raw.str.replace(",", ".", regex=False), errors="coerce")
    dec.loc[is_am] = raw[is_am].map(_american_to_decimal_token)
    # trata zeros/negativos como nulos (não são odds válidas)
    dec = dec.mask(dec <= 1.0)
    return pd.to_numeric(dec, errors="coerce")

# -------------------- aliases --------------------
THOME = ["team_home","home_team","mandante","time_casa","time_home","equipa_casa"]
TAWAY = ["team_away","away_team","visitante","time_fora","time_away","equipa_fora"]
MKEY  = ["match_key","game_key","fixture_key","key","match","partida","id_partida"]

HOME  = ["odds_home","home_odds","price_home","home_price","home_decimal","price1",
         "h2h_home","m1","selection_home","market_home","h"]
DRAW  = ["odds_draw","draw_odds","price_draw","draw_price","pricex","draw_decimal",
         "h2h_draw","mx","selection_draw","market_draw","x","tie"]
AWAY  = ["odds_away","away_odds","price_away","away_price","away_decimal","price2",
         "h2h_away","m2","selection_away","market_away","a"]

SEL_NAME = ["selection","outcome","side","result","pick","market","bet"]
PRICE    = ["odds","price","decimal","price_decimal","odds_decimal","h2h_price","value"]

def _pick(df: pd.DataFrame, cands: List[str]) -> str:
    for c in cands:
        if c in df.columns: return c
    # fallback: contém substring
    for want in cands:
        for col in df.columns:
            if want in col:
                return col
    return ""

def _build_match_key(df: pd.DataFrame, th: str, ta: str) -> pd.Series:
    return (
        df[th].astype(str).str.strip().str.lower() + "__vs__" +
        df[ta].astype(str).str.strip().str.lower()
    )

def _valid_row(r) -> bool:
    vals = [r.get("odds_home"), r.get("odds_draw"), r.get("odds_away")]
    vals = [float(x) for x in vals if pd.notna(x)]
    return sum(v > 1.0 for v in vals) >= 2

# -------------------- normalização por provedor --------------------
def _normalize_provider(raw: pd.DataFrame, tag: str) -> Tuple[pd.DataFrame, Dict[str,int]]:
    reasons = {"menos_de_duas_odds": 0}
    if raw is None or raw.empty:
        cols = ["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]
        return pd.DataFrame(columns=cols), reasons

    df = _clean_columns(raw)
    th = _pick(df, THOME) or "team_home"
    ta = _pick(df, TAWAY) or "team_away"
    mk = _pick(df, MKEY)

    sel_col   = _pick(df, SEL_NAME)
    price_col = _pick(df, PRICE)

    has_spread = (sel_col != "") and (price_col != "")
    has_direct = any(c in df.columns for c in HOME + DRAW + AWAY)

    # ----- modo "longo": pivot por seleção -----
    if has_spread and not has_direct:
        if not mk:
            mk = "match_key"
            df[mk] = _build_match_key(df, th, ta)

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
            "price": _to_number(df[price_col]),
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

    # ----- modo "colunas": ***AGORA AGREGA POR MATCH_KEY*** -----
    else:
        ch = _pick(df, HOME)
        cd = _pick(df, DRAW)
        ca = _pick(df, AWAY)

        work = pd.DataFrame({
            "team_home": df.get(th),
            "team_away": df.get(ta),
        })
        if not mk:
            work["match_key"] = _build_match_key(df, th, ta)
        else:
            work["match_key"] = df[mk].astype(str)

        work["odds_home"] = _to_number(df[ch]) if ch else pd.NA
        work["odds_draw"] = _to_number(df[cd]) if cd else pd.NA
        work["odds_away"] = _to_number(df[ca]) if ca else pd.NA

        # >>>>>>>>> AGREGA por match_key (pega o melhor valor não-nulo) <<<<<<<<<
        # time_home/away: pega o 1º não nulo da chave
        firsts = work.groupby("match_key")[["team_home","team_away"]].first()
        agg = work.groupby("match_key")[["odds_home","odds_draw","odds_away"]].max(min_count=1)
        out = firsts.join(agg, how="outer").reset_index()

    out["__valid"] = out.apply(_valid_row, axis=1)
    reasons["menos_de_duas_odds"] = int((~out["__valid"]).sum())
    out = out[out["__valid"]].drop(columns="__valid")
    out["__provider"] = tag
    return out, reasons

def _read_provider(path: str, tag: str) -> Tuple[pd.DataFrame, Dict[str,int]]:
    reasons = {"menos_de_duas_odds": 0}
    if not path:
        print(f"[consensus-safe] AVISO: {tag} não encontrado em /out nem /in.")
        return pd.DataFrame(), reasons
    try:
        raw = pd.read_csv(path)
    except Exception as e:
        print(f"[consensus-safe] ERRO ao ler {path}: {e}", file=sys.stderr)
        return pd.DataFrame(), reasons

    norm, reasons = _normalize_provider(raw, tag)
    print(f"[consensus-safe] lido {os.path.basename(path)} -> {len(raw)} linhas; válidas: {len(norm)}")
    if reasons.get("menos_de_duas_odds",0) > 0 and not norm.empty:
        print(f"[consensus-safe] motivos inválidos {tag}: {reasons}")
    return norm, reasons

def _merge_best(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    # se só um provedor existe, retorna ele
    if a.empty and b.empty:
        return pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"])
    if b.empty: return a.drop(columns=["__provider"], errors="ignore")
    if a.empty: return b.drop(columns=["__provider"], errors="ignore")

    cols = ["match_key","team_home","team_away","odds_home","odds_draw","odds_away"]
    m = pd.merge(a[cols], b[cols], on=["match_key","team_home","team_away"], how="outer", suffixes=("_a","_b"))

    def pick(col, r):
        va, vb = r.get(col+"_a"), r.get(col+"_b")
        vals = [v for v in [va, vb] if pd.notna(v)]
        return max(vals) if vals else pd.NA

    out = pd.DataFrame({
        "team_home": m["team_home"],
        "team_away": m["team_away"],
        "match_key": m["match_key"],
        "odds_home": m.apply(lambda r: pick("odds_home", r), axis=1),
        "odds_draw": m.apply(lambda r: pick("odds_draw", r), axis=1),
        "odds_away": m.apply(lambda r: pick("odds_away", r), axis=1),
    })
    # valida de novo
    out["__valid"] = out.apply(_valid_row, axis=1)
    out = out[out["__valid"]].drop(columns="__valid")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = _ensure_out_dir(args.rodada)
    p_out = os.path.join(out_dir, "odds_consensus.csv")

    # procura arquivos; basta 1 provedor
    p_theo = _probe("odds_theoddsapi.csv", args.rodada)
    p_api  = _probe("odds_apifootball.csv", args.rodada)

    df_theo, _ = _read_provider(p_theo, "theoddsapi")
    df_api , _ = _read_provider(p_api , "apifootball")
    df = _merge_best(df_theo, df_api)

    total = len(df)
    print(f"[consensus-safe] consenso final: {total} linhas")
    if total == 0:
        # escreve CSV vazio com header esperado (mantém semântica do pipeline)
        pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]).to_csv(p_out, index=False)
        print("[consensus-safe] ERRO: nenhuma linha de odds válida. Abortando.")
        sys.exit(10)

    print(f"[consensus-safe] AMOSTRA (top 5): {df.head(5).to_dict(orient='records')}")
    df.to_csv(p_out, index=False)
    print(f"[consensus-safe] OK -> {p_out} ({total} linhas)")

if __name__ == "__main__":
    main()
