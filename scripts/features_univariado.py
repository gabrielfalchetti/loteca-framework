#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cria features univariadas a partir de odds de consenso.

Entrada obrigatória:
  <OUT_DIR>/odds_consensus.csv
  Colunas aceitas (aliases normalizados internamente):
    Teams:   home | away  (aceita team_home/home_team/mandante e team_away/away_team/visitante)
    Odds:    odd_home | odd_draw | odd_away  (aceita odds_home/odds_draw/odds_away etc.)
    Probs:   imp_home | imp_draw | imp_away  (opcional; se ausente, calcula de 1/odds com overround)

Saída:
  <OUT_DIR>/features_univariado.csv
"""

import argparse, os, sys, json, unicodedata
from typing import List, Tuple
import pandas as pd
import numpy as np

DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

HOME_ALIASES = ["home","team_home","home_team","mandante"]
AWAY_ALIASES = ["away","team_away","away_team","visitante"]
OH_ALIASES   = ["odd_home","odds_home","home_odds","o1","price_home","h2h_home"]
OD_ALIASES   = ["odd_draw","odds_draw","draw_odds","ox","price_draw","h2h_draw"]
OA_ALIASES   = ["odd_away","odds_away","away_odds","o2","price_away","h2h_away"]
PH_ALIASES   = ["imp_home","p_home","prob_home"]
PD_ALIASES   = ["imp_draw","p_draw","prob_draw"]
PA_ALIASES   = ["imp_away","p_away","prob_away"]

def log(msg:str): print(f"[univariado] {msg}")
def die(code:int,msg:str):
    log(msg); sys.exit(code)

def first_col(df: pd.DataFrame, cands: List[str]) -> str:
    lower = {c.lower(): c for c in df.columns}
    for k in cands:
        if k.lower() in lower: return lower[k.lower()]
    # por substring
    for c in df.columns:
        for k in cands:
            if k.lower() in c.lower(): return c
    return ""

def strip_accents_lower(s:str) -> str:
    if pd.isna(s): return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s.lower()

def make_key(h:str,a:str)->str: return f"{strip_accents_lower(h)}__vs__{strip_accents_lower(a)}"

def load_consensus(path:str)->pd.DataFrame:
    if not os.path.exists(path): die(21,f"arquivo não encontrado: {path}")
    try:
        df = pd.read_csv(path)
        if df.empty or df.shape[1]==0: die(21,f"arquivo vazio/sem colunas: {path}")
        return df
    except Exception as e:
        die(21,f"falha ao ler {path}: {e}")

def normalize(df: pd.DataFrame)->Tuple[pd.DataFrame,dict]:
    ch = first_col(df, HOME_ALIASES); ca = first_col(df, AWAY_ALIASES)
    if not ch or not ca: die(21,"colunas de times (home/away) não identificadas")

    oh = first_col(df, OH_ALIASES); od = first_col(df, OD_ALIASES); oa = first_col(df, OA_ALIASES)
    if not (oh and od and oa):
        # odds são obrigatórias (sem elas não calculamos nada)
        die(21,"colunas de odds (odd_home/odd_draw/odd_away) não identificadas")

    ph = first_col(df, PH_ALIASES); pd_ = first_col(df, PD_ALIASES); pa = first_col(df, PA_ALIASES)

    out = pd.DataFrame()
    out["home"] = df[ch].astype(str)
    out["away"] = df[ca].astype(str)

    def fodd(x):
        try:
            v = float(x)
            return v if v>1.0001 else np.nan
        except: return np.nan

    out["odd_home"] = df[oh].map(fodd)
    out["odd_draw"] = df[od].map(fodd)
    out["odd_away"] = df[oa].map(fodd)

    # probs implícitas (opcional)
    if ph: out["imp_home"] = pd.to_numeric(df[ph], errors="coerce")
    if pd_: out["imp_draw"] = pd.to_numeric(df[pd_], errors="coerce")
    if pa: out["imp_away"] = pd.to_numeric(df[pa], errors="coerce")

    out["match_key"] = out.apply(lambda r: make_key(r["home"], r["away"]), axis=1)
    keep = out[["odd_home","odd_draw","odd_away"]].notna().all(axis=1)
    out = out[keep].reset_index(drop=True)
    if out.empty: die(21,"nenhuma linha válida após normalização")

    used = {"home":ch,"away":ca,"odd_home":oh,"odd_draw":od,"odd_away":oa,"imp_home":ph,"imp_draw":pd_,"imp_away":pa}
    return out, used

def probs_from_odds(df: pd.DataFrame) -> pd.DataFrame:
    has_imp = df.filter(regex=r"^imp_").notna().any(axis=None)
    if not has_imp:
        inv = pd.DataFrame({
            "h": 1.0/df["odd_home"],
            "x": 1.0/df["odd_draw"],
            "a": 1.0/df["odd_away"],
        })
        s = inv.sum(axis=1)
        df["imp_home"] = inv["h"]/s
        df["imp_draw"] = inv["x"]/s
        df["imp_away"] = inv["a"]/s
    else:
        # completa faltantes com 1/odds normalizado
        inv = pd.DataFrame({
            "h": 1.0/df["odd_home"],
            "x": 1.0/df["odd_draw"],
            "a": 1.0/df["odd_away"],
        })
        s = inv.sum(axis=1)
        for tgt,src in [("imp_home","h"),("imp_draw","x"),("imp_away","a")]:
            if tgt not in df.columns: df[tgt]=np.nan
            miss = df[tgt].isna()
            df.loc[miss, tgt] = (inv[src]/s)[miss]
    return df

def add_univariate_features(df: pd.DataFrame)->pd.DataFrame:
    # overround e fair odds
    inv_sum = (1/df["odd_home"]) + (1/df["odd_draw"]) + (1/df["odd_away"])
    df["overround"] = inv_sum
    df["fair_p_home"] = (1/df["odd_home"]) / inv_sum
    df["fair_p_draw"] = (1/df["odd_draw"]) / inv_sum
    df["fair_p_away"] = (1/df["odd_away"]) / inv_sum

    # gaps e skew
    df["gap_home_away"] = df["imp_home"] - df["imp_away"]
    df["gap_top_second"] = (df[["imp_home","imp_draw","imp_away"]].max(axis=1) -
                            df[["imp_home","imp_draw","imp_away"]].apply(
                                lambda r: r.sort_values(ascending=False).iloc[1], axis=1))

    # log-odds (logit) — estabilidade numérica
    eps = 1e-9
    for col in ["imp_home","imp_draw","imp_away"]:
        df[f"logit_{col}"] = np.log((df[col].clip(eps,1-eps))/(1-df[col].clip(eps,1-eps)))

    # favoritos
    df["fav_label"] = df[["imp_home","imp_draw","imp_away"]].idxmax(axis=1).map(
        {"imp_home":"1","imp_draw":"X","imp_away":"2"}
    )

    # entropia (incerteza)
    p = df[["imp_home","imp_draw","imp_away"]].clip(1e-12,1)
    df["entropy_bits"] = -(p*np.log2(p)).sum(axis=1)

    # sinal de valor bruto: p_implícita - fair_p
    df["value_home"] = df["imp_home"] - df["fair_p_home"]
    df["value_draw"] = df["imp_draw"] - df["fair_p_draw"]
    df["value_away"] = df["imp_away"] - df["fair_p_away"]

    # controles
    df["has_probs"] = df[["imp_home","imp_draw","imp_away"]].notna().all(axis=1).astype(int)
    df["has_all_odds"] = df[["odd_home","odd_draw","odd_away"]].notna().all(axis=1).astype(int)

    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada: ex. data/out/<RID>")
    ap.add_argument("--season", required=False, help="(Opcional) temporada para meta", default="")
    ap.add_argument("--debug", action="store_true", default=False)
    args = ap.parse_args()

    out_dir = args.rodada
    if not os.path.isdir(out_dir): die(21,f"OUT_DIR inexistente: {out_dir}")

    in_p = os.path.join(out_dir, "odds_consensus.csv")
    df_raw = load_consensus(in_p)
    df, used = normalize(df_raw)
    df = probs_from_odds(df)
    df = add_univariate_features(df)

    # colunas finais
    cols = [
        "match_key","home","away",
        "odd_home","odd_draw","odd_away",
        "imp_home","imp_draw","imp_away",
        "overround","fair_p_home","fair_p_draw","fair_p_away",
        "gap_home_away","gap_top_second",
        "logit_imp_home","logit_imp_draw","logit_imp_away",  # nomes harmonizados
        "fav_label","entropy_bits",
        "value_home","value_draw","value_away",
        "has_probs","has_all_odds"
    ]
    # renomeia logits conforme add_univariate
    rename_map = {
        "logit_imp_home":"logit_imp_home",
        "logit_imp_draw":"logit_imp_draw",
        "logit_imp_away":"logit_imp_away",
    }
    # já estão com esses nomes; apenas garante presença
    for c in cols:
        if c not in df.columns: df[c]=np.nan

    out_p = os.path.join(out_dir, "features_univariado.csv")
    df[cols].to_csv(out_p, index=False)

    if not os.path.exists(out_p) or os.path.getsize(out_p)==0:
        die(21,"features_univariado.csv não gerado")

    # meta
    meta = {
        "rows": int(df.shape[0]),
        "season": args.season,
        "source": os.path.relpath(in_p),
        "used_columns": used
    }
    with open(os.path.join(out_dir,"features_univariado_meta.json"),"w",encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    log(f"OK -> {out_p} ({df.shape[0]} jogos)")
    if args.debug or DEBUG:
        print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()