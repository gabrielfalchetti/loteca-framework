# scripts/xg_bivariate.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xg_bivariate: baseline bivariado (placeholder)

Objetivo imediato: garantir a produção de xg_bivariate.csv para o pipeline.
Comportamento:
  1) Se xg_univariate.csv existir em --rodada, reaproveita e grava como xg_bivariate.csv.
  2) Caso contrário, recomputa probabilidades implícitas a partir de odds_consensus.csv
     (mesma lógica do univariate) e grava o resultado.

Entradas esperadas em --rodada/:
  - matches_whitelist.csv  [match_id, home, away]
  - odds_consensus.csv     [team_home, team_away, odds_home, odds_draw, odds_away]
  - (opcional) xg_univariate.csv

Saída:
  - xg_bivariate.csv [match_id, team_home, team_away, odds_home, odds_draw, odds_away, p_home, p_draw, p_away]
"""

import os
import re
import sys
import argparse
from unicodedata import normalize as _ucnorm
import pandas as pd

REQ_WL = {"match_id", "home", "away"}
REQ_ODDS = {"team_home", "team_away", "odds_home", "odds_draw", "odds_away"}
OUTPUT_COLS = [
    "match_id","team_home","team_away","odds_home","odds_draw","odds_away","p_home","p_draw","p_away"
]

STOPWORD_TOKENS = {
    "aa","ec","ac","sc","fc","afc","cf","ca","cd","ud",
    "sp","pr","rj","rs","mg","go","mt","ms","pa","pe","pb","rn","ce","ba","al","se","pi","ma","df","es","sc",
}

def log(level, msg):
    tag = "" if level == "INFO" else f"[{level}] "
    print(f"[xg_bi]{tag}{msg}", flush=True)

def _deaccent(s: str) -> str:
    return _ucnorm("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")

def norm_key(name: str) -> str:
    s = _deaccent(name).lower()
    s = s.replace("&", " e ")
    s = re.sub(r"[/()\-_.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_key_tokens(name: str) -> str:
    toks = [t for t in re.split(r"\s+", norm_key(name)) if t and t not in STOPWORD_TOKENS]
    return " ".join(toks)

def secure_float(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        log("CRITICAL", f"Arquivo não encontrado: {path}")
        sys.exit(8)
    try:
        return pd.read_csv(path)
    except Exception as e:
        log("CRITICAL", f"Falha lendo {path}: {e}")
        sys.exit(8)

def implied_probs(oh, od, oa):
    ih = (1.0 / oh) if oh and oh > 0 else None
    idr = (1.0 / od) if od and od > 0 else None
    ia = (1.0 / oa) if oa and oa > 0 else None
    if None in (ih, idr, ia):
        return None, None, None
    s = ih + idr + ia
    if s <= 0:
        return None, None, None
    return ih / s, idr / s, ia / s  # p_home, p_draw, p_away

def build_from_consensus(rodada: str) -> pd.DataFrame:
    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    oc_path = os.path.join(rodada, "odds_consensus.csv")

    wl = read_csv_safe(wl_path)
    if not REQ_WL.issubset(set(wl.columns)):
        log("CRITICAL", f"Whitelist sem colunas necessárias: {REQ_WL}")
        sys.exit(8)

    oc = read_csv_safe(oc_path)
    if not REQ_ODDS.issubset(set(oc.columns)):
        faltantes = list(REQ_ODDS - set(oc.columns))
        log("CRITICAL", f"odds_consensus.csv sem colunas: {faltantes}")
        sys.exit(8)

    wl = wl.rename(columns={"home":"team_home","away":"team_away"})[["match_id","team_home","team_away"]].copy()
    wl["key"] = wl["team_home"].apply(norm_key_tokens) + "|" + wl["team_away"].apply(norm_key_tokens)

    oc = oc[list(REQ_ODDS)].copy()
    oc["key"] = oc["team_home"].apply(norm_key_tokens) + "|" + oc["team_away"].apply(norm_key_tokens)

    wl_idx = wl.drop_duplicates(subset=["key"]).set_index("key")
    oc_idx = oc.drop_duplicates(subset=["key"]).set_index("key")

    inter_keys = [k for k in oc_idx.index if k in wl_idx.index]

    rows = []
    missing_after_match = []

    for k in inter_keys:
        wlr = wl_idx.loc[k]
        ocr = oc_idx.loc[k]

        oh = secure_float(ocr["odds_home"])
        od = secure_float(ocr["odds_draw"])
        oa = secure_float(ocr["odds_away"])
        ph, pdr, pa = implied_probs(oh, od, oa)

        if None in (oh, od, oa, ph, pdr, pa):
            missing_after_match.append((wlr["match_id"], wlr["team_home"], wlr["team_away"]))
            continue

        rows.append({
            "match_id": wlr["match_id"],
            "team_home": wlr["team_home"],
            "team_away": wlr["team_away"],
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
            "p_home": round(ph, 6),
            "p_draw": round(pdr, 6),
            "p_away": round(pa, 6),
        })

    # Fallback: join direto se nada casou
    if not rows:
        log("WARN", "Nenhum match por chave normalizada; tentando fallback por strings cruas…")
        merged = wl.merge(oc, on=["team_home","team_away"], how="inner")
        for _, r in merged.iterrows():
            oh = secure_float(r["odds_home"])
            od = secure_float(r["odds_draw"])
            oa = secure_float(r["odds_away"])
            ph, pdr, pa = implied_probs(oh, od, oa)
            if None in (oh, od, oa, ph, pdr, pa):
                continue
            rows.append({
                "match_id": r["match_id"],
                "team_home": r["team_home"],
                "team_away": r["team_away"],
                "odds_home": oh,
                "odds_draw": od,
                "odds_away": oa,
                "p_home": round(ph, 6),
                "p_draw": round(pdr, 6),
                "p_away": round(pa, 6),
            })

    if not rows:
        log("CRITICAL", "Nenhuma linha gerada (sem match entre whitelist e odds_consensus).")
        sys.exit(8)

    out_df = pd.DataFrame(rows, columns=OUTPUT_COLS)
    if missing_after_match:
        log("WARN", f"Jogos sem probabilidade (pós-match): {len(missing_after_match)} -> {[m[0] for m in missing_after_match]}")
    return out_df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    rodada = args.rodada
    uni_path = os.path.join(rodada, "xg_univariate.csv")
    out_path = os.path.join(rodada, "xg_bivariate.csv")

    # 1) Se já existe o univariate, reaproveita (garante compatibilidade de colunas).
    if os.path.isfile(uni_path):
        try:
            df = pd.read_csv(uni_path)
            # Garante colunas mínimas
            cols_ok = set(OUTPUT_COLS).issubset(set(df.columns))
            if not cols_ok:
                log("WARN", "xg_univariate.csv não tem todas as colunas esperadas; recalculando via odds_consensus…")
                df = build_from_consensus(rodada)
        except Exception as e:
            log("WARN", f"Falha lendo xg_univariate.csv ({e}); recalculando via odds_consensus…")
            df = build_from_consensus(rodada)
    else:
        # 2) Recalcula via odds_consensus
        df = build_from_consensus(rodada)

    # Salva resultado
    df[OUTPUT_COLS].to_csv(out_path, index=False)
    log("INFO", f"xg_bivariate gerado: {out_path}  linhas={len(df)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())