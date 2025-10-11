#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
calibrate_probs: gera probs_calibrated.csv

Prioridade de fonte:
  1) xg_bivariate.csv
  2) xg_univariate.csv
  3) odds_consensus.csv + matches_whitelist.csv

Saída garantida:
  probs_calibrated.csv com colunas:
    [match_id, team_home, team_away,
     odds_home, odds_draw, odds_away,
     p_home, p_draw, p_away]
"""

import os
import re
import sys
import argparse
from unicodedata import normalize as _ucnorm
import pandas as pd

REQ_WL = {"match_id", "home", "away"}
REQ_OC = {"team_home", "team_away", "odds_home", "odds_draw", "odds_away"}
REQ_XG = {"match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "p_home", "p_draw", "p_away"}

STOPWORD_TOKENS = {
    "aa","ec","ac","sc","fc","afc","cf","ca","cd","ud",
    "sp","pr","rj","rs","mg","go","mt","ms","pa","pe","pb","rn","ce","ba","al","se","pi","ma","df","es","sc",
}

def log(level, msg):
    prefix = f"[{level}]" if level != "INFO" else ""
    print(f"[calibrate]{prefix} {msg}", flush=True)

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

def implied_probs(oh, od, oa):
    ih = (1.0 / oh) if oh and oh > 0 else None
    idr = (1.0 / od) if od and od > 0 else None
    ia = (1.0 / oa) if oa and oa > 0 else None
    if None in (ih, idr, ia):
        return None, None, None
    s = ih + idr + ia
    if s <= 0:
        return None, None, None
    return ih / s, idr / s, ia / s

def read_csv_safe(path: str, required=None) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    if required and not set(required).issubset(df.columns):
        raise ValueError(f"{os.path.basename(path)} sem colunas {sorted(set(required) - set(df.columns))}")
    return df

def build_from_odds_consensus(rodada: str) -> pd.DataFrame:
    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    oc_path = os.path.join(rodada, "odds_consensus.csv")

    wl = read_csv_safe(wl_path, REQ_WL).rename(columns={"home":"team_home","away":"team_away"})[
        ["match_id","team_home","team_away"]
    ].copy()
    oc = read_csv_safe(oc_path, REQ_OC)[["team_home","team_away","odds_home","odds_draw","odds_away"]].copy()

    wl["key"] = wl["team_home"].apply(norm_key_tokens) + "|" + wl["team_away"].apply(norm_key_tokens)
    oc["key"] = oc["team_home"].apply(norm_key_tokens) + "|" + oc["team_away"].apply(norm_key_tokens)

    wl_idx = wl.drop_duplicates(subset=["key"]).set_index("key")
    oc_idx = oc.drop_duplicates(subset=["key"]).set_index("key")

    rows = []
    matched = 0

    for k in oc_idx.index:
        if k not in wl_idx.index:
            continue
        wlr = wl_idx.loc[k]
        ocr = oc_idx.loc[k]
        oh = secure_float(ocr["odds_home"])
        od = secure_float(ocr["odds_draw"])
        oa = secure_float(ocr["odds_away"])
        ph, pdr, pa = implied_probs(oh, od, oa)
        if None in (oh, od, oa, ph, pdr, pa):
            continue
        matched += 1
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

    if matched == 0:
        # Fallback por join crua
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
        raise RuntimeError("Nenhuma linha gerada a partir de odds_consensus + whitelist")

    return pd.DataFrame(rows, columns=[
        "match_id","team_home","team_away","odds_home","odds_draw","odds_away","p_home","p_draw","p_away"
    ])

def coerce_and_clip(df: pd.DataFrame) -> pd.DataFrame:
    # Tipos
    for c in ["odds_home","odds_draw","odds_away","p_home","p_draw","p_away"]:
        if c in df.columns:
            df[c] = df[c].apply(secure_float)

    # Normaliza e clipa probabilidades
    def _fix_row(r):
        ph, pd, pa = r["p_home"], r["p_draw"], r["p_away"]
        vals = [ph, pd, pa]
        if any(v is None or v < 0 for v in vals):
            return None, None, None
        s = sum(vals)
        if s <= 0:
            return None, None, None
        ph, pd, pa = ph/s, pd/s, pa/s
        # clipping leve pra evitar zeros exatos
        eps = 1e-9
        ph = min(max(ph, eps), 1.0 - 2*eps)
        pd = min(max(pd, eps), 1.0 - 2*eps)
        pa = min(max(pa, eps), 1.0 - 2*eps)
        s2 = ph + pd + pa
        return ph/s2, pd/s2, pa/s2

    out = df.copy()
    trip = out.apply(lambda r: _fix_row(r), axis=1, result_type="expand")
    out[["p_home","p_draw","p_away"]] = trip
    out = out.dropna(subset=["p_home","p_draw","p_away"])
    return out

def choose_base(rodada: str) -> pd.DataFrame:
    bi = os.path.join(rodada, "xg_bivariate.csv")
    uni = os.path.join(rodada, "xg_univariate.csv")

    if os.path.isfile(bi):
        log("INFO", "Usando xg_bivariate.csv como base")
        df = read_csv_safe(bi)
        # Se não tiver todas, tenta complementar com odds_consensus
        if not REQ_XG.issubset(df.columns):
            log("WARN", "xg_bivariate.csv incompleto; recomputando via odds_consensus …")
            return build_from_odds_consensus(rodada)
        return df[list(REQ_XG)].copy()

    if os.path.isfile(uni):
        log("INFO", "Usando xg_univariate.csv como base")
        df = read_csv_safe(uni)
        if not REQ_XG.issubset(df.columns):
            log("WARN", "xg_univariate.csv incompleto; recomputando via odds_consensus …")
            return build_from_odds_consensus(rodada)
        return df[list(REQ_XG)].copy()

    log("WARN", "predictions_market ausente/incompleto. Tentando odds_consensus.csv ...")
    return build_from_odds_consensus(rodada)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    rodada = args.rodada
    out_path = os.path.join(rodada, "probs_calibrated.csv")

    print("===================================================")
    print("[calibrate] INICIANDO CALIBRAÇÃO DE PROBABILIDADES")
    print(f"[calibrate] Diretório de rodada : {rodada}")
    print("===================================================")

    try:
        base = choose_base(rodada)
    except FileNotFoundError as e:
        log("CRITICAL", f"Arquivo não encontrado: {e}")
        sys.exit(9)
    except Exception as e:
        log("CRITICAL", f"Falha preparando base: {e}")
        sys.exit(9)

    # “Calibração” identidade (normalização e clipping)
    df = base.rename(columns={"home":"team_home","away":"team_away"}).copy()
    # Garante colunas de odds (se base veio de XG já tem; se não, calcula não-destrutivo)
    for c in ["odds_home","odds_draw","odds_away"]:
        if c not in df.columns:
            df[c] = None

    df = df[["match_id","team_home","team_away","odds_home","odds_draw","odds_away","p_home","p_draw","p_away"]]
    df = coerce_and_clip(df)

    if df.empty:
        log("CRITICAL", "Nenhuma linha calibrada gerada.")
        # Mesmo assim grava cabeçalho para não travar totalmente o pipeline
        pd.DataFrame(columns=["match_id","team_home","team_away","odds_home","odds_draw","odds_away","p_home","p_draw","p_away"]).to_csv(out_path, index=False)
        sys.exit(9)

    df.to_csv(out_path, index=False)
    print("[ok] Calibração concluída com sucesso.")
    return 0

if __name__ == "__main__":
    sys.exit(main())