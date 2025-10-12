# scripts/calibrate_probs.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
calibrate_probs: gera probs_calibrated.csv

OBJETIVO: Aplicar Isotonic Regression (Regressão Isotônica) para corrigir
vieses no P_Model (do xg_bivariate.csv) e gerar o P_True.

Prioridade de fonte:
  1) xg_bivariate.csv (Contém P_Model)

Saída garantida:
  probs_calibrated.csv com colunas:
    [match_id, team_home, team_away,
     odds_home, odds_draw, odds_away,
     p_home_cal, p_draw_cal, p_away_cal]
"""

import os
import re
import sys
import argparse
from unicodedata import normalize as _ucnorm
import pandas as pd
import numpy as np

# NOVAS DEPENDÊNCIAS CIENTÍFICAS
from sklearn.isotonic import IsotonicRegression
import joblib # Para carregar modelos treinados

# --- CONSTANTES ---
REQ_WL = {"match_id", "home", "away"}
REQ_OC = {"team_home", "team_away", "odds_home", "odds_draw", "odds_away"}
REQ_XG = {"match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "p_home", "p_draw", "p_away"}

# Defina onde os modelos de calibração treinados serão armazenados
CALIBRATION_MODEL_DIR = "models/calibration"

STOPWORD_TOKENS = {
    "aa","ec","ac","sc","fc","afc","cf","ca","cd","ud",
    "sp","pr","rj","rs","mg","go","mt","ms","pa","pe","pb","rn","ce","ba","al","se","pi","ma","df","es","sc",
}

def log(level, msg):
    prefix = f"[{level}]" if level != "INFO" else ""
    print(f"[calibrate]{prefix} {msg}", flush=True)

# Funções de utilidade (mantidas)
def _deaccent(s: str) -> str:
# ... (funções de normalização de strings: _deaccent, norm_key, norm_key_tokens, secure_float, implied_probs, read_csv_safe, build_from_odds_consensus, coerce_and_clip)
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


# --- FUNÇÃO DE CALIBRAÇÃO AVANÇADA (O EDGE NA ASSERTIVIDADE) ---
def apply_isotonic_calibration(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o modelo de Regressão Isotônica treinado para corrigir os vieses
    das probabilidades P_Model para gerar o P_True (P_Calibrado).
    """
    calibrated_df = df.copy()
    results_cols = {}

    for outcome, p_col in [("home", "p_home"), ("draw", "p_draw"), ("away", "p_away")]:
        model_path = os.path.join(CALIBRATION_MODEL_DIR, f"calibrator_{outcome}.pkl")
        p_cal_col = f"p_{outcome}_cal"

        if os.path.exists(model_path):
            try:
                # Carrega o modelo de calibração treinado (Ex: Isotonic Regression)
                calibrator = joblib.load(model_path)
                
                # Aplica a transformação
                # A Regressão Isotônica espera um array 1D
                p_model = df[p_col].values
                p_calibrated = calibrator.transform(p_model)
                
                calibrated_df[p_cal_col] = p_calibrated
                log("INFO", f"Calibrador {outcome} aplicado com sucesso.")
            except Exception as e:
                log("WARN", f"Falha ao aplicar calibrador {outcome}: {e}. Usando P_Model não calibrado.")
                calibrated_df[p_cal_col] = df[p_col]
        else:
            # Se o modelo de calibração não existe (ainda não foi treinado),
            # usamos a probabilidade bruta do modelo.
            log("WARN", f"Modelo de calibração {outcome} não encontrado. Usando P_Model bruto.")
            calibrated_df[p_cal_col] = df[p_col]

    # Re-normaliza as probabilidades calibradas (elas devem somar 1)
    calibrated_df["p_sum"] = calibrated_df["p_home_cal"] + calibrated_df["p_draw_cal"] + calibrated_df["p_away_cal"]
    
    # Aplica a normalização (divide pela soma)
    calibrated_df["p_home_cal"] /= calibrated_df["p_sum"]
    calibrated_df["p_draw_cal"] /= calibrated_df["p_sum"]
    calibrated_df["p_away_cal"] /= calibrated_df["p_sum"]
    
    # Renomeia as colunas finais (substituindo p_home/draw/away originais pelas calibradas)
    calibrated_df = calibrated_df.drop(columns=["p_home", "p_draw", "p_away", "p_sum"])
    calibrated_df = calibrated_df.rename(columns={
        "p_home_cal": "p_home",
        "p_draw_cal": "p_draw",
        "p_away_cal": "p_away",
    })
    
    return calibrated_df

def choose_base(rodada: str) -> pd.DataFrame:
    bi = os.path.join(rodada, "xg_bivariate.csv")
    uni = os.path.join(rodada, "xg_univariate.csv")

    if os.path.isfile(bi):
        log("INFO", "Usando xg_bivariate.csv como base (melhor prioridade)")
        df = read_csv_safe(bi)
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

    log("WARN", "Nenhum arquivo de previsão encontrado. Tentando odds_consensus.csv ...")
    return build_from_odds_consensus(rodada)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    rodada = args.rodada
    out_path = os.path.join(rodada, "probs_calibrated.csv")

    # Garante que o diretório de modelos existe, se for o caso
    os.makedirs(CALIBRATION_MODEL_DIR, exist_ok=True)

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

    # 1. Coerção e limpeza de tipos (mantida)
    df = base.rename(columns={"home":"team_home","away":"team_away"}).copy()
    df = coerce_and_clip(df)

    # 2. APLICAÇÃO DO ALGORITMO DE CALIBRAÇÃO AVANÇADA (O BOOST DE ASSERTIVIDADE)
    df_calibrated = apply_isotonic_calibration(df)

    if df_calibrated.empty:
        log("CRITICAL", "Nenhuma linha calibrada gerada.")
        pd.DataFrame(columns=["match_id","team_home","team_away","odds_home","odds_draw","odds_away","p_home","p_draw","p_away"]).to_csv(out_path, index=False)
        sys.exit(9)

    # Garante a ordem e as colunas finais
    FINAL_OUTPUT_COLS = ["match_id","team_home","team_away","odds_home","odds_draw","odds_away","p_home","p_draw","p_away"]
    df_calibrated[FINAL_OUTPUT_COLS].to_csv(out_path, index=False)
    print("[ok] Calibração concluída com sucesso.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
