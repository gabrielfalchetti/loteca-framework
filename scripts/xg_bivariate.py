# scripts/xg_bivariate.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xg_bivariate: MODELO CIENTÍFICO BIVARIATE POISSON DINÂMICO (V2.1)

OBJETIVO:
  Gerar P_True (Probabilidade Verdadeira) lendo as Forças Dinâmicas (alfa/beta) 
  do dynamic_params.json e usando a fórmula log-linear do Bivariate Poisson.

Entradas CRÍTICAS:
  - dynamic_params.json  [Parâmetros alfa e beta de todos os times]
"""

import os
import re
import sys
import argparse
from unicodedata import normalize as _ucnorm
import pandas as pd
import numpy as np
from scipy.stats import poisson
import json
import math # Necessário para math.exp

# --- CONSTANTES CIENTÍFICAS ---
MAX_GOALS = 6
DYNAMIC_PARAMS_FILE = "dynamic_params.json"
# ------------------------------

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

# --- FUNÇÕES CIENTÍFICAS ---

def biv_poisson_match_probs(lambda_h: float, lambda_a: float) -> tuple[float, float, float]:
    """
    Núcleo do Modelo Poisson (Independente para simplificação inicial).
    Calcula P_Home, P_Draw, P_Away somando as probabilidades de placares.
    """
    p_home, p_draw, p_away = 0.0, 0.0, 0.0
    
    for h in range(MAX_GOALS):
        for a in range(MAX_GOALS):
            # P(h, a) = P(h | lambda_h) * P(a | lambda_a)
            p_score = poisson.pmf(h, lambda_h) * poisson.pmf(a, lambda_a)
            
            if h > a:
                p_home += p_score
            elif h == a:
                p_draw += p_score
            else:
                p_away += p_score

    total_final = p_home + p_draw + p_away
    if total_final > 0:
        p_home /= total_final
        p_draw /= total_final
        p_away /= total_final
    
    return round(p_home, 6), round(p_draw, 6), round(p_away, 6)

def calculate_dynamic_bivariate_poisson_probs(
    team_home: str, team_away: str, params: dict
) -> tuple[float, float, float]:
    """
    HOOK CIENTÍFICO CHAVE: Calcula lambdas a partir dos parâmetros dinâmicos.
    (Implementação da fórmula log-linear de Koopman/Dixon & Coles)
    """
    
    home_key = norm_key_tokens(team_home)
    away_key = norm_key_tokens(team_away)
    
    team_params = params.get("team_params", {})
    fixed_effects = params.get("league_fixed_effects", {})
    
    home_data = team_params.get(home_key)
    away_data = team_params.get(away_key)
    # Pega o coeficiente de vantagem de casa (δ)
    home_advantage = fixed_effects.get("home_advantage", 0.0) 
    
    if not home_data or not away_data:
        log("WARN", f"Parâmetros dinâmicos ausentes para {team_home} ({home_key}) ou {team_away} ({away_key}). Pulando.")
        return None, None, None

    # Parâmetros de Força (em log-escala):
    alpha_h = home_data.get("alpha", 0.0) # Ataque Home
    beta_h = home_data.get("beta", 0.0)   # Defesa Home
    alpha_a = away_data.get("alpha", 0.0)  # Ataque Away
    beta_a = away_data.get("beta", 0.0)    # Defesa Away

    # Fórmulas Log-Lineares:
    # λ_home = exp( home_advantage + alpha_home - beta_away )
    lambda_h = math.exp(home_advantage + alpha_h - beta_a)

    # λ_away = exp( alpha_away - beta_home )
    lambda_a = math.exp(alpha_a - beta_h)

    if lambda_h <= 0 or lambda_a <= 0:
        log("WARN", f"Lambda inválido (<=0) para {team_home} vs {team_away}. Pulando.")
        return None, None, None

    return biv_poisson_match_probs(lambda_h, lambda_a)


def build_model_predictions(rodada: str) -> pd.DataFrame:
    """
    REVISÃO: Gera probabilidades usando o Modelo Bivariate Poisson, lendo os parâmetros dinâmicos.
    """
    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    oc_path = os.path.join(rodada, "odds_consensus.csv")
    params_path = os.path.join(rodada, DYNAMIC_PARAMS_FILE)

    wl = read_csv_safe(wl_path)
    oc = read_csv_safe(oc_path)

    # --- CARREGAMENTO DOS PARÂMETROS DINÂMICOS ---
    try:
        with open(params_path, 'r', encoding='utf-8') as f:
            dynamic_params = json.load(f)
    except FileNotFoundError:
        log("CRITICAL", f"Arquivo {DYNAMIC_PARAMS_FILE} ausente. Treinamento falhou.")
        sys.exit(8)
    except json.JSONDecodeError as e:
        log("CRITICAL", f"Falha lendo {DYNAMIC_PARAMS_FILE} (JSON inválido): {e}")
        sys.exit(8)
    
    if not dynamic_params:
        log("CRITICAL", "Parâmetros dinâmicos vazios. Impossível prever.")
        sys.exit(8)
    
    # -----------------------------------------------

    # Lógica de merge e limpeza (mantida)
    wl = wl.rename(columns={"home":"team_home","away":"team_away"})[["match_id","team_home","team_away"]].copy()
    wl["key"] = wl["team_home"].apply(norm_key_tokens) + "|" + wl["team_away"].apply(norm_key_tokens)
    oc = oc[list(REQ_ODDS)].copy()
    oc["key"] = oc["team_home"].apply(norm_key_tokens) + "|" + oc["team_away"].apply(norm_key_tokens)

    wl_idx = wl.drop_duplicates(subset=["key"]).set_index("key")
    oc_idx = oc.drop_duplicates(subset=["key"]).set_index("key")
    inter_keys = [k for k in oc_idx.index if k in wl_idx.index]

    rows = []
    
    for k in inter_keys:
        wlr = wl_idx.loc[k]
        ocr = oc_idx.loc[k]

        oh = secure_float(ocr["odds_home"])
        od = secure_float(ocr["odds_draw"])
        oa = secure_float(ocr["odds_away"])
        
        # --- AQUI: O SEU EDGE PREDITIVO É GERADO ---
        ph, pdr, pa = calculate_dynamic_bivariate_poisson_probs(
            wlr["team_home"], wlr["team_away"], dynamic_params
        )
        # ------------------------------------------

        if None in (oh, od, oa, ph, pdr, pa): 
            continue

        rows.append({
            "match_id": wlr["match_id"],
            "team_home": wlr["team_home"],
            "team_away": wlr["team_away"],
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
            "p_home": ph,
            "p_draw": pdr,
            "p_away": pa,
        })
    
    if not rows:
        log("CRITICAL", "Nenhuma linha gerada (sem match entre whitelist e odds_consensus).")
        sys.exit(8)

    return pd.DataFrame(rows, columns=OUTPUT_COLS)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    rodada = args.rodada
    out_path = os.path.join(rodada, "xg_bivariate.csv")
    
    log("INFO", "Iniciando previsão com Modelo Científico Bivariate Poisson Dinâmico.")
    
    df = build_model_predictions(rodada)

    # Salva resultado
    df[OUTPUT_COLS].to_csv(out_path, index=False)
    log("INFO", f"xg_bivariate gerado: {out_path}  linhas={len(df)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
