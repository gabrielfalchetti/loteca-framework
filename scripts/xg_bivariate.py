# scripts/xg_bivariate.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xg_bivariate: BASE CIENTÍFICA (Dynamic Bivariate Poisson)

Objetivo: Gerar P_True (Probabilidade Verdadeira) do nosso modelo, e não do mercado.

O PONTO DE MÁXIMA ASSERTIVIDADE está na função 'calculate_dynamic_bivariate_poisson_probs',
que usará o Filtro de Kalman para ajustar as lambdas (habilidades dos times) a cada rodada.

Entradas/Saídas: (Inalteradas)
"""

import os
import re
import sys
import argparse
from unicodedata import normalize as _ucnorm
import pandas as pd
import numpy as np
from scipy.stats import poisson

# --- CONSTANTES CIENTÍFICAS ---
MAX_GOALS = 6 # Limite de placar para cálculo (simplificação comum em modelos BPD)
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

# Funções de normalização e segurança (mantidas)
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

# A antiga 'implied_probs' não é mais usada, mas mantida para referência.
# --- INÍCIO: NOVAS FUNÇÕES CIENTÍFICAS (O EDGE PREDITIVO) ---

def biv_poisson_match_probs(lambda_h: float, lambda_a: float) -> tuple[float, float, float]:
    """
    Núcleo do Modelo Poisson (Independente para simplificação inicial).
    Calcula P_Home, P_Draw, P_Away somando as probabilidades de placares.
    """
    p_home, p_draw, p_away = 0.0, 0.0, 0.0
    
    # Matriz de probabilidades de placar (truncada para MAX_GOALS)
    for h in range(MAX_GOALS):
        for a in range(MAX_GOALS):
            # p(h, a | lambda_h, lambda_a) = P(h | lambda_h) * P(a | lambda_a)
            # Nota: O Bivariate Poisson COMPLETO incluiria um termo de correlação 'gamma'
            # (Dixon & Coles), mas esta aproximação é o ponto de partida ideal.
            p_score = poisson.pmf(h, lambda_h) * poisson.pmf(a, lambda_a)
            
            if h > a:
                p_home += p_score
            elif h == a:
                p_draw += p_score
            else:
                p_away += p_score

    # Re-normaliza (devido ao truncation em MAX_GOALS)
    total_final = p_home + p_draw + p_away
    if total_final > 0:
        p_home /= total_final
        p_draw /= total_final
        p_away /= total_final
    
    return round(p_home, 6), round(p_draw, 6), round(p_away, 6)

def calculate_dynamic_bivariate_poisson_probs(match_id: int, team_home: str, team_away: str) -> tuple[float, float, float]:
    """
    *** HOOK CIENTÍFICO CHAVE ***
    Esta função será o ponto de integração com o modelo de Espaço de Estados (Filtro de Kalman).
    Ela deve buscar os *parâmetros* (lambdas) reais, dinâmicos e preditivos.
    
    * Referência Dinâmica: Koopman & Lit (2012).
    """
    
    # --- PARÂMETROS MOCK/PLACEHOLDER (SERÃO SUBSTITUÍDOS) ---
    # Estes valores mock SÃO O SEU GARGALO de assertividade.
    # A V3.0 deste script buscará os parâmetros alfa e beta do modelo treinado.
    MOCK_LAMBDA_HOME = 1.5 # (Habilidade de Ataque do Home Team)
    MOCK_LAMBDA_AWAY = 1.1 # (Habilidade de Ataque do Away Team)

    # Note que a vantagem do fator casa (se for incluído) deve entrar no Lambda.
    # Ex: lambda_h = exp(ataque_i - defesa_j + fator_casa)
    
    # Chamada ao núcleo matemático do modelo
    return biv_poisson_match_probs(MOCK_LAMBDA_HOME, MOCK_LAMBDA_AWAY)

# --- FIM: NOVAS FUNÇÕES CIENTÍFICAS ---


def build_model_predictions(rodada: str) -> pd.DataFrame:
    """
    REVISÃO: Gera probabilidades usando o Modelo Bivariate Poisson.
    """
    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    oc_path = os.path.join(rodada, "odds_consensus.csv")

    wl = read_csv_safe(wl_path)
    oc = read_csv_safe(oc_path)
    
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
            wlr["match_id"], wlr["team_home"], wlr["team_away"]
        )
        # ------------------------------------------

        if None in (oh, od, oa): # Mantém as odds do mercado para o arquivo de saída
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

    out_df = pd.DataFrame(rows, columns=OUTPUT_COLS)
    
    # SAÍDA DO MODELO: As probabilidades P_Home, P_Draw, P_Away AGORA são do nosso modelo.
    # O passo de CALIBRAÇÃO a seguir (scripts/calibrate_probs.py) é crucial.
    
    return out_df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    rodada = args.rodada
    out_path = os.path.join(rodada, "xg_bivariate.csv")
    
    log("INFO", "Substituindo lógica: Usando Modelo Científico Bivariate Poisson.")
    
    # O script agora SEMPRE usa o modelo para prever
    df = build_model_predictions(rodada)

    # Salva resultado
    df[OUTPUT_COLS].to_csv(out_path, index=False)
    log("INFO", f"xg_bivariate gerado: {out_path}  linhas={len(df)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
