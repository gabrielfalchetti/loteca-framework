#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
postgame_update.py
Calibra probabilidades (home/draw/away) com base em resultados reais de uma rodada,
ajustando fatores k_home, k_draw, k_away para minimizar Brier score agregada.
Saída:
 - {OUT_DIR}/predictions_calibrated.csv
 - {OUT_DIR}/postgame_report.txt / .json
 - Atualiza data/model/model_params.yaml com os novos fatores
Uso:
  python scripts/postgame_update.py --out-dir data/out/<rodada ou jobid> --results data/in/results_XXXX.csv --debug
"""
import argparse, json, os, sys, math, datetime
import unicodedata
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Tuple
from scipy.optimize import minimize

try:
    import yaml
except Exception:
    print("[postgame] ERRO: PyYAML não encontrado. Adicione 'PyYAML' ao requirements.txt.", file=sys.stderr)
    sys.exit(2)

# ---------- util -----------
def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join([c for c in s if not unicodedata.combining(c)])
    s = s.lower().strip()
    s = " ".join(s.split())
    return s

def make_match_key(home: str, away: str) -> str:
    return f"{_norm(home)}__vs__{_norm(away)}"

def load_results(path: str, debug: bool=False) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"results csv não encontrado: {path}")
    df = pd.read_csv(path)
    # Aceita: (a) result em {'H','D','A'} OU (b) home_goals/away_goals
    cols = [c.lower() for c in df.columns]
    df.columns = cols
    required_any = (("result" in cols) or ("home_goals" in cols and "away_goals" in cols))
    if not required_any:
        raise ValueError("CSV de resultados deve ter 'result' (H/D/A) OU 'home_goals' e 'away_goals'.")
    if "match_key" not in cols:
        if not {"team_home","team_away"}.issubset(set(cols)):
            raise ValueError("Se não houver 'match_key', o arquivo de resultados precisa de 'team_home' e 'team_away'.")
        df["match_key"] = [make_match_key(h, a) for h, a in zip(df["team_home"], df["team_away"])]
    # Deriva 'result' se veio por gols
    if "result" not in cols:
        def _r(hg, ag):
            if pd.isna(hg) or pd.isna(ag): return None
            if hg > ag: return "H"
            if hg < ag: return "A"
            return "D"
        df["result"] = [ _r(hg, ag) for hg,ag in zip(df["home_goals"], df["away_goals"]) ]
    df = df.dropna(subset=["match_key","result"]).copy()
    df["result"] = df["result"].str.upper().str.strip()
    df = df[df["result"].isin(["H","D","A"])]
    if debug:
        print(f"[postgame][DEBUG] resultados lidos: {len(df)}")
    return df[["match_key","result"]].drop_duplicates()

def brier_score(p: np.ndarray, y: np.ndarray) -> float:
    # p: (n,3), y: (n,3) one-hot
    return float(np.mean(np.sum((p - y)**2, axis=1)))

def calibrate_scalars(probs: np.ndarray, y_onehot: np.ndarray, debug: bool=False) -> Tuple[float,float,float,float]:
    """
    Ajusta (kH,kD,kA) em [0.85,1.15] minimizando Brier. Retorna (kH,kD,kA,brier_new).
    """
    eps = 1e-12
    def project(p):
        p = np.clip(p, eps, 1.0)
        p = p / p.sum(axis=1, keepdims=True)
        return p

    def objective(k):
        kH,kD,kA = k
        scaled = probs * np.array([kH,kD,kA])[None,:]
        scaled = project(scaled)
        return brier_score(scaled, y_onehot)

    x0 = np.array([1.0,1.0,1.0])
    bounds = [(0.85,1.15),(0.85,1.15),(0.85,1.15)]
    res = minimize(objective, x0, method="L-BFGS-B", bounds=bounds)
    kH,kD,kA = res.x
    new_brier = objective(res.x)
    if debug:
        print(f"[postgame][DEBUG] k*: home={kH:.4f} draw={kD:.4f} away={kA:.4f} | brier={new_brier:.6f} (ok={res.success})")
    return float(kH),float(kD),float(kA),float(new_brier)

def load_model_params(path_yaml: str) -> dict:
    if os.path.isfile(path_yaml):
        with open(path_yaml,"r",encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    # default
    return {
        "calibration": {"k_home":1.0,"k_draw":1.0,"k_away":1.0,"updated_at":None,"note":"default"},
        "version": "4.3.RC1+"
    }

def save_model_params(path_yaml: str, data: dict):
    os.makedirs(os.path.dirname(path_yaml), exist_ok=True)
    with open(path_yaml,"w",encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, help="Diretório da rodada/job com predictions_market.csv")
    ap.add_argument("--results", required=True, help="CSV com resultados reais (ver template).")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.out_dir
    res_path = args.results
    debug = args.debug

    in_pred = os.path.join(out_dir, "predictions_market.csv")
    if not os.path.isfile(in_pred):
        print(f"[postgame] ERRO: arquivo não encontrado: {in_pred}", file=sys.stderr)
        sys.exit(2)
    pred = pd.read_csv(in_pred)
    cols = [c.lower() for c in pred.columns]
    pred.columns = cols
    need = {"match_key","prob_home","prob_draw","prob_away"}
    if not need.issubset(set(cols)):
        print(f"[postgame] ERRO: predictions_market.csv deve ter colunas: {need}", file=sys.stderr)
        sys.exit(2)

    results = load_results(res_path, debug=debug)
    # Join
    df = pred.merge(results, on="match_key", how="inner")
    if len(df) == 0:
        print("[postgame] ERRO: nenhum jogo com resultados para calibrar (join vazio).", file=sys.stderr)
        sys.exit(2)

    # Preparar matrizes
    P = df[["prob_home","prob_draw","prob_away"]].to_numpy(dtype=float)
    y = df["result"].map({"H":0,"D":1,"A":2}).to_numpy()
    Y = np.eye(3)[y]  # one-hot

    # Brier antes
    brier_old = brier_score(P, Y)

    # Calibração
    kH,kD,kA,brier_new = calibrate_scalars(P, Y, debug=debug)

    # Gerar predictions_calibrated.csv
    scale = np.array([kH,kD,kA])[None,:]
    P2 = P * scale
    P2 = P2 / P2.sum(axis=1, keepdims=True)
    out_pred = df.copy()
    out_pred["prob_home_cal"] = P2[:,0]
    out_pred["prob_draw_cal"] = P2[:,1]
    out_pred["prob_away_cal"] = P2[:,2]
    out_csv = os.path.join(out_dir, "predictions_calibrated.csv")
    out_pred.to_csv(out_csv, index=False, encoding="utf-8")
    if debug:
        print(f"[postgame] OK -> {out_csv} ({len(out_pred)} linhas)")

    # Atualizar model_params.yaml
    model_yaml = "data/model/model_params.yaml"
    mp = load_model_params(model_yaml)
    mp["calibration"] = {
        "k_home": round(kH,6),
        "k_draw": round(kD,6),
        "k_away": round(kA,6),
        "updated_at": datetime.datetime.utcnow().isoformat()+"Z",
        "note": "auto-calibrated from last results (postgame_update.py)"
    }
    save_model_params(model_yaml, mp)

    # Relatórios
    delta = brier_old - brier_new
    rep_txt = os.path.join(out_dir, "postgame_report.txt")
    rep_json = os.path.join(out_dir, "postgame_report.json")
    with open(rep_txt,"w",encoding="utf-8") as f:
        f.write(f"[postgame] jogos usados: {len(df)}\n")
        f.write(f"[postgame] brier_before: {brier_old:.6f}\n")
        f.write(f"[postgame] brier_after : {brier_new:.6f}\n")
        f.write(f"[postgame] improvement : {delta:.6f}\n")
        f.write(f"[postgame] k_home={kH:.6f} k_draw={kD:.6f} k_away={kA:.6f}\n")
        f.write(f"[postgame] params_yaml: {model_yaml}\n")
    with open(rep_json,"w",encoding="utf-8") as f:
        json.dump({
            "games_used": int(len(df)),
            "brier_before": brier_old,
            "brier_after": brier_new,
            "improvement": delta,
            "k_home": kH, "k_draw": kD, "k_away": kA,
            "model_params_yaml": model_yaml
        }, f, ensure_ascii=False, indent=2)

    print(f"[postgame] OK -> {rep_txt}")
    print(f"[postgame] OK -> {rep_json}")
    print(f"[postgame] Calibração conservadora aplicada. Nada no pipeline de coleta foi alterado.")

if __name__ == "__main__":
    main()