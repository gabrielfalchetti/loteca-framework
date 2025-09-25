#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
join_features.py — versão simples e didática (cola-e-usa)

O que este script faz:
1) Lê a configuração em config/config.yaml (caminhos e opções).
2) Carrega a tabela de jogos (matches.csv) da rodada.
3) (Opcional) Agrega clima (weather) por partida, se existir.
4) Monta um dataset "joined" mínimo e salva em data/processed/joined_<rodada>.csv.
5) Se houver histórico com rótulo (result) e is_past=1, treina
   uma Regressão Logística com calibração (Platt "sigmoid").
   Caso contrário, usa as probabilidades de mercado do CSV.
6) Gera o relatório final com p_home, p_draw, p_away e context_score
   em reports/context_scores_<rodada>.csv.
7) Integra com Weights & Biases (W&B) de forma opcional/segura:
   - Só ativa se a lib 'wandb' estiver instalada E se WANDB_API_KEY existir no ambiente.
"""

import argparse
import os
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss

# ---- W&B: liga/desliga automático e seguro ----
USE_WANDB = True
try:
    import wandb  # type: ignore
except Exception:
    USE_WANDB = False
# ------------------------------------------------


# -------------------- utilidades --------------------
def load_cfg():
    """Carrega config/config.yaml como dicionário."""
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def safe_read_csv(path: str) -> pd.DataFrame:
    """Lê CSV se existir; senão retorna DataFrame vazio."""
    p = Path(path)
    if p.exists():
        return pd.read_csv(p)
    return pd.DataFrame()


def ensure_cols(df: pd.DataFrame, cols, fill=0.0):
    """Garante que as colunas existam; se não houver, cria com fill."""
    for c in cols:
        if c not in df.columns:
            df[c] = fill
    return df


# --------------- construção do "joined" ---------------
def build_joined(cfg: dict, rodada: str) -> str:
    """
    Monta um joined mínimo a partir do matches + (opcional) weather agregado por partida.
    Salva e retorna o caminho do joined.
    """
    matches_path = cfg["paths"]["matches_csv"].replace("${rodada}", rodada)
    dfm = safe_read_csv(matches_path)
    if dfm.empty:
        raise SystemExit(f"[ERRO] Arquivo de partidas não encontrado ou vazio: {matches_path}")

    # WEATHER (opcional): agrega média por match_id
    weather_path = cfg["paths"]["weather_out"].replace("${rodada}", rodada)
    w = safe_read_csv(weather_path)
    if not w.empty:
        # tenta agregar colunas comuns; cria nomes padronizados
        agg = w.groupby("match_id").agg({
            "precipitation_probability": "mean",
            "wind_speed_10m": "mean",
            "precipitation": "mean",
            "temperature_2m": "mean",
        }).reset_index()
        agg.columns = [
            "match_id",
            "weather_precipitation_probability",
            "weather_wind_speed_10m",
            "weather_precipitation",
            "weather_temperature_2m",
        ]
        dfm = dfm.merge(agg, on="match_id", how="left")

    # salva joined mínimo
    joined_out = cfg["paths"]["joined_out"].replace("${rodada}", rodada)
    Path(joined_out).parent.mkdir(parents=True, exist_ok=True)
    dfm.to_csv(joined_out, index=False)
    return joined_out


# --------------------- features ----------------------
FEATURES_MIN = [
    # probabilidades de mercado (já devigadas, se possível)
    "home_prob_market",
    "draw_prob_market",
    "away_prob_market",
    # forma/descanso/notícias — colunas que sugerimos no CSV de matches
    "home_form5",
    "away_form5",
    "home_rest_days",
    "away_rest_days",
    "news_home_hits",
    "news_away_hits",
    # clima agregado
    "weather_precipitation_probability",
    "weather_wind_speed_10m",
]


def prepare_X(df: pd.DataFrame, feat_list=FEATURES_MIN) -> np.ndarray:
    """Prepara matriz X com as FEATURES_MIN, preenchendo ausentes com 0."""
    ensure_cols(df, feat_list, fill=0.0)
    return df[feat_list].astype(float).fillna(0.0).to_numpy()


# -------------------- pipeline main -------------------
def main(rodada: str):
    cfg = load_cfg()

    # --- iniciar W&B (se a chave existir no ambiente) ---
    if USE_WANDB and os.getenv("WANDB_API_KEY"):
        wandb.login(key=os.getenv("WANDB_API_KEY"))
        run = wandb.init(project="loteca-framework", config={"rodada": rodada})
        # Log de “batimento” pra você ver algo no painel
        wandb.log({"wandb_ping": 1})
    else:
        run = None
    # -----------------------------------------------------

    # 1) montar o "joined"
    joined_path = build_joined(cfg, rodada)
    df = pd.read_csv(joined_path)

    # 2) separar jogos passados e futuros (se houver a coluna is_past)
    has_past_flag = "is_past" in df.columns
    df_future = df.copy()
    df_past = pd.DataFrame()
    if has_past_flag:
        df_past = df[df["is_past"] == 1].copy()
        df_future = df[df["is_past"] == 0].copy()

    # 3) checar se temos rótulos de resultado no passado (H/D/A)
    has_labels = not df_past.empty and ("result" in df_past.columns) and df_past["result"].notna().any()

    out = df[["match_id", "home", "away"]].copy()

    if has_labels:
        # 3a) Treinar modelo com calibração (multiclasse H/D/A)
        ymap = {"H": 0, "D": 1, "A": 2}
        ytr = df_past["result"].map(ymap).to_numpy()

        Xtr = prepare_X(df_past)
        Xte = prepare_X(df_future) if not df_future.empty else np.zeros((0, len(FEATURES_MIN)))

        base = LogisticRegression(max_iter=2000, multi_class="multinomial")
        model = CalibratedClassifierCV(base, method="sigmoid", cv=5)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(Xtr, ytr)

        # métrica simples no treino (logloss)
        try:
            p_tr = model.predict_proba(Xtr)
            ll = float(log_loss(ytr, p_tr))
            if USE_WANDB and run is not None:
                wandb.log({"logloss_train": ll})
        except Exception:
            pass

        # previsões para jogos futuros (is_past=0)
        if not df_future.empty:
            p = model.predict_proba(Xte)
            future_out = df_future[["match_id", "home", "away"]].copy()
            future_out["p_home"] = p[:, 0]
            future_out["p_draw"] = p[:, 1]
            future_out["p_away"] = p[:, 2]
            out = pd.concat([out, future_out], ignore_index=True).drop_duplicates("match_id", keep="last")
        else:
            # caso extremo: só passado; ainda assim manter as colunas
            out["p_home"] = np.nan
            out["p_draw"] = np.nan
            out["p_away"] = np.nan

    else:
        # 3b) Sem histórico rotulado: usar probabilidades de mercado como fallback
        out["p_home"] = df.get("home_prob_market", pd.Series([np.nan] * len(df)))
        out["p_draw"] = df.get("draw_prob_market", pd.Series([np.nan] * len(df)))
        out["p_away"] = df.get("away_prob_market", pd.Series([np.nan] * len(df)))

    # 4) context score simples (p_home*1 + p_draw*0.5 + p_away*0)
    #    ajuste os pesos se quiser outra “função valor”
    out["context_score"] = out["p_home"].astype(float) * 1.0 + out["p_draw"].astype(float) * 0.5

    # 5) salvar relatório final
    report_path = cfg["paths"]["context_score_out"].replace("${rodada}", rodada)
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(report_path, index=False)
    print(f"[OK] context score → {report_path}")

    # 6) subir o arquivo final para a run do W&B (aparece em Files)
    if USE_WANDB and run is not None:
        try:
            wandb.save(report_path)
        except Exception:
            pass
        try:
            wandb.finish()
        except Exception:
            pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-09-20_21")
    args = ap.parse_args()
    main(args.rodada)
