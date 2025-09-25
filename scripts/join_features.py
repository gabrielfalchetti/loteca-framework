#!/usr/bin/env python3
import argparse, os, pandas as pd, numpy as np, yaml
from pathlib import Path
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import log_loss
import warnings

# (Opcional) Weights & Biases
USE_WANDB = True
try:
    import wandb
except Exception:
    USE_WANDB = False

def load_cfg():
    with open("config/config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def safe_merge(df_left, path_right, on_cols, how="left"):
    if not Path(path_right).exists():
        return df_left
    right = pd.read_csv(path_right)
    return df_left.merge(right, on=on_cols, how=how)

def build_joined(cfg, rodada):
    # Começa do matches
    base = pd.read_csv(cfg["paths"]["matches_csv"].replace("${rodada}", rodada))

    # Anexa standings (opcional se você quiser usar)
    # base = safe_merge(base, cfg["paths"]["standings_out"].replace("${rodada}", rodada), on_cols=["home","away"], how="left")

    # Anexa weather (opcional): agregue uma hora de referência (ex.: mais próxima do kickoff)
    weather_path = cfg["paths"]["weather_out"].replace("${rodada}", rodada)
    if Path(weather_path).exists():
        w = pd.read_csv(weather_path)
        # Exemplo simples: média por match_id
        agg = w.groupby("match_id").agg({
            "precipitation_probability":"mean",
            "wind_speed_10m":"mean",
            "precipitation":"mean",
            "temperature_2m":"mean"
        }).reset_index()
        agg.columns = ["match_id","weather_precipitation_probability","weather_wind_speed_10m","weather_precipitation","weather_temperature_2m"]
        base = base.merge(agg, on="match_id", how="left")

    # Anexa sinais de news (já está no matches.csv como news_home_hits/news_away_hits; se quiser cruzar, leia cfg["paths"]["news_out"])

    # Salva joined
    out_join = cfg["paths"]["joined_out"].replace("${rodada}", rodada)
    Path(out_join).parent.mkdir(parents=True, exist_ok=True)
    base.to_csv(out_join, index=False)
    return out_join

def prepare_features(df):
    # Features mínimas (coerentes com matches.csv de exemplo)
    feats = [
        "home_prob_market","draw_prob_market","away_prob_market",
        "home_form5","away_form5","home_rest_days","away_rest_days",
        "news_home_hits","news_away_hits",
        "weather_precipitation_probability","weather_wind_speed_10m"
    ]
    for c in feats:
        if c not in df.columns:
            df[c] = 0.0
    X = df[feats].astype(float).fillna(0.0).to_numpy()
    return X, feats

def main(rodada):
    cfg = load_cfg()
    joined_path = build_joined(cfg, rodada)
    df = pd.read_csv(joined_path)

    # Se tiver histórico (is_past=1), treinamos; senão usamos fallback: as probs de mercado já calibradas
    has_past = "is_past" in df.columns and df["is_past"].sum() > 0
    out = df[["match_id","home","away"]].copy()

    if has_past:
        train = df[df["is_past"]==1].copy()
        test  = df[df["is_past"]==0].copy()

        # Precisa da coluna 'result' no histórico (H/D/A). Se não tiver, cai no fallback abaixo.
        if "result" in train.columns and train["result"].notna().any():
            ymap = {"H":0,"D":1,"A":2}
            ytr = train["result"].map(ymap)
            Xtr,_ = prepare_features(train)
            Xte,_ = prepare_features(test)

            base = LogisticRegression(max_iter=2000, multi_class="multinomial")
            model = CalibratedClassifierCV(base, method="sigmoid", cv=5)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model.fit(Xtr, ytr)

            # métricas treino (se possível)
            try:
                p_tr = model.predict_proba(Xtr)
                ll = float(log_loss(ytr, p_tr))
            except Exception:
                ll = None

            p = model.predict_proba(Xte)
            out_te = test[["match_id","home","away"]].copy()
            out_te["p_home"] = p[:,0]; out_te["p_draw"] = p[:,1]; out_te["p_away"] = p[:,2]
            out = pd.concat([out, out_te], ignore_index=True).drop_duplicates("match_id", keep="last")

            # W&B
            if USE_WANDB and os.getenv("WANDB_API_KEY"):
                wandb.login(key=os.getenv("WANDB_API_KEY"))
                run = wandb.init(project="loteca-framework", config={"rodada": rodada})
                if ll is not None:
                    wandb.log({"logloss_train": ll})
        else:
            # Fallback sem 'result': usa probs do mercado
            out["p_home"] = df["home_prob_market"]
            out["p_draw"] = df["draw_prob_market"]
            out["p_away"] = df["away_prob_market"]
    else:
        # Sem histórico: usa probs do mercado
        out["p_home"] = df["home_prob_market"]
        out["p_draw"] = df["draw_prob_market"]
        out["p_away"] = df["away_prob_market"]

    # Context score (exemplo simples ponderado)
    out["context_score"] = (out["p_home"]*1.0 + out["p_draw"]*0.5 + out["p_away"]*0.0)

    report_path = cfg["paths"]["context_score_out"].replace("${rodada}", rodada)
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(report_path, index=False)
    print(f"[OK] context score → {report_path}")

    # W&B: salvar relatório
    if USE_WANDB and os.getenv("WANDB_API_KEY"):
        try:
            wandb.save(report_path)
            wandb.finish()
        except Exception:
            pass

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
