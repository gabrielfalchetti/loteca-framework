#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
blend_models.py (STRICT + robustez do --use-context)

Gera:
  - <OUT_DIR>/predictions_blend.csv (sempre)
  - <OUT_DIR>/predictions_final.csv (se --use-context habilitado e contexto disponível)

Regras:
- Requer pelo menos um dos seguintes com p_*: calibrated_probs.csv ou predictions_market.csv.
  (Se ambos existirem, aplica blend com pesos informados.)
- Se --use-context for "true"/"1"/"yes" → requer context_features.csv com 'context_score'.
- Se qualquer insumo requerido faltar → falha (exit 24).
"""

import os
import sys
import argparse
import pandas as pd


def die(msg: str, code: int = 24):
    print(f"##[error]{msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def parse_bool(s: str) -> bool:
    if s is True:
        return True
    s = str(s).strip().lower()
    return s in ("1","true","yes","y","on")


def read_csv_ok(path: str, must_have_cols=None) -> pd.DataFrame:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if must_have_cols and not all(c in df.columns for c in must_have_cols):
        return pd.DataFrame()
    return df


def ensure_match_id(df: pd.DataFrame) -> pd.DataFrame:
    if "match_id" not in df.columns:
        if "team_home" in df.columns and "team_away" in df.columns:
            df["match_id"] = df["team_home"].astype(str) + "__" + df["team_away"].astype(str)
        elif "home" in df.columns and "away" in df.columns:
            df["match_id"] = df["home"].astype(str) + "__" + df["away"].astype(str)
        elif "match_key" in df.columns:
            df["match_id"] = df["match_key"].astype(str)
        else:
            die("Não foi possível derivar 'match_id' em alguma base.")
    df["match_id"] = df["match_id"].astype(str)
    return df


def std_teams(df: pd.DataFrame) -> pd.DataFrame:
    if "team_home" not in df.columns and "home" in df.columns:
        df = df.rename(columns={"home":"team_home"})
    if "team_away" not in df.columns and "away" in df.columns:
        df = df.rename(columns={"away":"team_away"})
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--w_calib", type=float, default=0.65)
    ap.add_argument("--w_market", type=float, default=0.35)
    # Aceita tanto "--use-context" (sem valor) quanto "--use-context true/false"
    ap.add_argument("--use-context", nargs="?", const="true", default="false")
    ap.add_argument("--context-strength", type=float, default=0.15)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    use_context = parse_bool(args.use_context)

    # Carrega bases
    calib = read_csv_ok(os.path.join(out_dir, "calibrated_probs.csv"),
                        must_have_cols=["match_id","calib_home","calib_draw","calib_away"])
    market = read_csv_ok(os.path.join(out_dir, "predictions_market.csv"),
                         must_have_cols=["match_id","team_home","team_away","p_home","p_draw","p_away"])

    if calib.empty and market.empty:
        die("Nem calibrated_probs.csv nem predictions_market.csv disponíveis com probabilidades.")

    # Normaliza chaves
    if not calib.empty:
        calib = ensure_match_id(calib)
    if not market.empty:
        market = ensure_match_id(std_teams(market))

    # Base de nomes:
    base = market if not market.empty else calib.copy()
    if "team_home" not in base.columns or "team_away" not in base.columns:
        # tenta derivar de match_id
        if "team_home" not in base.columns or "team_away" not in base.columns:
            base[["team_home","team_away"]] = base["match_id"].str.split("__", n=1, expand=True)

    # Blend
    w_calib = float(args.w_calib)
    w_market = float(args.w_market)
    w_sum = w_calib + w_market
    if w_sum <= 0:
        die("Soma de pesos inválida (w_calib + w_market <= 0).")
    w_calib /= w_sum
    w_market /= w_sum

    df = base.copy()[["match_id","team_home","team_away"]]
    if not market.empty:
        df = df.merge(market[["match_id","p_home","p_draw","p_away"]], on="match_id", how="left", suffixes=("","_m"))
    if not calib.empty:
        df = df.merge(calib[["match_id","calib_home","calib_draw","calib_away"]], on="match_id", how="left")

    # se só existe uma fonte, usa-a; senão blend
    def blend(row):
        mh, md, ma = row.get("p_home"), row.get("p_draw"), row.get("p_away")
        ch, cd, ca = row.get("calib_home"), row.get("calib_draw"), row.get("calib_away")
        if pd.notna(ch) and pd.notna(mh):
            return w_market*mh + w_calib*ch, w_market*md + w_calib*cd, w_market*ma + w_calib*ca, "market+calib"
        if pd.notna(mh):
            return mh, md, ma, "market"
        if pd.notna(ch):
            return ch, cd, ca, "calib"
        return float("nan"), float("nan"), float("nan"), "none"

    out_rows = []
    for _, r in df.iterrows():
        ph, pd_, pa, src = blend(r)
        if not (pd.notna(ph) and pd.notna(pd_) and pd.notna(pa)):
            die(f"Probabilidades ausentes para {r.get('match_id')} (fonte='{src}').")
        out_rows.append({
            "match_id": r["match_id"],
            "team_home": r["team_home"],
            "team_away": r["team_away"],
            "p_home": ph, "p_draw": pd_, "p_away": pa,
            "used_sources": src,
            "weights": f"market:{w_market:.2f},calib:{w_calib:.2f}"
        })
    blend_df = pd.DataFrame(out_rows)
    blend_path = os.path.join(out_dir, "predictions_blend.csv")
    blend_df.to_csv(blend_path, index=False)

    if args.debug:
        print(f"[blend] rodada: {out_dir}")
        print(f"[blend] OK -> {blend_path}")

    # Contexto opcional (estrito se habilitado)
    if use_context:
        ctx = read_csv_ok(os.path.join(out_dir, "context_features.csv"))
        if ctx.empty or "context_score" not in ctx.columns:
            die("Contexto habilitado mas context_features.csv está ausente/incompleto (sem 'context_score').")
        ctx = ensure_match_id(ctx)[["match_id","context_score"]]
        final = blend_df.merge(ctx, on="match_id", how="inner")
        if final.empty or final["context_score"].isna().any():
            die("Falha no merge do contexto: linhas ausentes ou context_score NaN.")
        # ajuste simples: suaviza probs em direção ao favorito quando context_score é alto
        alpha = float(args.context_strength)
        def adj(row):
            ph, pd_, pa = row["p_home"], row["p_draw"], row["p_away"]
            fav = max(("HOME",ph), ("DRAW",pd_), ("AWAY",pa), key=lambda x: x[1])[0]
            ch, cd, ca = ph, pd_, pa
            if fav == "HOME":
                ch = ph + alpha*row["context_score"]
                cd = pd_
                ca = pa - alpha*row["context_score"]/2
            elif fav == "AWAY":
                ca = pa + alpha*row["context_score"]
                cd = pd_
                ch = ph - alpha*row["context_score"]/2
            else:
                cd = pd_ + alpha*row["context_score"]/2
                ch = ph - alpha*row["context_score"]/4
                ca = pa - alpha*row["context_score"]/4
            # re-normaliza
            s = ch + cd + ca
            return max(ch/s,0), max(cd/s,0), max(ca/s,0)

        final[["p_home","p_draw","p_away"]] = final.apply(
            lambda r: pd.Series(adj(r)),
            axis=1
        )
        final_path = os.path.join(out_dir, "predictions_final.csv")
        final.to_csv(final_path, index=False)
        if args.debug:
            print(f"[blend] OK -> {final_path}")
    else:
        # não gera predictions_final.csv
        pass


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        die(f"Erro inesperado: {e}")