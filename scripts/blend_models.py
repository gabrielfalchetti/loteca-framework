#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
blend_models.py
---------------
Faz o blend (ensemble) das fontes de probabilidade disponíveis e gera:
  data/out/<RODADA>/predictions_blend.csv

Entradas POTENCIAIS (usa as que existirem):
- data/out/<RODADA>/predictions_market.csv          -> colunas: match_id, prob_home, prob_draw, prob_away
- data/out/<RODADA>/odds_consensus.csv              -> colunas: match_id,team_home,team_away,odds_home,odds_draw,odds_away,source
- data/out/<RODADA>/calibrated_probs.csv            -> colunas: match_id, prob_home, prob_draw, prob_away
- data/out/<RODADA>/features_univariado.csv         -> colunas: match_id, prob_home, prob_draw, prob_away (ou similares)
- data/out/<RODADA>/features_bivariado.csv          -> colunas: match_id, prob_home, prob_draw, prob_away (ou similares)

Saída (SEMPRE MESMO ESQUEMA):
- data/out/<RODADA>/predictions_blend.csv
  colunas: match_id, team_home, team_away, p_home, p_draw, p_away, used_sources, weights

Uso:
  python scripts/blend_models.py --rodada <ID_ou_PATH> [--debug]
"""

import os
import re
import argparse
import numpy as np
import pandas as pd

# ===================== CLI =====================
def parse_args():
    p = argparse.ArgumentParser(description="Blend/ensemble de modelos de probabilidade")
    p.add_argument("--rodada", required=True, help="ID da rodada (ex: 1829...) OU caminho data/out/<ID>")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()

# ===================== Utils ====================
def _is_id_like(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{6,}", str(s)))

def _out_dir(rodada: str) -> str:
    return rodada if rodada.startswith("data/") else os.path.join("data", "out", str(rodada))

def _log(debug, *msg):
    if debug:
        print("[blend]", *msg)

def _safe_read_csv(path, debug=False):
    if not os.path.exists(path):
        _log(debug, f"arquivo ausente: {path}")
        return None
    try:
        df = pd.read_csv(path)
        if df is None or df.empty:
            _log(debug, f"arquivo vazio: {path}")
            return None
        return df
    except Exception as e:
        _log(debug, f"falha lendo {path}: {e}")
        return None

def _coerce_probs(df, cols, rename_to=None):
    """
    Garante que as colunas sejam float e 0<=p<=1. Se rename_to for dado, renomeia.
    """
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            return None
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # descarta linhas inválidas
    df = df.dropna(subset=cols)
    # recorta
    for c in cols:
        df[c] = df[c].clip(0, 1)
    if rename_to and len(rename_to) == len(cols):
        df = df.rename(columns=dict(zip(cols, rename_to)))
    return df

def _normalize_row(p_home, p_draw, p_away):
    arr = np.array([p_home, p_draw, p_away], dtype=float)
    if not np.isfinite(arr).all() or (arr <= 0).all():
        # fallback seguro: empate mínimo
        return np.array([1/3, 1/3, 1/3], dtype=float)
    s = arr.sum()
    if s <= 0:
        return np.array([1/3, 1/3, 1/3], dtype=float)
    return arr / s

def _implied_from_odds(df_odds):
    """
    Converte odds decimais em probabilidades implícitas normalizadas (com overround).
    Espera colunas: match_id, odds_home, odds_draw, odds_away.
    """
    probs = []
    for _, r in df_odds.iterrows():
        oh, od, oa = float(r["odds_home"]), float(r["odds_draw"]), float(r["odds_away"])
        if oh <= 1 or od <= 1 or oa <= 1:
            # odds inválidas -> pula
            probs.append([np.nan, np.nan, np.nan])
            continue
        imp = np.array([1.0/oh, 1.0/od, 1.0/oa], dtype=float)
        s = imp.sum()
        if s <= 0:
            probs.append([np.nan, np.nan, np.nan])
        else:
            # normaliza tirando overround
            probs.append(list(imp / s))
    x = np.array(probs, dtype=float)
    out = df_odds[["match_id"]].copy()
    out["prob_home"] = x[:,0]
    out["prob_draw"] = x[:,1]
    out["prob_away"] = x[:,2]
    return out.dropna(subset=["prob_home","prob_draw","prob_away"])

def _std_match_table(out_dir, debug=False):
    """
    Tenta montar tabela base com match_id, team_home, team_away a partir de:
      - odds_consensus.csv (preferencial)
      - predictions_market.csv (se tiver team_home, team_away)
      - por fim, se só houver match_id, tenta separar "home__away"
    """
    # 1) odds_consensus.csv
    path_cons = os.path.join(out_dir, "odds_consensus.csv")
    dfc = _safe_read_csv(path_cons, debug)
    if dfc is not None:
        cols_ok = {"match_id","team_home","team_away"}
        if cols_ok.issubset(set(map(str, dfc.columns))):
            base = dfc[["match_id","team_home","team_away"]].dropna().drop_duplicates()
            if not base.empty:
                _log(debug, "base de nomes vinda de odds_consensus.csv")
                return base

    # 2) predictions_market.csv
    path_pm = os.path.join(out_dir, "predictions_market.csv")
    dfm = _safe_read_csv(path_pm, debug)
    if dfm is not None:
        cols_a = {"match_id","team_home","team_away"}
        if cols_a.issubset(set(map(str, dfm.columns))):
            base = dfm[["match_id","team_home","team_away"]].dropna().drop_duplicates()
            if not base.empty:
                _log(debug, "base de nomes vinda de predictions_market.csv")
                return base

    # 3) fallback: tentar deduzir de match_id (home__away)
    # procurar em qualquer arquivo disponível uma lista de match_id
    candidates = []
    for fname in ["predictions_market.csv","calibrated_probs.csv",
                  "features_univariado.csv","features_bivariado.csv","odds_consensus.csv"]:
        p = os.path.join(out_dir, fname)
        df = _safe_read_csv(p, debug)
        if df is not None and "match_id" in df.columns:
            candidates.append(df["match_id"].dropna().astype(str))
    if candidates:
        ids = pd.concat(candidates, ignore_index=True).drop_duplicates()
        base = pd.DataFrame({"match_id": ids})
        sp = base["match_id"].str.split("__", n=1, expand=True)
        if sp.shape[1] == 2:
            base["team_home"] = sp[0].str.strip()
            base["team_away"] = sp[1].str.strip()
            _log(debug, "base de nomes deduzida de match_id")
            return base.dropna()

    # nada
    return pd.DataFrame(columns=["match_id","team_home","team_away"])

def _read_probs(out_dir, debug=False):
    """
    Lê as diversas fontes e retorna dicionário:
      {
        "market": df (match_id, prob_home, prob_draw, prob_away),
        "calibrated": df (...),
        "univariado": df (...),
        "bivariado": df (...),
      }
    'market' pode vir de predictions_market.csv ou de odds_consensus.csv->implied
    """
    res = {}

    # 1) market (preferir predictions_market.csv; se não tiver, derivar de odds_consensus.csv)
    p_market = os.path.join(out_dir, "predictions_market.csv")
    dfm = _safe_read_csv(p_market, debug)
    if dfm is not None:
        # mapear possíveis nomes de prob col
        cols = set(map(str.lower, dfm.columns))
        # tentativas de nomes
        cand_home = [c for c in dfm.columns if c.lower() in ["prob_home","p_home","home_prob","prob_h"]]
        cand_draw = [c for c in dfm.columns if c.lower() in ["prob_draw","p_draw","draw_prob","prob_d","prob_x"]]
        cand_away = [c for c in dfm.columns if c.lower() in ["prob_away","p_away","away_prob","prob_a"]]
        if cand_home and cand_draw and cand_away and "match_id" in dfm.columns:
            dm = dfm[["match_id", cand_home[0], cand_draw[0], cand_away[0]]].copy()
            dm = _coerce_probs(dm, [cand_home[0], cand_draw[0], cand_away[0]],
                               rename_to=["prob_home","prob_draw","prob_away"])
            if dm is not None and not dm.empty:
                res["market"] = dm
    if "market" not in res:
        # tentar odds_consensus -> implied
        p_cons = os.path.join(out_dir, "odds_consensus.csv")
        dfo = _safe_read_csv(p_cons, debug)
        if dfo is not None:
            need = {"match_id","odds_home","odds_draw","odds_away"}
            if need.issubset(set(dfo.columns)):
                dm = dfo[list(need)].dropna()
                dm = _implied_from_odds(dm)
                if dm is not None and not dm.empty:
                    res["market"] = dm
                    _log(debug, "market derivado de odds_consensus (implied)")

    # 2) calibrated
    p_cal = os.path.join(out_dir, "calibrated_probs.csv")
    dfc = _safe_read_csv(p_cal, debug)
    if dfc is not None and "match_id" in dfc.columns:
        cand_home = [c for c in dfc.columns if c.lower() in ["prob_home","p_home","home_prob","prob_h"]]
        cand_draw = [c for c in dfc.columns if c.lower() in ["prob_draw","p_draw","draw_prob","prob_d","prob_x"]]
        cand_away = [c for c in dfc.columns if c.lower() in ["prob_away","p_away","away_prob","prob_a"]]
        if cand_home and cand_draw and cand_away:
            dc = dfc[["match_id", cand_home[0], cand_draw[0], cand_away[0]]].copy()
            dc = _coerce_probs(dc, [cand_home[0], cand_draw[0], cand_away[0]],
                               rename_to=["prob_home","prob_draw","prob_away"])
            if dc is not None and not dc.empty:
                res["calibrated"] = dc

    # 3) univariado
    p_uni = os.path.join(out_dir, "features_univariado.csv")
    dfu = _safe_read_csv(p_uni, debug)
    if dfu is not None and "match_id" in dfu.columns:
        cand_home = [c for c in dfu.columns if c.lower() in ["prob_home","p_home","home_prob","prob_h"]]
        cand_draw = [c for c in dfu.columns if c.lower() in ["prob_draw","p_draw","draw_prob","prob_d","prob_x"]]
        cand_away = [c for c in dfu.columns if c.lower() in ["prob_away","p_away","away_prob","prob_a"]]
        if cand_home and cand_draw and cand_away:
            du = dfu[["match_id", cand_home[0], cand_draw[0], cand_away[0]]].copy()
            du = _coerce_probs(du, [cand_home[0], cand_draw[0], cand_away[0]],
                               rename_to=["prob_home","prob_draw","prob_away"])
            if du is not None and not du.empty:
                res["univariado"] = du

    # 4) bivariado
    p_bi = os.path.join(out_dir, "features_bivariado.csv")
    dfb = _safe_read_csv(p_bi, debug)
    if dfb is not None and "match_id" in dfb.columns:
        cand_home = [c for c in dfb.columns if c.lower() in ["prob_home","p_home","home_prob","prob_h"]]
        cand_draw = [c for c in dfb.columns if c.lower() in ["prob_draw","p_draw","draw_prob","prob_d","prob_x"]]
        cand_away = [c for c in dfb.columns if c.lower() in ["prob_away","p_away","away_prob","prob_a"]]
        if cand_home and cand_draw and cand_away:
            db = dfb[["match_id", cand_home[0], cand_draw[0], cand_away[0]]].copy()
            db = _coerce_probs(db, [cand_home[0], cand_draw[0], cand_away[0]],
                               rename_to=["prob_home","prob_draw","prob_away"])
            if db is not None and not db.empty:
                res["bivariado"] = db

    return res

# ===================== Blend ====================
def _blend(out_dir, debug=False):
    # pesos default (podem ser ajustados empiricamente ou por W&B)
    weights = {
        "market": 0.35,
        "calibrated": 0.35,
        "univariado": 0.15,
        "bivariado": 0.15,
    }

    # base de nomes
    base = _std_match_table(out_dir, debug=debug)
    # fontes de prob
    sources = _read_probs(out_dir, debug=debug)

    if not sources:
        raise SystemExit("[blend] ERRO: nenhuma fonte de probabilidade disponível.")

    # juntar tudo por match_id
    # começamos com base (pode estar vazia) só para levar team_home/team_away
    df = base.copy()

    # se base vazia, ao menos reunir todos match_ids presentes
    if df.empty:
        all_ids = []
        for k, d in sources.items():
            all_ids.append(d["match_id"])
        ids = pd.concat(all_ids, ignore_index=True).drop_duplicates()
        df = pd.DataFrame({"match_id": ids})
        # tentar extrair nomes de match_id
        sp = df["match_id"].astype(str).str.split("__", n=1, expand=True)
        if sp.shape[1] == 2:
            df["team_home"] = sp[0].str.strip()
            df["team_away"] = sp[1].str.strip()

    # anexar cada fonte com sufixos
    for name, d in sources.items():
        d = d.copy()
        d.columns = ["match_id", f"{name}_home", f"{name}_draw", f"{name}_away"]
        df = df.merge(d, on="match_id", how="left")

    # blend linha a linha
    rows = []
    for _, r in df.iterrows():
        used = []
        wsum = 0.0
        agg = np.array([0.0, 0.0, 0.0], dtype=float)
        for name in ["market","calibrated","univariado","bivariado"]:
            ph = r.get(f"{name}_home", np.nan)
            pdw = r.get(f"{name}_draw", np.nan)
            pa = r.get(f"{name}_away", np.nan)
            if np.isfinite(ph) and np.isfinite(pdw) and np.isfinite(pa):
                p = _normalize_row(ph, pdw, pa)
                w = weights[name]
                agg += w * p
                wsum += w
                used.append(name)

        if wsum <= 0.0:
            # nenhuma fonte neste jogo -> tentar odds_consensus como fallback local
            # (já deveria ter sido coberto em "market"; se ainda assim falhar, usar 1/3)
            p_final = np.array([1/3, 1/3, 1/3], dtype=float)
            used_str = "fallback_uniform"
            wdesc = "n/a"
        else:
            p_final = agg / wsum
            p_final = _normalize_row(*p_final)
            used_str = "+".join(used)
            # registrar pesos efetivos usados
            wdesc = ",".join([f"{u}:{weights[u]}" for u in used])

        rows.append({
            "match_id": r["match_id"],
            "team_home": r.get("team_home", ""),
            "team_away": r.get("team_away", ""),
            "p_home": float(p_final[0]),
            "p_draw": float(p_final[1]),
            "p_away": float(p_final[2]),
            "used_sources": used_str,
            "weights": wdesc
        })

    out = pd.DataFrame(rows)
    # sanidade: normalizar novamente quaisquer linhas degeneradas
    out[["p_home","p_draw","p_away"]] = out[["p_home","p_draw","p_away"]].apply(
        lambda col: pd.to_numeric(col, errors="coerce")
    )
    def _normrow(row):
        v = _normalize_row(row["p_home"], row["p_draw"], row["p_away"])
        return pd.Series({"p_home": v[0], "p_draw": v[1], "p_away": v[2]})
    out[["p_home","p_draw","p_away"]] = out.apply(_normrow, axis=1)

    # ordena por nome de casa/fora se disponível, senão por match_id
    if "team_home" in out.columns and "team_away" in out.columns:
        out = out.sort_values(by=["team_home","team_away","match_id"]).reset_index(drop=True)
    else:
        out = out.sort_values(by=["match_id"]).reset_index(drop=True)

    return out

# ===================== Main =====================
def main():
    args = parse_args()
    out_dir = _out_dir(args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    _log(args.debug, "rodada:", out_dir)

    blended = _blend(out_dir, debug=args.debug)
    out_path = os.path.join(out_dir, "predictions_blend.csv")
    blended.to_csv(out_path, index=False)
    print(f"[blend] OK -> {out_path}")
    if args.debug:
        print(blended.head(20).to_string(index=False))

if __name__ == "__main__":
    main()