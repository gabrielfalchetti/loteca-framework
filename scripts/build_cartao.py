#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_cartao.py
Gera o cartão da Loteca APENAS no final do pipeline, usando a melhor fonte disponível:

Prioridade de entrada (todas em ${OUT_DIR}):
  1) kelly_stakes.csv
  2) predictions_blend.csv
  3) calibrated_probs.csv
  4) predictions_market.csv
  5) odds_consensus.csv  (converte odds -> probs)

A ordem dos jogos vem de data/in/matches_source.csv (match_id,home,away,source[,...]).
Saídas:
  - ${OUT_DIR}/loteca_cartao.txt  (legível)
  - ${OUT_DIR}/loteca_cartao.csv  (estruturado)

Fail-fast:
  - Se faltar OUT_DIR ou matches_source.csv -> exit 26
  - Com --strict: se algum jogo não tiver pick -> exit 26
"""

import argparse
import os
import sys
import csv
from typing import Dict, List, Optional, Tuple

import pandas as pd

REQ_INPUT = "data/in/matches_source.csv"

CANDIDATE_FILES = [
    "kelly_stakes.csv",
    "predictions_blend.csv",
    "calibrated_probs.csv",
    "predictions_market.csv",
    "odds_consensus.csv",
]

# possíveis aliases de colunas
HOME_ALIASES = ["team_home","home","home_name","mandante","home_team"]
AWAY_ALIASES = ["team_away","away","away_name","visitante","away_team"]
MID_ALIASES  = ["match_id","id","jogo_id","partida_id"]

PH_ALIASES   = ["prob_home","p_home","ph","prob_h","prob1","prob_mandante"]
PD_ALIASES   = ["prob_draw","p_draw","pd","prob_empate","probx","px"]
PA_ALIASES   = ["prob_away","p_away","pa","prob_a","prob2","prob_visitante"]

OH_ALIASES   = ["odds_home","o_home","odd_home","cotacao_home","odds1","odds_mandante"]
OD_ALIASES   = ["odds_draw","o_draw","odd_draw","cotacao_draw","oddsx","odds_empate"]
OA_ALIASES   = ["odds_away","o_away","odd_away","cotacao_away","odds2","odds_visitante"]

PICK_ALIASES = ["pick","bet","selection","aposta","sinal","resultado","side"]

STAKE_ALIASES = ["stake","valor","unidades","unidade_kelly","stake_units"]
CONF_ALIASES  = ["confidence","conf","score","edge","kelly","advantage"]

def _first(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None

def _load_matches() -> pd.DataFrame:
    if not os.path.isfile(REQ_INPUT):
        print(f"##[error]Entrada {REQ_INPUT} não encontrada.", file=sys.stderr)
        sys.exit(26)
    df = pd.read_csv(REQ_INPUT)
    df.columns = [c.strip().lower() for c in df.columns]
    need = ["match_id","home","away","source"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        print(f"##[error]{REQ_INPUT} sem colunas obrigatórias: {miss}", file=sys.stderr)
        sys.exit(26)
    return df

def _try_read(out_dir: str) -> Tuple[str, pd.DataFrame]:
    for name in CANDIDATE_FILES:
        path = os.path.join(out_dir, name)
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            try:
                df = pd.read_csv(path)
                # normaliza header
                df.columns = [c.strip().lower() for c in df.columns]
                return path, df
            except Exception as e:
                print(f"##[warning]Falha lendo {path}: {e}", file=sys.stderr)
                continue
    print("##[error]Nenhum arquivo de entrada final encontrado em OUT_DIR.", file=sys.stderr)
    sys.exit(26)

def _ensure_probs(df: pd.DataFrame) -> pd.DataFrame:
    """Garante colunas prob_home/draw/away. Se não existirem, tenta a partir de odds."""
    has_ph = _first(df, PH_ALIASES)
    has_pd = _first(df, PD_ALIASES)
    has_pa = _first(df, PA_ALIASES)

    if has_ph and has_pd and has_pa:
        df["prob_home"] = df[has_ph].astype(float)
        df["prob_draw"] = df[has_pd].astype(float)
        df["prob_away"] = df[has_pa].astype(float)
        return df

    # tentar derivar de odds
    oh = _first(df, OH_ALIASES)
    od = _first(df, OD_ALIASES)
    oa = _first(df, OA_ALIASES)
    if oh and od and oa:
        for c in (oh, od, oa):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        # prob ~ 1/odds; normaliza para somar 1
        inv = pd.DataFrame({
            "h": 1.0 / df[oh],
            "d": 1.0 / df[od],
            "a": 1.0 / df[oa],
        })
        inv = inv.fillna(0.0).clip(lower=0.0)
        s = inv.sum(axis=1).replace(0, pd.NA)
        df["prob_home"] = (inv["h"] / s).astype(float)
        df["prob_draw"] = (inv["d"] / s).astype(float)
        df["prob_away"] = (inv["a"] / s).astype(float)
        return df

    # se chegou aqui, não há como obter probabilidades
    return df

def _normalize_pick_string(x: str) -> Optional[str]:
    if not isinstance(x, str):
        return None
    t = x.strip().lower()
    m = {
        "1": "1", "home": "1", "mandante": "1", "casa": "1", "h": "1",
        "x": "X", "draw": "X", "empate": "X",
        "2": "2", "away": "2", "visitante": "2", "fora": "2", "a": "2",
    }
    return m.get(t, None)

def _best_pick_from_probs(ph: float, pd: float, pa: float) -> Tuple[str, float]:
    vals = [("1", ph), ("X", pd), ("2", pa)]
    vals.sort(key=lambda z: z[1], reverse=True)
    return vals[0][0], float(vals[0][1])

def _choose_columns(df: pd.DataFrame) -> Tuple[str, str, Optional[str]]:
    hcol = _first(df, HOME_ALIASES)
    acol = _first(df, AWAY_ALIASES)
    mid  = _first(df, MID_ALIASES)
    return hcol or "", acol or "", mid

def _merge_with_matches(matches: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    # tenta merge por match_id se existir nos dois
    _, _, mid = _choose_columns(df)
    if mid and mid in df.columns:
        return matches.merge(df, left_on="match_id", right_on=mid, how="left")

    # senão, tenta chave por nome (casefold/strip)
    def kjoin(h, a):
        return (str(h).strip().lower(), str(a).strip().lower())

    mh = "home"; ma = "away"
    dh, da, _ = _choose_columns(df)
    if not dh or not da:
        # cria colunas vazias para evitar KeyError e permitir detectar faltas
        df["_tmp_home"] = ""
        df["_tmp_away"] = ""
        dh, da = "_tmp_home", "_tmp_away"

    A = matches.copy()
    A["_k"] = A.apply(lambda r: kjoin(r[mh], r[ma]), axis=1)

    B = df.copy()
    B["_k"] = B.apply(lambda r: kjoin(r[dh], r[da]), axis=1)

    out = A.merge(B, on="_k", how="left", suffixes=("",""))
    return out

def build_card(out_dir: str, strict: bool) -> None:
    path, df = _try_read(out_dir)

    # normaliza e garante probs
    df = _ensure_probs(df)

    # escolhe colunas de times
    hcol, acol, mid = _choose_columns(df)

    # tenta pick direto, se existir
    pick_col = _first(df, PICK_ALIASES)
    if pick_col:
        df["pick_norm"] = df[pick_col].apply(_normalize_pick_string)
    else:
        df["pick_norm"] = None

    # garantir colunas de times mínimas (se não houver, deixamos para o merge com matches)
    if not hcol: df["team_home"] = None; hcol = "team_home"
    if not acol: df["team_away"] = None; acol = "team_away"

    # faz merge com a lista de jogos para definir ordem e preencher nomes
    matches = _load_matches()
    merged = _merge_with_matches(matches, df)

    # se nomes não vieram do arquivo de previsões, usa os da entrada
    def _fallback(a, b):  # a preferencial, b fallback
        return a if (isinstance(a,str) and a.strip()) else b

    merged["home_final"] = merged.apply(lambda r: _fallback(r.get(hcol,""), r["home"]), axis=1)
    merged["away_final"] = merged.apply(lambda r: _fallback(r.get(acol,""), r["away"]), axis=1)

    # probabilidades finais (se existirem)
    for c in ["prob_home","prob_draw","prob_away"]:
        if c not in merged.columns:
            merged[c] = pd.NA
    merged[["prob_home","prob_draw","prob_away"]] = merged[["prob_home","prob_draw","prob_away"]].apply(pd.to_numeric, errors="coerce")

    # se não houver pick_norm, derive das probabilidades
    def _ensure_pick(r):
        if isinstance(r.get("pick_norm"), str) and r["pick_norm"] in ("1","X","2"):
            if r["pick_norm"] == "1":
                conf = r.get("prob_home", None)
            elif r["pick_norm"] == "X":
                conf = r.get("prob_draw", None)
            else:
                conf = r.get("prob_away", None)
            return r["pick_norm"], conf
        # deriva se tiver probs
        ph, pd_, pa = r.get("prob_home"), r.get("prob_draw"), r.get("prob_away")
        if pd.notna(ph) and pd.notna(pd_) and pd.notna(pa):
            return _best_pick_from_probs(float(ph or 0), float(pd_ or 0), float(pa or 0))
        return None, None

    picks, confs = [], []
    for _, row in merged.iterrows():
        p, c = _ensure_pick(row)
        picks.append(p)
        confs.append(c)

    merged["pick_final"] = picks
    merged["conf_final"] = confs

    # stakes (se vierem do kelly)
    stake_col = _first(merged, STAKE_ALIASES) or ""
    if stake_col:
        merged["stake_final"] = pd.to_numeric(merged[stake_col], errors="coerce")
    else:
        merged["stake_final"] = pd.NA

    # sanity por jogo
    miss_rows = merged[merged["pick_final"].isna()]
    if len(miss_rows) > 0 and strict:
        faltando = [{"match_id": r["match_id"], "home": r["home_final"], "away": r["away_final"]} for _, r in miss_rows.iterrows()]
        print(f"##[error][strict] Sem pick para {len(miss_rows)} jogo(s): {faltando}", file=sys.stderr)
        sys.exit(26)

    # monta linhas do cartão
    lines_txt: List[str] = []
    out_rows: List[Dict[str, str]] = []
    jogo_n = 1
    for _, r in merged.iterrows():
        home = str(r["home_final"])
        away = str(r["away_final"])
        pick = r["pick_final"] if isinstance(r["pick_final"], str) else "?"
        conf = r["conf_final"]
        try:
            conf_pct = f"{float(conf)*100:.1f}%" if conf is not None else ""
        except Exception:
            conf_pct = ""
        stake = r["stake_final"]
        stake_txt = f" (stake={stake:.1f})" if pd.notna(stake) else ""
        lines_txt.append(f"Jogo {jogo_n:02d} - {home} x {away}: {pick}{stake_txt} {('['+conf_pct+']') if conf_pct else ''}")
        out_rows.append({
            "jogo": jogo_n,
            "match_id": str(r.get("match_id","")),
            "home": home,
            "away": away,
            "pick": pick,
            "confidence": f"{conf:.6f}" if conf is not None else "",
            "stake": f"{stake:.4f}" if pd.notna(stake) else "",
            "fonte_arquivo": os.path.basename(path),
        })
        jogo_n += 1

    # grava TXT
    txt_path = os.path.join(out_dir, "loteca_cartao.txt")
    os.makedirs(out_dir, exist_ok=True)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("==== CARTÃO LOTECA ====\n")
        for ln in lines_txt:
            f.write(ln + "\n")
        f.write("=======================\n")

    # grava CSV
    csv_path = os.path.join(out_dir, "loteca_cartao.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["jogo","match_id","home","away","pick","confidence","stake","fonte_arquivo"])
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    # log
    print("==== CARTÃO LOTECA ====")
    for ln in lines_txt:
        print(ln)
    print("=======================")
    # fim

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", dest="out_dir", required=True, help="Diretório OUT da rodada (ex.: data/out/<RID>)")
    ap.add_argument("--strict", action="store_true", help="Falhar se faltar pick em algum jogo.")
    args = ap.parse_args()

    out_dir = args.out_dir
    if not out_dir:
        print("##[error]OUT_DIR/--rodada não informado.", file=sys.stderr)
        sys.exit(26)
    if not os.path.isdir(out_dir):
        print(f"##[error]Diretório de saída inexiste: {out_dir}", file=sys.stderr)
        sys.exit(26)

    build_card(out_dir, strict=args.strict)

if __name__ == "__main__":
    main()