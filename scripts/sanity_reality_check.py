#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import argparse
from typing import List, Dict, Any
import pandas as pd

REQ_ODDS_COLS = ["team_home", "team_away", "match_key", "odds_home", "odds_draw", "odds_away"]
REQ_PRED_COLS = ["match_key", "prob_home", "prob_draw", "prob_away"]
REQ_KELLY_COLS = ["match_key", "team_home", "team_away", "pick", "prob", "odds", "stake"]

def _exists_non_empty_csv(path: str, req_cols: List[str] = None) -> (bool, str, int):
    if not os.path.isfile(path):
        return False, f"arquivo ausente: {path}", 0
    try:
        df = pd.read_csv(path)
    except Exception as e:
        return False, f"falha ao ler {path}: {e}", 0
    if df.empty:
        return False, f"arquivo vazio: {path}", 0
    if req_cols:
        cols = [c.lower() for c in df.columns]
        miss = [c for c in req_cols if c not in [x.lower() for x in cols]]
        if miss:
            return False, f"colunas ausentes em {path}: {miss}", len(df)
    return True, "ok", len(df)

def _find_matches_input() -> str:
    """
    Suporta dois layouts:
      1) Simplificado: data/in/matches_source.csv
      2) Antigo por rodada: data/in/<RODADA_ID>/matches_source.csv (se existir)
    Dá preferência ao simplificado.
    """
    p1 = os.path.join("data", "in", "matches_source.csv")
    if os.path.isfile(p1):
        return p1
    # fallback: procurar uma pasta única dentro de data/in
    din = os.path.join("data", "in")
    if os.path.isdir(din):
        for name in sorted(os.listdir(din)):
            cand = os.path.join(din, name, "matches_source.csv")
            if os.path.isfile(cand):
                return cand
    return p1  # padrão (mesmo se não existir; a verificação vai acusar)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="OUT_DIR (ex.: data/out/<RUN_ID>)")
    ap.add_argument("--strict", action="store_true", help="Falhar se qualquer etapa vital estiver ausente/vazia")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    strict = args.strict
    debug = args.debug

    os.makedirs(out_dir, exist_ok=True)

    report: Dict[str, Any] = {"checks": [], "summary": {}, "strict": strict}
    errors = 0
    warnings = 0

    def add_check(name: str, ok: bool, msg: str, rows: int = 0, vital: bool = True):
        nonlocal errors, warnings
        level = "OK" if ok else ("ERRO" if vital else "AVISO")
        if not ok:
            if vital: errors += 1
            else: warnings += 1
        item = {"check": name, "status": level, "message": msg, "rows": rows, "vital": vital}
        report["checks"].append(item)
        tag = "[reality]"
        print(f"{tag} {level} {name}: {msg}")

    # 1) Matches reais (entrada)
    matches_path = _find_matches_input()
    ok, msg, n = _exists_non_empty_csv(matches_path, req_cols=["home", "away"])
    add_check("matches_source", ok, f"{msg} | linhas={n} | path={matches_path}", n, vital=True)

    # 2) Odds (consensus OU theoddsapi)
    odds_consensus = os.path.join(out_dir, "odds_consensus.csv")
    odds_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")

    ok_c, msg_c, n_c = _exists_non_empty_csv(odds_consensus, req_cols=REQ_ODDS_COLS)
    ok_t, msg_t, n_t = _exists_non_empty_csv(odds_theodds,   req_cols=REQ_ODDS_COLS)

    if ok_c:
        add_check("odds_consensus", True, f"{msg_c} | linhas={n_c} | path={odds_consensus}", n_c, vital=True)
        odds_ok = True
        odds_rows = n_c
    elif ok_t:
        add_check("odds_consensus", False, f"{msg_c}", 0, vital=False)  # aviso (não vital se temos theoddsapi)
        add_check("odds_theoddsapi", True, f"{msg_t} | linhas={n_t} | path={odds_theodds}", n_t, vital=True)
        odds_ok = True
        odds_rows = n_t
    else:
        add_check("odds_consensus", False, f"{msg_c}", 0, vital=True)
        add_check("odds_theoddsapi", False, f"{msg_t}", 0, vital=True)
        odds_ok = False
        odds_rows = 0

    # 3) Predições de mercado (probabilidades) — vital
    preds_path = os.path.join(out_dir, "predictions_market.csv")
    ok, msg, n = _exists_non_empty_csv(preds_path, req_cols=REQ_PRED_COLS)
    add_check("predictions_market", ok, f"{msg} | linhas={n} | path={preds_path}", n, vital=True)

    # 4) Kelly stakes — vital (garante que foi calculado com base em dados reais)
    kelly_path = os.path.join(out_dir, "kelly_stakes.csv")
    ok, msg, n = _exists_non_empty_csv(kelly_path, req_cols=REQ_KELLY_COLS)
    add_check("kelly_stakes", ok, f"{msg} | linhas={n} | path={kelly_path}", n, vital=True)

    # 5) News e Injuries — recomendados (podem ser não vitais se você ainda não ativou)
    news_path = os.path.join(out_dir, "news.csv")
    ok_news, msg_news, n_news = _exists_non_empty_csv(news_path)
    add_check("news", ok_news, f"{msg_news} | linhas={n_news} | path={news_path}", n_news, vital=False)

    inj_path = os.path.join(out_dir, "injuries.csv")
    ok_inj, msg_inj, n_inj = _exists_non_empty_csv(inj_path)
    add_check("injuries", ok_inj, f"{msg_inj} | linhas={n_inj} | path={inj_path}", n_inj, vital=False)

    # 6) Cartão — recomendado (gera após kelly). Se não existir, avisa.
    cart_path = os.path.join(out_dir, "loteca_cartao.txt")
    if os.path.isfile(cart_path) and os.path.getsize(cart_path) > 0:
        add_check("loteca_cartao", True, f"ok | path={cart_path}", 1, vital=False)
    else:
        add_check("loteca_cartao", False, f"cartão ausente ou vazio: {cart_path}", 0, vital=False)

    # Resumo e saída
    report["summary"] = {
        "errors": errors,
        "warnings": warnings,
        "out_dir": out_dir,
        "odds_rows": odds_rows,
        "matches_path": matches_path,
    }

    # Persistir relatórios
    json_path = os.path.join(out_dir, "reality_report.json")
    txt_path = os.path.join(out_dir, "reality_report.txt")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    with open(txt_path, "w", encoding="utf-8") as f:
        for c in report["checks"]:
            f.write(f"{c['status']} {c['check']}: {c['message']}\n")
        f.write(f"\nErros: {errors}  |  Avisos: {warnings}\n")

    print(f"[reality] OK -> {json_path}")
    print(f"[reality] RESUMO -> {txt_path}")

    if strict and errors > 0:
        sys.exit(2)
    sys.exit(0)

if __name__ == "__main__":
    main()