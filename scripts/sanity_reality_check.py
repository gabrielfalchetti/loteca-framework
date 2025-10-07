#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sanity_reality_check.py
-----------------------
Executa checagens básicas de integridade dos artefatos gerados na rodada.
Gera:
- reality_report.json
- reality_report.txt
"""

import argparse
import json
import os
import pandas as pd


def resolve_out_dir(rodada: str) -> str:
    if os.path.isdir(rodada):
        return rodada
    path = os.path.join("data", "out", str(rodada))
    os.makedirs(path, exist_ok=True)
    return path


def check_file_columns(path, required):
    if not os.path.isfile(path):
        return False, f"arquivo ausente: {path}"
    try:
        df = pd.read_csv(path, nrows=3)
    except Exception as e:
        return False, f"erro leitura: {e}"
    miss = [c for c in required if c not in df.columns]
    if miss:
        return False, f"colunas ausentes em {path}: {miss}"
    return True, f"ok | linhas={sum(1 for _ in open(path)) - 1} | path={path}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    report = {}

    # matches_source
    ms = "data/in/matches_source.csv"
    ok, msg = check_file_columns(ms, ["match_id", "home", "away", "source", "lat", "lon"])
    print(f"[reality] {'OK' if ok else 'ERRO'} matches_source: {msg}")
    report["matches_source"] = {"ok": ok, "msg": msg}

    # odds_consensus
    fp = os.path.join(out_dir, "odds_consensus.csv")
    ok, msg = check_file_columns(fp, ["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"])
    print(f"[reality] {'OK' if ok else 'ERRO'} odds_consensus: {msg}")
    report["odds_consensus"] = {"ok": ok, "msg": msg}

    # odds_theoddsapi
    fp = os.path.join(out_dir, "odds_theoddsapi.csv")
    ok, msg = check_file_columns(fp, ["match_id", "home", "away", "region", "sport", "odds_home", "odds_draw", "odds_away"])
    print(f"[reality] {'OK' if ok else 'ERRO'} odds_theoddsapi: {msg}")
    report["odds_theoddsapi"] = {"ok": ok, "msg": msg}

    # predictions_market
    fp = os.path.join(out_dir, "predictions_market.csv")
    # aceita tanto prob_* quanto p_* (para nosso pipeline)
    if os.path.isfile(fp):
        df = pd.read_csv(fp, nrows=1)
        if all(c in df.columns for c in ["prob_home", "prob_draw", "prob_away"]):
            req = ["match_key", "home", "away", "odd_home", "odd_draw", "odd_away", "prob_home", "prob_draw", "prob_away"]
        else:
            req = ["match_key", "home", "away", "odd_home", "odd_draw", "odd_away", "p_home", "p_draw", "p_away"]
        ok, msg = check_file_columns(fp, req)
    else:
        ok, msg = False, f"arquivo ausente: {fp}"
    print(f"[reality] {'OK' if ok else 'ERRO'} predictions_market: {msg}")
    report["predictions_market"] = {"ok": ok, "msg": msg}

    # kelly
    fp = os.path.join(out_dir, "kelly_stakes.csv")
    ok, msg = check_file_columns(fp, ["match_key", "team_home", "team_away", "pick", "prob", "odds", "stake"])
    print(f"[reality] {'OK' if ok else 'ERRO'} kelly_stakes: {msg}")
    report["kelly_stakes"] = {"ok": ok, "msg": msg}

    # news
    fp = os.path.join(out_dir, "news.csv")
    ok, msg = check_file_columns(fp, ["source", "author", "title", "description", "url", "publishedAt"])
    print(f"[reality] {'OK' if ok else 'ERRO'} news: {msg}")
    report["news"] = {"ok": ok, "msg": msg}

    # injuries
    fp = os.path.join(out_dir, "injuries.csv")
    ok, msg = check_file_columns(fp, ["team_id", "team_name"])
    print(f"[reality] {'OK' if ok else 'ERRO'} injuries: {msg}")
    report["injuries"] = {"ok": ok, "msg": msg}

    # cartao
    fp = os.path.join(out_dir, "loteca_cartao.txt")
    ok = os.path.isfile(fp)
    msg = f"ok | path={fp}" if ok else f"arquivo ausente: {fp}"
    print(f"[reality] {'OK' if ok else 'ERRO'} loteca_cartao: {msg}")
    report["loteca_cartao"] = {"ok": ok, "msg": msg}

    # salva json e resumo
    json_fp = os.path.join(out_dir, "reality_report.json")
    with open(json_fp, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    txt_fp = os.path.join(out_dir, "reality_report.txt")
    with open(txt_fp, "w", encoding="utf-8") as f:
        for k, v in report.items():
            f.write(f"{k}: {'OK' if v['ok'] else 'ERRO'} - {v['msg']}\n")
    print(f"[reality] OK -> {json_fp}")
    print(f"[reality] RESUMO -> {txt_fp}")


if __name__ == "__main__":
    main()