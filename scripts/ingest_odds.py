#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ingest_odds.py — Odds 1X2 com de-vig e validação (API-Football via RapidAPI)

Corrige o erro UnboundLocalError de 'pd' garantindo:
- import pandas as pd no topo
- nenhuma variável local chamada 'pd' no escopo da função
"""

import os
import sys
import argparse
import yaml
import requests
import pandas as pd       # <- manter este import
import numpy as np
from pathlib import Path


def load_cfg() -> dict:
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_headers(hcfg: dict, token_env: str) -> dict:
    """Monta headers substituindo ${TOKEN} e sanitiza o token de quebras de linha/tabs."""
    raw = os.getenv(token_env, "")
    tok = (raw or "").strip().replace("\r", "").replace("\n", "")
    if not tok:
        print(f"[ERRO] token ausente no env: {token_env}", file=sys.stderr)
        sys.exit(1)
    if any(c in tok for c in (" ", "\t")):
        print("[ERRO] token contém espaço/tab. Edite o Secret para 1 linha, sem espaços.", file=sys.stderr)
        sys.exit(1)
    out = {}
    for k, v in (hcfg or {}).items():
        out[k] = v.replace("${TOKEN}", tok)
    return out


def implied_prob(odd):
    try:
        o = float(odd)
        if o <= 1.0:
            return np.nan
        return 1.0 / o
    except Exception:
        return np.nan


def devig(p_home, p_draw, p_away):
    s = (p_home or 0.0) + (p_draw or 0.0) + (p_away or 0.0)
    if s <= 0:
        return np.nan, np.nan, np.nan, np.nan
    vig = max(0.0, s - 1.0)
    return (p_home / s if s else np.nan,
            p_draw / s if s else np.nan,
            p_away / s if s else np.nan,
            vig)


def fetch_odds(url_odds: str, headers_http: dict, params: dict) -> pd.DataFrame:
    r = requests.get(url_odds, headers=headers_http, params=params, timeout=40)
    if r.status_code >= 400:
        print(f"[WARN] odds HTTP {r.status_code}: {r.text[:400]}", file=sys.stderr)
    r.raise_for_status()
    j = r.json()
    response = j.get("response", j)
    return pd.json_normalize(response)


def main(rodada: str):
    cfg = load_cfg()
    provider = cfg.get("provider", {})
    fx = cfg["fixtures_odds"]
    sanity = cfg.get("sanity", {})

    min_books = int(sanity.get("min_bookmakers", 3))
    max_vig = float(sanity.get("max_vig", 0.12))

    # headers e endpoint
    headers_http = build_headers(fx["api_headers"], fx["api_token_env"])
    url_odds = fx["api_url_odds"].replace("${base_url}", provider["base_url"])
    # por padrão aqui usamos a liga BR configurada; ajuste se quiser outra liga
    params = {
        "league": provider.get("league_br"),
        "season": provider.get("season"),
    }

    df_odds_raw = fetch_odds(url_odds, headers_http, params)

    # parse estrutura: fixture.id -> bookmakers -> bets -> values (Home/Draw/Away)
    rows = []
    for rec in df_odds_raw.to_dict("records"):
        match_id = rec.get("fixture.id")
        bookmakers = rec.get("bookmakers", [])
        if not isinstance(bookmakers, list) or not bookmakers:
            continue
        for bm in bookmakers:
            bname = bm.get("name")
            bets = bm.get("bets", [])
            for bet in bets:
                market = (bet.get("name") or "").lower()
                if ("match winner" in market) or ("1x2" in market) or ("winner" in market):
                    vals = bet.get("values", [])
                    # mapear value -> odd
                    map_vals = {str(v.get("value", "")).lower(): v.get("odd") for v in vals}
                    odd_home = map_vals.get("home") or map_vals.get("1") or map_vals.get("local_team") or map_vals.get("home team")
                    odd_draw = map_vals.get("draw") or map_vals.get("x")
                    odd_away = map_vals.get("away") or map_vals.get("2") or map_vals.get("away team") or map_vals.get("visitor_team")

                    p_home_raw = implied_prob(odd_home)
                    p_draw_raw = implied_prob(odd_draw)
                    p_away_raw = implied_prob(odd_away)

                    p_home, p_draw, p_away, vig = devig(p_home_raw, p_draw_raw, p_away_raw)

                    rows.append({
                        "match_id": match_id,
                        "bookmaker": bname,
                        "odd_home": odd_home,
                        "odd_draw": odd_draw,
                        "odd_away": odd_away,
                        "p_home_raw": p_home_raw,
                        "p_draw_raw": p_draw_raw,
                        "p_away_raw": p_away_raw,
                        "p_home": p_home,
                        "p_draw": p_draw,
                        "p_away": p_away,
                        "vig": vig
                    })

    out = pd.DataFrame(rows)

    odds_out = cfg["paths"]["odds_out"].replace("${rodada}", rodada)
    Path(odds_out).parent.mkdir(parents=True, exist_ok=True)

    if out.empty:
        print("[WARN] Nenhuma odds encontrada (verifique plano/endpoints).")
        out.to_csv(odds_out, index=False)
        print(f"[OK] odds (vazia) → {odds_out}")
        return

    # filtros de sanidade
    out = out[out["vig"] <= max_vig].copy()
    # exigir mínimo de casas por jogo
    bm_counts = out.groupby("match_id")["bookmaker"].nunique()
    keep_ids = set(bm_counts[bm_counts >= min_books].index)
    filtered = out[out["match_id"].isin(keep_ids)].copy()
    if filtered.empty:
        print("[WARN] Odds filtradas ficaram vazias; mantendo sem filtro.")
        filtered = out.copy()

    # agregação robusta (mediana) por match
    best = filtered.groupby("match_id", as_index=False)[["p_home", "p_draw", "p_away"]].median()
    best.to_csv(odds_out, index=False)
    print(f"[OK] odds → {odds_out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
