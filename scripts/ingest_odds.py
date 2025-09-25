#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ingest_odds.py — Busca odds 1X2 POR JOGO (fixture) na API-Football (RapidAPI),
converte em probabilidades, remove a margem (de-vig), aplica filtros de sanidade
e agrega por mediana.

Saída: data/processed/odds_<rodada>.csv com colunas: match_id, p_home, p_draw, p_away
"""

import os, sys, argparse, yaml, requests, pandas as pd, numpy as np
from pathlib import Path
from time import sleep

def load_cfg():
    with open("config/config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def build_headers(hcfg: dict, token_env: str) -> dict:
    raw = os.getenv(token_env, "")
    tok = (raw or "").strip().replace("\r","").replace("\n","")
    if not tok:
        print(f"[ERRO] token ausente no env: {token_env}", file=sys.stderr); sys.exit(1)
    if any(c in tok for c in (" ", "\t")):
        print("[ERRO] token contém espaço/tab. Edite o Secret p/ 1 linha.", file=sys.stderr); sys.exit(1)
    return {k: v.replace("${TOKEN}", tok) for k,v in (hcfg or {}).items()}

def implied_prob(odd):
    try:
        o = float(odd)
        if o <= 1.0: return np.nan
        return 1.0 / o
    except Exception:
        return np.nan

def devig(p_home, p_draw, p_away):
    s = (p_home or 0.0) + (p_draw or 0.0) + (p_away or 0.0)
    if s <= 0: return np.nan, np.nan, np.nan, np.nan
    vig = max(0.0, s - 1.0)
    return p_home/s, p_draw/s, p_away/s, vig

def get_fixture_odds(url_odds, headers_http, fixture_id, timeout=30, retries=2, backoff=1.5):
    """Busca odds para UM fixture. Tenta algumas vezes (retries)."""
    params = {"fixture": int(fixture_id)}
    for i in range(retries+1):
        try:
            r = requests.get(url_odds, headers=headers_http, params=params, timeout=timeout)
            if r.status_code >= 400:
                print(f"[WARN] fixture={fixture_id} HTTP {r.status_code}: {r.text[:200]}", file=sys.stderr)
            r.raise_for_status()
            j = r.json()
            return j.get("response", j)
        except requests.RequestException as e:
            if i == retries: raise
            sleep(backoff**i)
    return []

def main(rodada: str):
    C = load_cfg()
    prov = C.get("provider", {})
    fx   = C["fixtures_odds"]
    sanity = C.get("sanity", {})
    min_books = int(sanity.get("min_bookmakers", 2))  # um pouco mais permissivo
    max_vig   = float(sanity.get("max_vig", 0.15))    # idem

    # arquivos de entrada/saída
    fixtures_out = C["paths"]["fixtures_out"].replace("${rodada}", rodada)
    if not Path(fixtures_out).exists():
        print(f"[ERRO] fixtures não encontrado: {fixtures_out}", file=sys.stderr); sys.exit(2)
    df_fix = pd.read_csv(fixtures_out)
    if df_fix.empty:
        print("[ERRO] fixtures vazio.", file=sys.stderr); sys.exit(2)

    # headers e endpoint
    headers_http = build_headers(fx["api_headers"], fx["api_token_env"])
    url_odds = fx["api_url_odds"].replace("${base_url}", prov["base_url"])

    rows = []
    # para cada jogo, pedir odds por fixture
    for rec in df_fix.to_dict("records"):
        match_id = rec.get("fixture.id") or rec.get("match_id")
        if pd.isna(match_id): continue
        try:
            resp = get_fixture_odds(url_odds, headers_http, int(match_id))
        except Exception as e:
            print(f"[WARN] odds falharam p/ match_id={match_id}: {e}", file=sys.stderr)
            continue

        # normaliza bookmakers/bets/values
        df = pd.json_normalize(resp)
        if df.empty:
            continue

        for r in df.to_dict("records"):
            bms = r.get("bookmakers", [])
            if not isinstance(bms, list): continue
            for bm in bms:
                bname = bm.get("name")
                bets = bm.get("bets", [])
                for bet in bets:
                    market = (bet.get("name") or "").lower()
                    if ("match winner" in market) or ("1x2" in market) or ("winner" in market):
                        vals = bet.get("values", [])
                        d = {str(v.get("value","")).lower(): v.get("odd") for v in vals}
                        oh = d.get("home") or d.get("1") or d.get("local_team") or d.get("home team")
                        od = d.get("draw") or d.get("x")
                        oa = d.get("away") or d.get("2") or d.get("away team") or d.get("visitor_team")
                        ph = implied_prob(oh); pdp = implied_prob(od); pa = implied_prob(oa)
                        ph2, pd2, pa2, vig = devig(ph, pdp, pa)
                        rows.append({
                            "match_id": int(match_id),
                            "bookmaker": bname,
                            "odd_home": oh, "odd_draw": od, "odd_away": oa,
                            "p_home_raw": ph, "p_draw_raw": pdp, "p_away_raw": pa,
                            "p_home": ph2, "p_draw": pd2, "p_away": pa2,
                            "vig": vig
                        })

    out = pd.DataFrame(rows)
    odds_out = C["paths"]["odds_out"].replace("${rodada}", rodada)
    Path(odds_out).parent.mkdir(parents=True, exist_ok=True)

    if out.empty:
        print("[WARN] Nenhuma odds por fixture encontrada. Verifique plano/limites/rodada.")
        # ainda assim escreve arquivo vazio para auditoria
        out.to_csv(odds_out, index=False)
        print(f"[OK] odds (vazia) → {odds_out}")
        return

    # Regras de sanidade
    out = out[out["vig"].astype(float) <= max_vig].copy()
    bm_counts = out.groupby("match_id")["bookmaker"].nunique()
    keep_ids = set(bm_counts[bm_counts >= min_books].index)
    filtered = out[out["match_id"].isin(keep_ids)].copy()
    if filtered.empty:
        print("[WARN] filtro deixou vazio; mantendo sem filtro.")
        filtered = out.copy()

    # Agrega por mediana
    best = filtered.groupby("match_id", as_index=False)[["p_home","p_draw","p_away"]].median()
    best.to_csv(odds_out, index=False)
    print(f"[OK] odds → {odds_out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
