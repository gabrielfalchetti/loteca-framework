#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ingest_odds.py — Odds 1X2 por fixture (robusto)
- Lê fixtures_<rodada>.csv, itera pelos fixture.id e chama /odds?fixture=<id>.
- Converte odds em probabilidades e faz de-vig.
- Agrega por mediana por match_id (e aplica filtros de sanidade).
"""

import os, sys, argparse, yaml, requests, pandas as pd, numpy as np
from pathlib import Path
from time import sleep

def cfg(): 
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def headers(hcfg, token_env):
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

def get_odds_for_fixture(url_odds, h, fixture_id, retries=2, backoff=1.6):
    for i in range(retries+1):
        try:
            r = requests.get(url_odds, headers=h, params={"fixture": fixture_id}, timeout=40)
            if r.status_code >= 400:
                print(f"[WARN] fixture {fixture_id} odds HTTP {r.status_code}: {r.text[:180]}", file=sys.stderr)
            r.raise_for_status()
            return r.json().get("response", [])
        except requests.RequestException as e:
            if i==retries: raise
            sleep(backoff**i)

def main(rodada: str):
    C = cfg()
    prov = C.get("provider", {})
    fx   = C["fixtures_odds"]
    sanity = C.get("sanity", {})
    min_books = int(sanity.get("min_bookmakers", 3))
    max_vig   = float(sanity.get("max_vig", 0.12))

    fixtures_path = C["paths"]["fixtures_out"].replace("${rodada}", rodada)
    if not Path(fixtures_path).exists():
        print(f"[ERRO] fixtures não encontrado: {fixtures_path}", file=sys.stderr); sys.exit(2)
    fixtures = pd.read_csv(fixtures_path)
    if fixtures.empty:
        print(f"[ERRO] fixtures vazio: {fixtures_path}", file=sys.stderr); sys.exit(2)

    h = headers(fx["api_headers"], fx["api_token_env"])
    url_odds = fx["api_url_odds"].replace("${base_url}", prov["base_url"])

    rows=[]
    for rec in fixtures.to_dict("records"):
        mid = rec.get("fixture.id")
        if pd.isna(mid): continue
        try:
            resp = get_odds_for_fixture(url_odds, h, int(mid))
        except Exception as e:
            print(f"[WARN] Falha odds p/ fixture {mid}: {e}", file=sys.stderr)
            continue

        # normaliza bookmakers/bets/values
        for book in (resp or []):
            bname = book.get("bookmaker", {}).get("name") or book.get("bookmakers", [{}])[0].get("name") if isinstance(book.get("bookmakers", None), list) else None
        # API-Football v3 devolve: response: [ { bookmaker: {...}, bets: [ {name, values:[{value,odd}]} ] } ]
            bookmaker = (book.get("bookmaker") or {}).get("name") or book.get("name")
            bets = book.get("bets", [])
            for bet in bets:
                name = (bet.get("name") or "").lower()
                if ("match winner" in name) or ("1x2" in name) or ("winner" in name):
                    vals = bet.get("values", [])
                    m = {str(v.get("value","")).lower(): v.get("odd") for v in vals}
                    oh = m.get("home") or m.get("1") or m.get("local_team") or m.get("home team")
                    od = m.get("draw") or m.get("x")
                    oa = m.get("away") or m.get("2") or m.get("away team") or m.get("visitor_team")
                    ph = implied_prob(oh); pdv = implied_prob(od); pa = implied_prob(oa)
                    ph2,pd2,pa2,vig = devig(ph,pdv,pa)
                    rows.append({"match_id": mid, "bookmaker": bookmaker,
                                 "odd_home": oh, "odd_draw": od, "odd_away": oa,
                                 "p_home_raw": ph, "p_draw_raw": pdv, "p_away_raw": pa,
                                 "p_home": ph2, "p_draw": pd2, "p_away": pa2, "vig": vig})

    out = pd.DataFrame(rows)
    odds_out = C["paths"]["odds_out"].replace("${rodada}", rodada)
    Path(odds_out).parent.mkdir(parents=True, exist_ok=True)

    if out.empty:
        print("[WARN] odds ficaram vazias (plano/endpoint?). Salvando vazio.", file=sys.stderr)
        out.to_csv(odds_out, index=False)
        print(f"[OK] odds (vazia) → {odds_out}")
        return

    out = out[out["vig"] <= max_vig].copy()
    bm_counts = out.groupby("match_id")["bookmaker"].nunique()
    keep_ids = set(bm_counts[bm_counts >= min_books].index)
    filtered = out[out["match_id"].isin(keep_ids)].copy()
    if filtered.empty:
        print("[WARN] Após filtros, sem linhas; mantendo tudo.", file=sys.stderr)
        filtered = out.copy()

    best = filtered.groupby("match_id", as_index=False)[["p_home","p_draw","p_away"]].median()
    best.to_csv(odds_out, index=False)
    print(f"[OK] odds → {odds_out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
