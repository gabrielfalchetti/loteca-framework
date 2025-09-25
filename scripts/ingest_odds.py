#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, argparse, yaml, requests, pandas as pd, numpy as np
from pathlib import Path

def cfg(): return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

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
    s = p_home + p_draw + p_away
    if s <= 0: return np.nan, np.nan, np.nan, np.nan
    vig = max(0.0, s - 1.0)
    return p_home/s, p_draw/s, p_away/s, vig

def main(rodada: str):
    C = cfg()
    prov = C.get("provider", {})
    fx   = C["fixtures_odds"]
    sanity = C.get("sanity", {})
    min_books = int(sanity.get("min_bookmakers", 3))
    max_vig   = float(sanity.get("max_vig", 0.12))

    h = headers(fx["api_headers"], fx["api_token_env"])
    url_odds = fx["api_url_odds"].replace("${base_url}", prov["base_url"])
    params = {"league": prov.get("league_br"), "season": prov.get("season")}

    r = requests.get(url_odds, headers=h, params=params, timeout=40)
    if r.status_code >= 400:
        print(f"[WARN] odds HTTP {r.status_code}: {r.text[:400]}", file=sys.stderr)
    r.raise_for_status()
    j = r.json()
    resp = j.get("response", j)
    df = pd.json_normalize(resp)

    rows=[]
    for rec in df.to_dict("records"):
        mid = rec.get("fixture.id")
        bms = rec.get("bookmakers", [])
        if not isinstance(bms, list) or not bms:
            continue
        for bm in bms:
            bname = bm.get("name")
            bets = bm.get("bets", [])
            for bet in bets:
                m = bet.get("name","").lower()
                if ("match winner" in m) or ("1x2" in m) or ("winner" in m):
                    vals = bet.get("values", [])
                    d = {v.get("value","").lower(): v.get("odd") for v in vals}
                    oh = d.get("home") or d.get("1") or d.get("local_team") or d.get("home team")
                    od = d.get("draw") or d.get("x")
                    oa = d.get("away") or d.get("2") or d.get("away team") or d.get("visitor_team")
                    ph = implied_prob(oh); pd = implied_prob(od); pa = implied_prob(oa)
                    ph2,pd2,pa2,vig = devig(ph,pd,pa)
                    rows.append({"match_id": mid, "bookmaker": bname,
                                 "odd_home": oh, "odd_draw": od, "odd_away": oa,
                                 "p_home_raw": ph, "p_draw_raw": pd, "p_away_raw": pa,
                                 "p_home": ph2, "p_draw": pd2, "p_away": pa2, "vig": vig})
    out = pd.DataFrame(rows)

    odds_out = C["paths"]["odds_out"].replace("${rodada}", rodada)
    Path(odds_out).parent.mkdir(parents=True, exist_ok=True)

    if out.empty:
        print("[WARN] Nenhuma odds encontrada (verifique plano/endpoints).")
        out.to_csv(odds_out, index=False)
        print(f"[OK] odds (vazia) → {odds_out}")
        return

    out = out[out["vig"] <= max_vig].copy()
    valid_matches = out.groupby("match_id")["bookmaker"].nunique()
    keep_ids = set(valid_matches[valid_matches >= min_books].index)
    filtered = out[out["match_id"].isin(keep_ids)].copy()
    if filtered.empty:
        print("[WARN] Odds filtradas vazias; mantendo sem filtro.")
        filtered = out.copy()

    best = filtered.groupby("match_id", as_index=False)[["p_home","p_draw","p_away"]].median()
    best.to_csv(odds_out, index=False)
    print(f"[OK] odds → {odds_out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
