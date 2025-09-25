#!/usr/bin/env python3
import os, sys, argparse, yaml, requests, pandas as pd
from pathlib import Path

def cfg():
    return yaml.safe_load(open("config/config.yaml", "r", encoding="utf-8"))

def headers(hcfg, token_env):
    raw = os.getenv(token_env, "")
    token = (raw or "").strip().replace("\r","").replace("\n","")
    if not token:
        print(f"[ERRO] token ausente no env: {token_env}", file=sys.stderr); sys.exit(1)
    if any(ch in token for ch in (" ", "\t")):
        print("[ERRO] token contém espaço/tab. Edite o Secret p/ 1 linha.", file=sys.stderr); sys.exit(1)
    return {k: v.replace("${TOKEN}", token) for k,v in (hcfg or {}).items()}

def parse_rodada(rodada: str):
    # 'YYYY-MM-DD_NN' -> data, 'Regular Season - NN'
    try:
        data, rn = rodada.split("_", 1)
    except ValueError:
        data, rn = rodada, ""
    rn = "".join([c for c in rn if c.isdigit()])
    round_api = f"Regular Season - {rn}" if rn else None
    return data, rn, round_api

def main(rodada: str):
    C = cfg()
    prov = C.get("provider", {})
    fx = C["fixtures_odds"]

    h = headers(fx["api_headers"], fx["api_token_env"])
    url_fix = fx["api_url_fixtures"].replace("${base_url}", prov["base_url"])

    date_str, rn, round_api = parse_rodada(rodada)
    base_params = {"league": prov.get("league_br"), "season": prov.get("season")}
    params_fix = {**base_params}
    if round_api: params_fix["round"] = round_api
    else: params_fix["date"] = date_str

    r = requests.get(url_fix, headers=h, params=params_fix, timeout=40)
    if r.status_code >= 400:
        print(f"[ERRO] fixtures HTTP {r.status_code}: {r.text[:500]}", file=sys.stderr)
    r.raise_for_status()
    jfix = r.json()
    fixtures = pd.json_normalize(jfix.get("response", jfix))

    odds = pd.DataFrame()  # odds por endpoint específico será tratada em ingest_odds.py

    fixtures_out = C["paths"]["fixtures_out"].replace("${rodada}", rodada)
    Path(fixtures_out).parent.mkdir(parents=True, exist_ok=True)
    fixtures.to_csv(fixtures_out, index=False)
    print(f"[OK] fixtures → {fixtures_out}")

    rows=[]
    for rec in fixtures.to_dict("records"):
        mid = rec.get("fixture.id")
        home = rec.get("teams.home.name")
        away = rec.get("teams.away.name")
        kickoff = rec.get("fixture.date")
        stadium = rec.get("fixture.venue.name") or ""
        stadium_id = rec.get("fixture.venue.id") or (str(stadium).lower().replace(" ","_") if stadium else "")
        rows.append({
            "match_id": mid,
            "home": home,
            "away": away,
            "stadium_id": stadium_id or "unknown",
            "kickoff_utc": kickoff,
            "home_prob_market": None,
            "draw_prob_market": None,
            "away_prob_market": None,
            "home_form5": None, "away_form5": None,
            "home_rest_days": None, "away_rest_days": None,
            "news_home_hits": 0, "news_away_hits": 0,
            "is_past": 0
        })
    matches = pd.DataFrame(rows)
    matches_path = C["paths"]["matches_csv"].replace("${rodada}", rodada)
    Path(matches_path).parent.mkdir(parents=True, exist_ok=True)
    matches.to_csv(matches_path, index=False)
    print(f"[OK] matches (para pipeline) → {matches_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
