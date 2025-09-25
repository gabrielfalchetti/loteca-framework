#!/usr/bin/env python3
import os, sys, argparse, yaml, requests, pandas as pd
from pathlib import Path

def cfg():
    return yaml.safe_load(open("config/config.yaml", "r", encoding="utf-8"))

def headers(hcfg, token_env):
    token = os.getenv(token_env, "")
    if not token:
        print(f"[ERRO] token ausente no env: {token_env}", file=sys.stderr); sys.exit(1)
    out = {}
    for k,v in (hcfg or {}).items():
        out[k] = v.replace("${TOKEN}", token)
    return out

def fill(template: dict, rodada: str, prov: dict):
    """substitui ${rodada}, ${base_url}, ${league}, ${season}…"""
    if not template: return {}
    s = yaml.safe_dump(template)
    s = s.replace("${rodada}", rodada)
    for k,v in prov.items():
        s = s.replace("${"+k+"}", str(v))
    return yaml.safe_load(s)

def main(rodada: str):
    C = cfg()
    prov = C.get("provider", {})
    # 1) Fixtures
    fcfg = C["fixtures_odds"]
    h = headers(fcfg["api_headers"], fcfg["api_token_env"])
    params_fix = fill(fcfg["params_fixtures"], rodada, prov)
    url_fix = fcfg["api_url_fixtures"].replace("${base_url}", prov["base_url"])
    r = requests.get(url_fix, headers=h, params=params_fix, timeout=40); r.raise_for_status()
    jfix = r.json()
    fixtures = pd.json_normalize(jfix.get("response", jfix))
    # 2) Odds (opcional: alguns planos limitam; se falhar, seguimos só com fixtures)
    try:
        params_odds = fill(fcfg["params_odds"], rodada, prov)
        url_odds = fcfg["api_url_odds"].replace("${base_url}", prov["base_url"])
        ro = requests.get(url_odds, headers=h, params=params_odds, timeout=40); ro.raise_for_status()
        jodds = ro.json()
        odds = pd.json_normalize(jodds.get("response", jodds))
    except Exception as e:
        print(f"[WARN] Falha ao obter odds: {e}")
        odds = pd.DataFrame()

    # salvar
    fixtures_out = C["paths"]["fixtures_out"].replace("${rodada}", rodada)
    Path(fixtures_out).parent.mkdir(parents=True, exist_ok=True)
    fixtures.to_csv(fixtures_out, index=False)
    print(f"[OK] fixtures → {fixtures_out}")

    odds_out = C["paths"]["odds_out"].replace("${rodada}", rodada)
    if not odds.empty:
        odds.to_csv(odds_out, index=False)
        print(f"[OK] odds → {odds_out}")

    # gerar matches.csv no formato mínimo do seu pipeline
    # mapeia campos típicos do API-Football
    rows=[]
    for rec in fixtures.to_dict("records"):
        mid = rec.get("fixture.id")
        home = rec.get("teams.home.name")
        away = rec.get("teams.away.name")
        kickoff = rec.get("fixture.date")
        # estádio (se vier)
        stadium = rec.get("fixture.venue.name") or ""
        stadium_id = rec.get("fixture.venue.id") or (str(stadium).lower().replace(" ","_") if stadium else "")
        rows.append({
            "match_id": mid,
            "home": home,
            "away": away,
            "stadium_id": stadium_id or "unknown",
            "kickoff_utc": kickoff,
            # campos que seu join usa (preencheremos depois conforme seus dados)
            "home_prob_market": None,
            "draw_prob_market": None,
            "away_prob_market": None,
            "home_form5": None, "away_form5": None,
            "home_rest_days": None, "away_rest_days": None,
            "news_home_hits": 0, "news_away_hits": 0,
            "is_past": 0
        })
    matches = pd.DataFrame(rows)
    # tentar preencher probs de mercado com odds (se disponível)
    if not odds.empty:
        # cada provedor tem formato; aqui um exemplo superficial:
        # você pode melhorar mapeando bookmaker/market específico (1X2).
        pass

    matches_path = C["paths"]["matches_csv"].replace("${rodada}", rodada)
    Path(matches_path).parent.mkdir(parents=True, exist_ok=True)
    matches.to_csv(matches_path, index=False)
    print(f"[OK] matches (para pipeline) → {matches_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
