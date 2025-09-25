#!/usr/bin/env python3
import os, sys, argparse, yaml, requests, pandas as pd
from pathlib import Path

def cfg():
    return yaml.safe_load(open("config/config.yaml", "r", encoding="utf-8"))

def headers(hcfg, token_env):
    """Monta headers substituindo ${TOKEN} e sanitiza o token (sem \n, \r, espaços/tabs)."""
    raw = os.getenv(token_env, "")
    token = (raw or "").strip().replace("\r", "").replace("\n", "")
    if not token:
        print(f"[ERRO] token ausente no env: {token_env}", file=sys.stderr); sys.exit(1)
    if any(ch in token for ch in (" ", "\t")):
        print("[ERRO] token contém espaço/tab. Edite o Secret para ser uma única linha, sem espaços.", file=sys.stderr); sys.exit(1)
    out = {}
    for k, v in (hcfg or {}).items():
        out[k] = v.replace("${TOKEN}", token)
    return out

def fill(template: dict, mapping: dict):
    """Substitui ${chaves} pelo mapping (ex.: ${base_url}, ${league}, ${season})."""
    if not template: return {}
    s = yaml.safe_dump(template)
    for k, v in mapping.items():
        s = s.replace("${"+k+"}", str(v))
    return yaml.safe_load(s)

def parse_rodada(rodada: str):
    """
    'YYYY-MM-DD_NN' -> data='YYYY-MM-DD', round_num='NN', round_api='Regular Season - NN'
    Se não tiver '_NN', usa só a data e faz fallback com ?date=YYYY-MM-DD.
    """
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

    # Headers + endpoints
    h = headers(fx["api_headers"], fx["api_token_env"])
    url_fix = fx["api_url_fixtures"].replace("${base_url}", prov["base_url"])

    # Converte rodada para formato aceito pela API-Football
    data_jogo, rn, round_api = parse_rodada(rodada)

    # Parâmetros base (league/season)
    base_params = {"league": prov.get("league"), "season": prov.get("season")}

    # Preferir 'round=Regular Season - N'; fallback para 'date=YYYY-MM-DD'
    params_fix = {**base_params}
    if round_api:
        params_fix["round"] = round_api
    else:
        params_fix["date"] = data_jogo

    # Chamada de fixtures
    try:
        r = requests.get(url_fix, headers=h, params=params_fix, timeout=40)
        if r.status_code >= 400:
            print(f"[ERRO] fixtures HTTP {r.status_code}: {r.text[:500]}", file=sys.stderr)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERRO] Falha ao obter fixtures: {e}", file=sys.stderr); sys.exit(1)

    jfix = r.json()
    fixtures = pd.json_normalize(jfix.get("response", jfix))

    # (Opcional) ODDS — alguns planos não permitem; se falhar, seguimos sem odds
    odds = pd.DataFrame()
    try:
        url_odds = fx["api_url_odds"].replace("${base_url}", prov["base_url"])
        params_odds = {**base_params}
        ro = requests.get(url_odds, headers=h, params=params_odds, timeout=40)
        if ro.status_code >= 400:
            print(f"[WARN] odds HTTP {ro.status_code}: {ro.text[:500]}", file=sys.stderr)
        ro.raise_for_status()
        jodds = ro.json()
        odds = pd.json_normalize(jodds.get("response", jodds))
    except Exception as e:
        print(f"[WARN] Falha ao obter odds: {e}")

    # salvar fixtures/odds
    fixtures_out = C["paths"]["fixtures_out"].replace("${rodada}", rodada)
    Path(fixtures_out).parent.mkdir(parents=True, exist_ok=True)
    fixtures.to_csv(fixtures_out, index=False)
    print(f"[OK] fixtures → {fixtures_out}")

    odds_out = C["paths"]["odds_out"].replace("${rodada}", rodada)
    if not odds.empty:
        odds.to_csv(odds_out, index=False)
        print(f"[OK] odds → {odds_out}")

    # gerar matches.csv para o pipeline
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
