#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json
import time
import argparse
from datetime import datetime, timedelta, timezone
from dateutil import tz
import requests
from unidecode import unidecode

# Regras: APIs obrigatórias. Falha se qualquer uma não responder com jogos.
# Output: data/in/matches_whitelist.csv  (match_id,home,away)

def log(msg):
    print(msg, flush=True)

def err(msg, code=3):
    print(f"::error::{msg}", flush=True)
    sys.exit(code)

def norm_team(name: str) -> str:
    if name is None:
        return ""
    s = unidecode(str(name)).lower().strip()
    # simplificações comuns
    for tok in [" fc", " sc", " afc", " cfd", " sp", ".", ","]:
        s = s.replace(tok, " ")
    s = s.replace("-", " ")
    s = " ".join(s.split())
    return s

def get_time_window(days_ahead: int):
    now_utc = datetime.now(timezone.utc)
    end_utc = now_utc + timedelta(days=days_ahead)
    # formatos ISO sem micros
    start = now_utc.replace(microsecond=0).isoformat()
    end = end_utc.replace(microsecond=0).isoformat()
    return start, end

def fetch_theodds(regions: str, lookahead_days: int, api_key: str, debug: bool=False):
    # Documentação TheOddsAPI: endpoint /v4/sports/upcoming/odds
    # Vamos pedir mercados "h2h" (3-way). Regiões do input (uk,eu,us,au)
    start, end = get_time_window(lookahead_days)
    params = {
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
        "apiKey": api_key,
      #  "commenceTimeFrom": start,   # alguns planos não aceitam ambos; cuidado
        "commenceTimeTo": end,
    }
    url = "https://api.the-odds-api.com/v4/sports/upcoming/odds"
    r = requests.get(url, params=params, timeout=30)
    if debug:
        log(f"[whitelist][theodds] GET {r.url} -> {r.status_code}")
    if r.status_code != 200:
        err(f"[whitelist][theodds] HTTP {r.status_code}: {r.text}", code=31)
    data = r.json()
    # Extrair pares home/away quando possível
    matches = []
    for item in data:
        # Alguns esportes retornam 'home_team' e 'away_team'
        home = item.get("home_team")
        away = None
        # Tentar deduzir away pelo 'bookmakers' outcomes quando necessário
        if "bookmakers" in item and item["bookmakers"]:
            # procurar outcomes com names
            outcomes = []
            for bm in item["bookmakers"]:
                for mk in bm.get("markets", []):
                    if mk.get("key") == "h2h":
                        for oc in mk.get("outcomes", []):
                            outcomes.append(oc.get("name"))
                if outcomes:
                    break
            # heurística: se home_team presente e outcomes tem 2 ou 3 times, escolher o que não é home com melhor match
            outs_norm = [norm_team(x) for x in outcomes if x]
            hnorm = norm_team(home) if home else ""
            cand = [o for o in outs_norm if o and o != hnorm]
            # Não temos certeza do ordenamento; manter None se não conseguir
            if cand:
                # pegar a string original correspondente ao primeiro cand
                idx = outs_norm.index(cand[0])
                away = outcomes[idx]
        # fallback: alguns retornos tem "away_team"
        if not away:
            away = item.get("away_team")

        if not home or not away:
            # pular entradas sem pares
            continue

        matches.append({
            "home": home,
            "away": away,
            "commence_time": item.get("commence_time"),
        })
    if debug:
        log(f"[whitelist][theodds] jogos brutos: {len(matches)}")
    return matches

def fetch_apifootball_rapidapi(lookahead_days: int, rapid_key: str, season: str, debug: bool=False):
    # API-Football (RapidAPI): usar endpoint fixtures entre datas (próximos dias)
    # https://api-football-v1.p.rapidapi.com/v3/fixtures?from=YYYY-MM-DD&to=YYYY-MM-DD
    now = datetime.utcnow().date()
    to = now + timedelta(days=lookahead_days)
    headers = {
        "X-RapidAPI-Key": rapid_key,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    params = {
        "from": now.isoformat(),
        "to": to.isoformat(),
        # Não filtramos por league aqui para cobrir o máximo, o consensus/ingest filtrará
        # "season": season,  # API-Football exige season para ligas específicas; deixamos amplo
        "timezone": "UTC"
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if debug:
        log(f"[whitelist][apifootball] GET {r.url} -> {r.status_code}")
    if r.status_code != 200:
        err(f"[whitelist][apifootball] HTTP {r.status_code}: {r.text}", code=32)
    js = r.json()
    resp = js.get("response", [])
    matches = []
    for fx in resp:
        teams = fx.get("teams", {})
        home = teams.get("home", {}).get("name")
        away = teams.get("away", {}).get("name")
        if home and away:
            matches.append({
                "home": home,
                "away": away,
                "date": fx.get("fixture", {}).get("date"),
            })
    if debug:
        log(f"[whitelist][apifootball] jogos brutos: {len(matches)}")
    return matches

def intersect_matches(theodds_list, api_list, debug=False):
    # indexar por (home_norm, away_norm)
    idx_theodds = {}
    for m in theodds_list:
        key = (norm_team(m["home"]), norm_team(m["away"]))
        if key[0] and key[1]:
            idx_theodds[key] = (m["home"], m["away"])

    out = []
    for m in api_list:
        key = (norm_team(m["home"]), norm_team(m["away"]))
        if key in idx_theodds:
            # usar nomes da API-Football (tendem a ser “oficiais”) para reduzir variação
            out.append({
                "home": m["home"],
                "away": m["away"]
            })
    if debug:
        log(f"[whitelist] interseção: {len(out)}")
    return out

def main():
    ap = argparse.ArgumentParser(description="Gera data/in/matches_whitelist.csv automáticamente a partir de TheOddsAPI e API-Football (RapidAPI).")
    ap.add_argument("--out", required=True, help="Arquivo de saída (ex.: data/in/matches_whitelist.csv)")
    ap.add_argument("--season", required=True, help="Temporada (ex.: 2025)")
    ap.add_argument("--regions", required=True, help="Regiões TheOddsAPI (ex.: uk,eu,us,au)")
    ap.add_argument("--lookahead-days", type=int, default=3, help="Dias à frente para buscar jogos (default: 3)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    theodds_key = os.getenv("THEODDS_API_KEY", "")
    rapid_key = os.getenv("X_RAPIDAPI_KEY", "")

    if not theodds_key:
        err("THEODDS_API_KEY ausente no ambiente", code=11)
    if not rapid_key:
        err("X_RAPIDAPI_KEY ausente no ambiente", code=12)

    if args.debug:
        log(f"[whitelist] params: season={args.season}, regions={args.regions}, lookahead={args.lookahead_days}")

    # Fetch das duas fontes (OBRIGATÓRIO)
    try:
        theodds_matches = fetch_theodds(args.regions, args.lookahead_days, theodds_key, args.debug)
    except Exception as e:
        err(f"[whitelist] Falha ao consultar TheOddsAPI: {e}", code=31)

    try:
        apifoot_matches = fetch_apifootball_rapidapi(args.lookahead_days, rapid_key, args.season, args.debug)
    except Exception as e:
        err(f"[whitelist] Falha ao consultar API-Football: {e}", code=32)

    if len(theodds_matches) == 0:
        err("[whitelist] TheOddsAPI retornou 0 jogos no intervalo. Não é permitido.", code=33)
    if len(apifoot_matches) == 0:
        err("[whitelist] API-Football retornou 0 jogos no intervalo. Não é permitido.", code=34)

    inter = intersect_matches(theodds_matches, apifoot_matches, args.debug)
    if len(inter) == 0:
        # Diagnóstico útil:
        # Imprimir até 10 nomes normalizados para ajudar ajuste posterior
        td_keys = set((norm_team(m['home']), norm_team(m['away'])) for m in theodds_matches if m.get('home') and m.get('away'))
        af_keys = set((norm_team(m['home']), norm_team(m['away'])) for m in apifoot_matches if m.get('home') and m.get('away'))
        only_td = list(td_keys - af_keys)[:10]
        only_af = list(af_keys - td_keys)[:10]
        log("[whitelist][DEBUG] exemplos apenas TheOdds (até 10): " + json.dumps(only_td))
        log("[whitelist][DEBUG] exemplos apenas APIFootball (até 10): " + json.dumps(only_af))
        err("[whitelist] Interseção vazia entre TheOdds e API-Football. Verifique padronização de nomes/ligas e janela de datas.", code=35)

    # Gerar IDs determinísticos a partir da string "home|away"
    rows = []
    for m in inter:
        key = f"{m['home']}|{m['away']}"
        # hash curto estável
        match_id = abs(hash(key)) % (10**10)
        rows.append({
            "match_id": str(match_id),
            "home": m["home"],
            "away": m["away"]
        })

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["match_id", "home", "away"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    log(f"[whitelist] OK -> {args.out} (linhas={len(rows)})")

if __name__ == "__main__":
    main()