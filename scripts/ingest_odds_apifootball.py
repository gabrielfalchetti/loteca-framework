#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestor de odds da API-FOOTBALL (modo direto ou RapidAPI).
Versão unificada para o framework Loteca v4.3.RC1+.

Saída: <rodada>/odds_apifootball.csv
Colunas: match_id,home,away,odds_home,odds_draw,odds_away
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from unidecode import unidecode


# ------------------------- CLI -------------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--aliases", default="data/aliases.json")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()


def log(level, msg):
    print(f"[apifootball][{level}] {msg}", flush=True)


# ------------------------- HTTP Session -------------------------
def build_session():
    api_key = (os.getenv("API_FOOTBALL_KEY") or "").strip()
    rapid_key = (os.getenv("RAPIDAPI_KEY") or os.getenv("X_RAPIDAPI_KEY") or "").strip()

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    if api_key:
        base = "https://v3.football.api-sports.io"
        sess.headers["x-apisports-key"] = api_key
        mode = "direct"
    elif rapid_key:
        base = "https://api-football-v1.p.rapidapi.com/v3"
        sess.headers["X-RapidAPI-Key"] = rapid_key
        sess.headers["X-RapidAPI-Host"] = "api-football-v1.p.rapidapi.com"
        mode = "rapidapi"
    else:
        log("ERROR", "Nenhuma chave encontrada (API_FOOTBALL_KEY ou RAPIDAPI_KEY).")
        sys.exit(5)

    log("INFO", f"Usando modo {mode.upper()} com base {base}")
    return sess, base


def api_get(sess, base, path, params=None, retries=3, delay=3):
    url = f"{base}{path}"
    for i in range(retries):
        try:
            r = sess.get(url, params=params or {}, timeout=25)
            if r.status_code == 429:
                time.sleep(delay * (i + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log("WARN", f"Tentativa {i+1} falhou: {e}")
            time.sleep(delay)
    return None


# ------------------------- Utils -------------------------
def normalize(s): return unidecode(str(s or "")).strip().lower()


def load_aliases(path):
    if not os.path.isfile(path):
        log("INFO", f"aliases.json não encontrado em {path} — seguindo sem.")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        norm = {}
        for k, vals in data.items():
            norm[normalize(k)] = {normalize(v) for v in ([k] + (vals if isinstance(vals, list) else []))}
        log("INFO", f"{len(norm)} aliases carregados.")
        return norm
    except Exception as e:
        log("WARN", f"Falha lendo aliases.json: {e}")
        return {}


def alias_variants(name, aliases):
    base = normalize(name)
    if base in aliases:
        return list(aliases[base])
    for k, vs in aliases.items():
        if base in vs:
            return list(vs)
    return [name]


# ------------------------- API helpers -------------------------
def find_team(sess, base, name, aliases):
    for alt in alias_variants(name, aliases):
        try:
            data = api_get(sess, base, "/teams", {"search": alt})
            resp = (data or {}).get("response") or []
            if resp:
                team = resp[0]["team"]
                return team["id"], team["name"]
        except Exception as e:
            log("WARN", f"Falha ao buscar time '{alt}': {e}")
    return None, None


def find_fixture(sess, base, home_id, away_id, days_ahead=3):
    today = datetime.now(timezone.utc).date()
    to_date = today + timedelta(days=days_ahead)
    try:
        data = api_get(sess, base, "/fixtures", {"h2h": f"{home_id}-{away_id}", "from": today.isoformat(), "to": to_date.isoformat()})
        resp = (data or {}).get("response") or []
        if resp:
            return resp[0]["fixture"]["id"]
    except Exception:
        pass
    return None


def get_odds(sess, base, fixture_id):
    data = api_get(sess, base, "/odds", {"fixture": fixture_id})
    if not data:
        return None
    resp = (data or {}).get("response") or []
    for item in resp:
        for book in item.get("bookmakers", []):
            for bet in book.get("bets", []):
                name = (bet.get("name") or "").lower()
                if any(k in name for k in ["winner", "match winner", "1x2"]):
                    home = draw = away = None
                    for v in bet.get("values", []):
                        n = (v["value"] or "").lower()
                        try:
                            o = float(v["odd"])
                        except Exception:
                            o = None
                        if n in ("home", "1"): home = o
                        elif n in ("draw", "x"): draw = o
                        elif n in ("away", "2"): away = o
                    if all([home, draw, away]):
                        return home, draw, away
    return None


# ------------------------- MAIN -------------------------
def main():
    args = parse_args()
    rodada, season, aliases_path, debug = args.rodada, args.season, args.aliases, args.debug

    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        log("ERROR", f"Whitelist não encontrada: {wl_path}")
        sys.exit(5)

    df = pd.read_csv(wl_path)
    aliases = load_aliases(aliases_path)
    sess, base = build_session()

    results, missing = [], []

    for _, r in df.iterrows():
        mid, home, away = str(r["match_id"]), str(r["home"]), str(r["away"])
        log("INFO", f"{mid}: {home} x {away}")

        hid, hname = find_team(sess, base, home, aliases)
        aid, aname = find_team(sess, base, away, aliases)
        if not hid or not aid:
            log("WARN", f"Time não encontrado: {home if not hid else away}")
            missing.append(mid)
            continue

        fix = find_fixture(sess, base, hid, aid, days_ahead=int(os.getenv("LOOKAHEAD_DAYS", "3")))
        if not fix:
            log("WARN", f"Fixture não encontrado para {home} x {away}")
            missing.append(mid)
            continue

        odds = get_odds(sess, base, fix)
        if not odds:
            log("WARN", f"Odds não encontradas para fixture {fix}")
            missing.append(mid)
            continue

        oh, od, oa = odds
        results.append({"match_id": mid, "home": home, "away": away,
                        "odds_home": oh, "odds_draw": od, "odds_away": oa})

    out_path = os.path.join(rodada, "odds_apifootball.csv")
    pd.DataFrame(results).to_csv(out_path, index=False)

    if not results:
        log("ERROR", "Nenhuma odd coletada da API-Football.")
        sys.exit(5)

    if missing:
        log("ERROR", f"Jogos sem odds: {len(missing)} ({missing})")
        sys.exit(5)

    log("INFO", f"Odds coletadas: {len(results)} salvas em {out_path}")


if __name__ == "__main__":
    sys.exit(main())