#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestor de odds da API-FOOTBALL (direto ou via RapidAPI).
Saída: <rodada>/odds_apifootball.csv
Colunas: match_id,home,away,odds_home,odds_draw,odds_away
"""

import os
import re
import sys
import json
import time
import argparse
from datetime import datetime, timedelta, timezone
from unicodedata import normalize as _ucnorm

import pandas as pd
import requests


CSV_COLS = ["match_id", "home", "away", "odds_home", "odds_draw", "odds_away"]


def log(level, msg):
    print(f"[apifootball][{level}] {msg}", flush=True)


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--aliases", default="data/aliases.json")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()


def build_session():
    api_key = (os.getenv("API_FOOTBALL_KEY") or "").strip()
    rapid_key = (os.getenv("RAPIDAPI_KEY") or os.getenv("X_RAPIDAPI_KEY") or "").strip()

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    if api_key:
        base = "https://v3.football.api-sports.io"
        sess.headers["x-apisports-key"] = api_key
        mode = "DIRECT"
    elif rapid_key:
        base = "https://api-football-v1.p.rapidapi.com/v3"
        sess.headers["X-RapidAPI-Key"] = rapid_key
        sess.headers["X-RapidAPI-Host"] = "api-football-v1.p.rapidapi.com"
        mode = "RAPIDAPI"
    else:
        log("ERROR", "Nenhuma chave encontrada (API_FOOTBALL_KEY ou RAPIDAPI_KEY/X_RAPIDAPI_KEY).")
        sys.exit(5)

    log("INFO", f"Usando modo {mode} com base {base}")
    return sess, base


def api_get(sess, base, path, params=None, retries=3, delay=2):
    url = f"{base}{path}"
    for i in range(retries):
        try:
            r = sess.get(url, params=params or {}, timeout=25)
            if r.status_code == 429:
                # rate limit — backoff simples
                time.sleep(delay * (i + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log("WARN", f"Tentativa {i+1} {path} falhou: {e}")
            time.sleep(delay)
    return None


# --------- Normalização / Aliases ---------

def _deaccent(s: str) -> str:
    return _ucnorm("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")


def norm_key(s: str) -> str:
    s = _deaccent(s).lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _strip_common_tokens(name: str) -> list[str]:
    """
    Gera variações sem tokens comuns (fc, sc, ac, gremio, atletico, etc)
    para melhorar o match no /teams?search=...
    """
    toks = norm_key(name).split()
    drop = {"fc", "sc", "ac", "ec", "afc", "cf", "club", "clube", "gremio", "grêmio",
            "atletico", "atlético", "america", "américa", "de", "do", "da"}
    core = [t for t in toks if t not in drop]
    variants = []
    if core:
        variants.append(" ".join(core))
        # última palavra (ex.: "novorizontino")
        variants.append(core[-1])
    return [v for v in variants if v and v != norm_key(name)]


def load_aliases_lenient(path: str) -> dict[str, set[str]]:
    """
    Carrega aliases permitindo comentários e vírgulas sobrando.
    Formato esperado:
      {
        "America Mineiro": ["America-MG", "América/MG"],
        "Novorizontino": ["Gremio Novorizontino", "Grêmio Novorizontino"]
      }
    """
    if not os.path.isfile(path):
        log("INFO", f"aliases.json não encontrado em {path} — seguindo sem.")
        return {}

    try:
        txt = open(path, "r", encoding="utf-8").read()
        # remove comentários //... e /* ... */
        txt = re.sub(r"//.*", "", txt)
        txt = re.sub(r"/\*.*?\*/", "", txt, flags=re.S)
        # remove vírgulas finais antes de } ]
        txt = re.sub(r",\s*([\]}])", r"\1", txt)
        data = json.loads(txt)
        norm = {}
        for k, vals in (data or {}).items():
            base = norm_key(k)
            lst = vals if isinstance(vals, list) else []
            allv = {norm_key(k)} | {norm_key(v) for v in lst}
            norm[base] = allv
        log("INFO", f"{len(norm)} aliases carregados (lenient).")
        return norm
    except Exception as e:
        log("WARN", f"Falha lendo aliases.json: {e}")
        return {}


def alias_variants(name: str, aliases: dict[str, set[str]]) -> list[str]:
    """
    Retorna variações normalizadas a tentar: aliases, variações sem tokens e o nome original.
    """
    out = []
    base = norm_key(name)
    # do dicionário
    if base in aliases:
        out.extend(list(aliases[base]))
    else:
        # se o nome aparecer como alias de outra chave
        for _, group in aliases.items():
            if base in group:
                out.extend(list(group))
                break

    # variações "inteligentes" (ex.: tirar 'gremio' -> 'novorizontino')
    out.extend(_strip_common_tokens(name))

    # por último, o nome original (sem normalizar, já que a API é case-insensitive)
    out.append(name)

    # dedup mantendo ordem
    seen, uniq = set(), []
    for v in out:
        if v not in seen:
            seen.add(v)
            uniq.append(v)
    return uniq if uniq else [name]


# --------- Buscas na API ---------

def find_team(sess, base, name: str, aliases: dict[str, set[str]]):
    for alt in alias_variants(name, aliases):
        data = api_get(sess, base, "/teams", {"search": alt})
        resp = (data or {}).get("response") or []
        if not resp:
            continue
        # tenta pegar o match que mais parece com o 'alt'
        best = resp[0]["team"]
        return best["id"], best["name"]
    return None, None


def _pick_fixture_by_teams(fixtures: list, hid: int, aid: int) -> int | None:
    for f in fixtures:
        try:
            th = f["teams"]["home"]["id"]
            ta = f["teams"]["away"]["id"]
            fid = f["fixture"]["id"]
            if th == hid and ta == aid:
                return fid
        except Exception:
            pass
    return None


def find_fixture(sess, base, home_id: int, away_id: int, look_ahead_days: int, season: str | None):
    today = datetime.now(timezone.utc).date()
    to_date = today + timedelta(days=look_ahead_days)

    # 1) tentativa: fixtures por intervalo de datas + h2h (quando suportado)
    data = api_get(sess, base, "/fixtures", {
        "h2h": f"{home_id}-{away_id}",
        "from": today.isoformat(),
        "to": to_date.isoformat(),
        **({"season": season} if season else {})
    })
    resp = (data or {}).get("response") or []
    fid = _pick_fixture_by_teams(resp, home_id, away_id)
    if fid:
        return fid

    # 2) fallback: próximos jogos do mandante e filtra pelo visitante
    data = api_get(sess, base, "/fixtures", {"team": home_id, "next": 25})
    resp = (data or {}).get("response") or []
    fid = _pick_fixture_by_teams(resp, home_id, away_id)
    if fid:
        return fid

    # 3) fallback: próximos do visitante e filtra pelo mandante
    data = api_get(sess, base, "/fixtures", {"team": away_id, "next": 25})
    resp = (data or {}).get("response") or []
    fid = _pick_fixture_by_teams(resp, home_id, away_id)
    if fid:
        return fid

    # 4) último recurso: varrer por data (hoje até N dias) — pode ser pesado, mas N é pequeno
    for d in (today + timedelta(days=i) for i in range(look_ahead_days + 1)):
        data = api_get(sess, base, "/fixtures", {"date": d.isoformat()})
        resp = (data or {}).get("response") or []
        fid = _pick_fixture_by_teams(resp, home_id, away_id)
        if fid:
            return fid

    return None


def get_odds(sess, base, fixture_id: int):
    data = api_get(sess, base, "/odds", {"fixture": fixture_id})
    if not data:
        return None
    resp = (data or {}).get("response") or []
    # A API traz bookies -> bets -> values
    for item in resp:
        for book in item.get("bookmakers", []):
            for bet in book.get("bets", []):
                name = (bet.get("name") or "").lower()
                if not any(k in name for k in ("winner", "match winner", "1x2", "win-draw-win", "full time result")):
                    continue
                home = draw = away = None
                for v in bet.get("values", []):
                    n = (v.get("value") or "").strip().lower()
                    try:
                        o = float(v.get("odd"))
                    except Exception:
                        o = None
                    if n in {"home", "1", "home team"}:
                        home = o
                    elif n in {"draw", "x"}:
                        draw = o
                    elif n in {"away", "2", "away team"}:
                        away = o
                if all([home, draw, away]):
                    return home, draw, away
    return None


# --------- Main ---------

def main():
    args = parse_args()
    rodada = args.rodada
    season = str(args.season).strip() if args.season else None
    aliases_path = args.aliases
    look_ahead = int(float(os.getenv("LOOKAHEAD_DAYS", "3")))

    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        log("ERROR", f"Whitelist não encontrada: {wl_path}")
        # Ainda assim, gravar CSV vazio com cabeçalho para não quebrar step seguinte
        pd.DataFrame(columns=CSV_COLS).to_csv(os.path.join(rodada, "odds_apifootball.csv"), index=False)
        sys.exit(5)

    df = pd.read_csv(wl_path)
    aliases = load_aliases_lenient(aliases_path)
    sess, base = build_session()

    results = []
    missing = []

    for _, r in df.iterrows():
        mid = str(r["match_id"])
        home = str(r["home"])
        away = str(r["away"])
        log("INFO", f"{mid}: {home} x {away}")

        hid, hname = find_team(sess, base, home, aliases)
        aid, aname = find_team(sess, base, away, aliases)
        if not hid or not aid:
            log("WARN", f"Time não encontrado: {home if not hid else away}")
            missing.append(mid)
            continue

        fid = find_fixture(sess, base, hid, aid, look_ahead, season)
        if not fid:
            log("WARN", f"Fixture não encontrado para {home} x {away}")
            missing.append(mid)
            continue

        odds = get_odds(sess, base, fid)
        if not odds:
            log("WARN", f"Odds não encontradas para fixture {fid}")
            missing.append(mid)
            continue

        oh, od, oa = odds
        results.append(dict(match_id=mid, home=home, away=away,
                            odds_home=oh, odds_draw=od, odds_away=oa))

    out_path = os.path.join(rodada, "odds_apifootball.csv")
    # SEMPRE escreve cabeçalho correto, mesmo que vazio
    pd.DataFrame(results, columns=CSV_COLS).to_csv(out_path, index=False)

    if not results:
        log("ERROR", "Nenhuma odd coletada da API-Football.")
        sys.exit(5)

    if missing:
        log("ERROR", f"Jogos sem odds: {len(missing)} -> {missing}")
        sys.exit(5)

    log("INFO", f"Odds coletadas: {len(results)} salvas em {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())