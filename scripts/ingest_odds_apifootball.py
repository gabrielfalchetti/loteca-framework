#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coleta odds na API-Football (API-Sports).
Saída: <rodada>/odds_apifootball.csv
Colunas: match_id,home,away,odds_home,odds_draw,odds_away
"""

import os
import re
import sys
import json
import time
import math
import argparse
from datetime import datetime, timedelta, timezone
from unicodedata import normalize as _ucnorm

import requests
import pandas as pd

CSV_COLS = ["match_id", "home", "away", "odds_home", "odds_draw", "odds_away"]

# ----------------- utils/log -----------------
def log(level, msg):
    print(f"[apifootball][{level}] {msg}", flush=True)

def env_float(name, default):
    v = os.getenv(name, "")
    try:
        return float(v)
    except Exception:
        return float(default)

# ----------------- normalização/aliases -----------------
def _deacc(s: str) -> str:
    return _ucnorm("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")

def norm_key(s: str) -> str:
    s = _deacc(s).lower()
    # remove UF ("/SP", "/PR" etc)
    s = re.sub(r"/[a-z]{2}($|[^a-z])", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_aliases_lenient(path: str) -> dict[str, set[str]]:
    if not os.path.isfile(path):
        return {}
    try:
        txt = open(path, "r", encoding="utf-8").read()
        # remove comentários e vírgulas finais
        txt = re.sub(r"//.*", "", txt)
        txt = re.sub(r"/\*.*?\*/", "", txt, flags=re.S)
        txt = re.sub(r",\s*([\]}])", r"\1", txt)
        data = json.loads(txt)
    except Exception as e:
        log("WARN", f"Falha lendo aliases.json: {e}")
        return {}
    norm = {}
    for k, vals in (data or {}).items():
        base = norm_key(k)
        lst = vals if isinstance(vals, list) else []
        group = {norm_key(k)} | {norm_key(v) for v in lst}
        norm[base] = group
    return norm

def alias_variants(name: str, aliases: dict[str, set[str]]) -> list[str]:
    base = norm_key(name)
    if base in aliases:
        cands = list(aliases[base])
    else:
        cands = [base]
        for _, group in aliases.items():
            if base in group:
                cands = list(group)
                break
    # sempre incluir a forma original também
    out = list({*cands, norm_key(name)})
    return out

# ----------------- API-Football -----------------
class APIFootball:
    def __init__(self, api_key: str, base="https://v3.football.api-sports.io"):
        self.base = base.rstrip("/")
        self.headers = {"x-apisports-key": api_key}
        self._team_cache = {}

    def get(self, path, params=None, retries=3, delay=1.5):
        url = f"{self.base}/{path.lstrip('/')}"
        for i in range(retries):
            try:
                r = requests.get(url, headers=self.headers, params=params or {}, timeout=25)
                if r.status_code == 429:
                    time.sleep(delay * (i + 1))
                    continue
                r.raise_for_status()
                j = r.json()
                return j.get("response", j)
            except Exception as e:
                if i == retries - 1:
                    log("WARN", f"GET {path} falhou: {e}")
                time.sleep(delay)
        return []

    def search_team(self, name: str, prefer_type: str | None = None):
        """Busca time (Club/National) por string; escolhe melhor match."""
        key = norm_key(name) + f"|{prefer_type or ''}"
        if key in self._team_cache:
            return self._team_cache[key]

        # alguns aliases "especiais"
        hacks = {
            "operario pr": "operario ferroviario",
            "america mineiro": "america mg",
            "gremio novorizontino": "gremio novorizontino",
            "criciuma": "criciuma",
            "cuiaba": "cuiaba",
        }
        q = hacks.get(norm_key(name), name)

        resp = self.get("teams", {"search": q})
        if not resp:
            self._team_cache[key] = None
            return None

        cand = []
        nk = norm_key(name)
        for it in resp:
            t = it.get("team", {})
            typ = it.get("team", {}).get("national", False)
            tname = t.get("name", "")
            nk_t = norm_key(tname)
            score = 0
            if nk == nk_t:
                score += 100
            if nk in nk_t or nk_t in nk:
                score += 50
            if prefer_type is not None:
                if prefer_type.lower() == "national" and typ:
                    score += 10
                if prefer_type.lower() == "club" and not typ:
                    score += 10
            cand.append((score, t.get("id"), tname, typ))

        cand.sort(reverse=True)
        best = cand[0] if cand else None
        self._team_cache[key] = best
        return best

    def window(self, lookahead_days: float):
        tz = timezone.utc
        start = datetime.now(tz).date()
        end = start + timedelta(days=max(1, math.ceil(lookahead_days)))
        return start.isoformat(), end.isoformat()

    def find_fixture_between(self, home_id: int, away_id: int, start: str, end: str):
        # busca por janela a partir do mandante
        fx = self.get("fixtures", {"team": home_id, "from": start, "to": end})
        for f in fx:
            t_home = f.get("teams", {}).get("home", {}).get("id")
            t_away = f.get("teams", {}).get("away", {}).get("id")
            if t_home == home_id and t_away == away_id:
                return f

        # tenta a partir do visitante
        fx = self.get("fixtures", {"team": away_id, "from": start, "to": end})
        for f in fx:
            t_home = f.get("teams", {}).get("home", {}).get("id")
            t_away = f.get("teams", {}).get("away", {}).get("id")
            if t_home == home_id and t_away == away_id:
                return f

        # como fallback, expande +7 dias
        fx = self.get("fixtures", {"team": home_id, "from": start, "to": (datetime.fromisoformat(end)+timedelta(days=7)).date().isoformat()})
        for f in fx:
            t_home = f.get("teams", {}).get("home", {}).get("id")
            t_away = f.get("teams", {}).get("away", {}).get("id")
            if t_home == home_id and t_away == away_id:
                return f

        return None

    def get_odds_1x2(self, fixture_id: int):
        data = self.get("odds", {"fixture": fixture_id})
        if not data:
            return None
        # Estrutura: bookmakers -> bets -> outcomes
        # Procurar mercado 1X2 (nome comum: "Match Winner" ou similar)
        best = None
        for bk in data:
            for bet in bk.get("bets", []):
                name = (bet.get("name") or "").lower()
                if "match" in name and ("winner" in name or "result" in name):
                    # outcomes: value in ["Home","Draw","Away"] ou nomes de times
                    oh = od = oa = None
                    for o in bet.get("values", []):
                        vname = (o.get("value") or "").lower()
                        price = o.get("odd")
                        try:
                            price = float(str(price).replace(",", "."))
                        except Exception:
                            price = None
                        if price is None:
                            continue
                        if vname in ("home", "1", "home team"):
                            oh = price
                        elif vname in ("draw", "x"):
                            od = price
                        elif vname in ("away", "2", "away team"):
                            oa = price
                    if oh and od and oa:
                        best = (oh, od, oa)
                        break
            if best:
                break
        return best

# ----------------- main -----------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", required=False)  # não forçamos na busca por data
    ap.add_argument("--aliases", default="data/aliases.json")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

def main():
    args = parse_args()
    rodada = args.rodada
    lookahead = env_float("LOOKAHEAD_DAYS", 3.0)

    api_key = (os.getenv("API_FOOTBALL_KEY") or "").strip()
    if not api_key:
        log("ERROR", "API_FOOTBALL_KEY ausente nos secrets.")
        sys.exit(5)

    wl_path = os.path.join(rodada, "matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        log("ERROR", f"Whitelist não encontrada: {wl_path}")
        pd.DataFrame(columns=CSV_COLS).to_csv(os.path.join(rodada, "odds_apifootball.csv"), index=False)
        sys.exit(5)

    aliases = load_aliases_lenient(args.aliases)
    if aliases:
        log("INFO", f"{len(aliases)} aliases carregados.")
    else:
        log("INFO", "Sem aliases (ou falha na leitura).")

    api = APIFootball(api_key)
    start, end = api.window(lookahead)
    log("INFO", f"Janela de busca {start} -> {end}")

    df = pd.read_csv(wl_path)
    results, missing = [], []

    for _, r in df.iterrows():
        mid = str(r["match_id"])
        home = str(r["home"])
        away = str(r["away"])
        log("INFO", f"{mid}: {home} x {away}")

        # tenta preferir tipo (National se parecer seleção)
        prefer_home = "national" if norm_key(home) in {"italy","estonia","spain","georgia","serbia","albania","portugal","ireland","netherlands","finland","romania","austria","denmark","greece","lithuania","poland"} else None
        prefer_away = "national" if norm_key(away) in {"italy","estonia","spain","georgia","serbia","albania","portugal","ireland","netherlands","finland","romania","austria","denmark","greece","lithuania","poland"} else None

        th = api.search_team(home, prefer_type=prefer_home)
        ta = api.search_team(away, prefer_type=prefer_away)
        if not th or not ta:
            log("WARN", f"Time não encontrado: {home if not th else away}")
            missing.append(mid)
            continue

        home_id = th[1]
        away_id = ta[1]

        fx = api.find_fixture_between(home_id, away_id, start, end)
        if not fx:
            log("WARN", f"Fixture não encontrado para {home} x {away}")
            missing.append(mid)
            continue

        fid = fx.get("fixture", {}).get("id")
        odds = api.get_odds_1x2(fid)
        if not odds:
            log("WARN", f"Odds não encontradas para fixture {fid} ({home} x {away})")
            missing.append(mid)
            continue

        oh, od, oa = odds
        results.append(dict(match_id=mid, home=home, away=away,
                            odds_home=oh, odds_draw=od, odds_away=oa))

    out_path = os.path.join(rodada, "odds_apifootball.csv")
    pd.DataFrame(results, columns=CSV_COLS).to_csv(out_path, index=False)

    if not results:
        log("ERROR", "Nenhuma odd coletada da API-Football.")
        sys.exit(5)

    if missing:
        log("ERROR", f"Jogos sem odds: {len(missing)} -> {missing}")
        sys.exit(5)

    log("INFO", f"Odds coletadas: {len(results)} -> {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())