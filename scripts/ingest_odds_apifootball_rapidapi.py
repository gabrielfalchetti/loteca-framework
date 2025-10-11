# scripts/ingest_odds_apifootball_rapidapi.py
# -*- coding: utf-8 -*-

"""
Ingestão de odds via API-Football (RapidAPI) com:
- normalização + fuzzy match;
- suporte a aliases em data/aliases.json;
- janela de datas para seleções (sem forçar season fixa);
- saída: {OUT_DIR}/odds_apifootball.csv (match_id,home,away,odds_home,odds_draw,odds_away).

Execução:
  python -m scripts.ingest_odds_apifootball_rapidapi \
      --rodada data/out/123456789 \
      --season 2025 \
      [--debug]
Requer:
  - env X_RAPIDAPI_KEY
  - arquivo {OUT_DIR}/matches_whitelist.csv com colunas: match_id,home,away
  - aliases em data/aliases.json  (formato {"teams": {"Nome Canonico": ["alias1", ...]}})
"""

import os
import sys
import csv
import json
import time
import math
import argparse
import datetime as dt
import unicodedata
from typing import Dict, List, Optional, Tuple

import requests
from rapidfuzz import process, fuzz  # pip install rapidfuzz
import pandas as pd


API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

REQ_TIMEOUT = 30
HARD_LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "7"))  # fallback maior p/ seleções


def log(msg: str):
    print(msg, flush=True)


def norm_ascii(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.replace("/", " ").replace("-", " ")
    return " ".join(s.split()).lower()


def fuzzy_pick(name: str, candidates: List[str], cutoff: int = 86) -> Optional[str]:
    if not name or not candidates:
        return None
    mapping = {c: norm_ascii(c) for c in candidates}
    best = process.extractOne(norm_ascii(name), mapping, scorer=fuzz.WRatio)
    if not best:
        return None
    (best_key, score, _idx) = best
    return best_key if score >= cutoff else None


def load_aliases(path: str = "data/aliases.json") -> Dict[str, List[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        teams = data.get("teams", {})
        # Canonical -> aliases (canonical também se torna alias)
        full = {}
        for canon, al in teams.items():
            al = al or []
            full[canon] = sorted(set([canon] + al))
        return full
    except FileNotFoundError:
        log("[apifootball][WARN] aliases.json não encontrado em data/aliases.json — prosseguindo sem aliases")
        return {}
    except Exception as e:
        log(f"[apifootball][WARN] falha ao carregar aliases.json: {e} — prosseguindo")
        return {}


def auth_headers() -> Dict[str, str]:
    key = os.environ.get("X_RAPIDAPI_KEY") or os.environ.get("RAPIDAPI_KEY")
    if not key:
        log("::error::X_RAPIDAPI_KEY ausente no ambiente")
        sys.exit(5)
    return {
        "x-rapidapi-key": key,
        "x-rapidapi-host": API_HOST,
    }


def rq(path: str, params: Dict) -> Dict:
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.get(url, params=params, headers=auth_headers(), timeout=REQ_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {url} {params} => {r.text[:300]}")
    js = r.json()
    # API-Football geralmente traz {"response":[...], "results":N}
    return js


def is_response_empty(js: Dict) -> bool:
    resp = js.get("response")
    if resp is None:
        return True
    if isinstance(resp, list) and len(resp) == 0:
        return True
    return False


def api_search_teams(query: str) -> List[Dict]:
    js = rq("teams", {"search": query})
    return js.get("response", []) or []


def team_pretty_name(t: Dict) -> str:
    # API structure: {"team":{"id":..,"name":"X","code":"YYY","country":"Brazil","national":False}, "venue":{...}}
    team = (t or {}).get("team", {})
    return team.get("name") or ""


def team_id(t: Dict) -> Optional[int]:
    team = (t or {}).get("team", {})
    return team.get("id")


def team_is_national(t: Dict) -> bool:
    team = (t or {}).get("team", {})
    return bool(team.get("national"))


def team_country(t: Dict) -> str:
    team = (t or {}).get("team", {})
    return team.get("country") or ""


NATIONAL_COUNTRIES = {
    "Bulgaria", "Turkey", "Türkiye", "Romania", "Austria",
    "Denmark", "Greece", "Lithuania", "Poland", "Brazil", "Portugal", "Spain",
    "Italy", "France", "Germany", "Netherlands", "England", "Ireland", "Serbia",
    "Albania", "Georgia", "Estonia", "Finland",  # etc.
}


def resolve_team_id(wanted: str, aliases: Dict[str, List[str]]) -> Tuple[Optional[int], Optional[str], Optional[bool], Optional[str]]:
    """
    Retorna: (id, nomeAPI, national?, country)
    Estratégia:
      1) tentar busca por cada alias (inclui o próprio canônico)
      2) se múltiplos, fuzzy por nome
    """
    # expande aliases
    candidates_terms: List[str] = []
    # Se "wanted" for um alias, descubra o canônico; caso contrário, o próprio
    for canon, al in aliases.items():
        allv = [canon] + al
        if wanted in allv:
            candidates_terms = allv
            break
    if not candidates_terms:
        # tentar achar quais canônicos possuem alias aproximado
        for canon, al in aliases.items():
            allv = [canon] + al
            if fuzzy_pick(wanted, allv, cutoff=96):
                candidates_terms = allv
                break
    if not candidates_terms:
        candidates_terms = [wanted]

    # busca e acumula times
    found: Dict[str, Dict] = {}
    for term in candidates_terms:
        js = api_search_teams(term)
        for obj in js:
            nm = team_pretty_name(obj)
            if nm:
                found[nm] = obj

    if not found:
        # fallback: uma única busca
        js = api_search_teams(wanted)
        for obj in js:
            nm = team_pretty_name(obj)
            if nm:
                found[nm] = obj

    if not found:
        return None, None, None, None

    # escolha por fuzzy contra nomes retornados
    best_name = fuzzy_pick(wanted, list(found.keys()), cutoff=86)
    if not best_name:
        # se não atingiu cutoff, pegue a 1a (heurística conservadora)
        best_name = list(found.keys())[0]

    obj = found[best_name]
    return team_id(obj), best_name, team_is_national(obj), team_country(obj)


def fixtures_h2h_in_window(home_id: int, away_id: int, date_from: str, date_to: str) -> List[Dict]:
    # v3/fixtures/headtohead?h2h=home-away&from=YYYY-MM-DD&to=YYYY-MM-DD
    js = rq("fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "from": date_from, "to": date_to})
    return js.get("response", []) or []


def fixtures_by_season(league_id: Optional[int], season: int, team_ids: List[int]) -> List[Dict]:
    """
    Para clubes, a rota mais comum é v3/fixtures?season=YYYY&team=ID (ou league + team).
    Aqui faremos por team para reduzir ambiguidades.
    """
    out = []
    for tid in team_ids:
        js = rq("fixtures", {"season": season, "team": tid})
        out.extend(js.get("response", []) or [])
        # pequeno delay anti rate-limit
        time.sleep(0.2)
    return out


def pick_best_fixture(fixtures: List[Dict], home_name: str, away_name: str) -> Optional[Dict]:
    """
    Seleciona o fixture que melhor casa (nome vs nome) e o mais próximo no tempo no horizonte.
    """
    if not fixtures:
        return None
    # filtra por nomes fuzzy (defensivo)
    scored = []
    for fx in fixtures:
        try:
            h = fx["teams"]["home"]["name"]
            a = fx["teams"]["away"]["name"]
            s_h = fuzz.WRatio(norm_ascii(home_name), norm_ascii(h))
            s_a = fuzz.WRatio(norm_ascii(away_name), norm_ascii(a))
            when = fx.get("fixture", {}).get("date")
            ts = 0
            if when:
                try:
                    ts = int(dt.datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp())
                except Exception:
                    ts = 0
            scored.append((min(s_h, s_a), -abs(int(time.time()) - ts), fx))
        except Exception:
            scored.append((0, 0, fx))
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    best = scored[0][2]
    return best


def extract_1x2_odds(odds_payload: Dict) -> Optional[Tuple[float, float, float]]:
    """
    odds v3/odds?fixture=ID
    Estrutura: response[0].bookmakers[].bets[].label in {"Match Winner","1X2"} e values com {value, odd}
    Retorna (home, draw, away) se encontrado.
    """
    resp = odds_payload.get("response", []) or []
    for item in resp:
        bookmakers = item.get("bookmakers") or []
        # varre bookies e bets procurando 1X2
        for bm in bookmakers:
            bets = bm.get("bets") or []
            for bet in bets:
                label = (bet.get("name") or bet.get("label") or "").strip().lower()
                if label in {"match winner", "1x2", "winner"}:
                    vals = bet.get("values") or []
                    home = draw = away = None
                    for v in vals:
                        vname = (v.get("value") or "").strip().lower()
                        odd = v.get("odd")
                        try:
                            oddf = float(odd)
                        except Exception:
                            continue
                        if vname in {"home", "1"}:
                            home = oddf
                        elif vname in {"draw", "x"}:
                            draw = oddf
                        elif vname in {"away", "2"}:
                            away = oddf
                    if home and draw and away:
                        return (home, draw, away)
    return None


def fetch_odds_for_fixture(fixture_id: int) -> Optional[Tuple[float, float, float]]:
    js = rq("odds", {"fixture": fixture_id})
    got = extract_1x2_odds(js)
    if got:
        return got
    # retry leve: odds/live como fallback
    try:
        js2 = rq("odds/live", {"fixture": fixture_id})
        got2 = extract_1x2_odds(js2)
        if got2:
            return got2
    except Exception:
        pass
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456789)")
    ap.add_argument("--season", required=False, type=int, default=int(os.environ.get("SEASON", "2025")))
    ap.add_argument("--aliases", required=False, default="data/aliases.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada_dir = args.rodada
    season = args.season
    debug = args.debug

    wl_path = os.path.join(rodada_dir, "matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        log("::error::whitelist ausente (matches_whitelist.csv)")
        sys.exit(5)

    # carrega aliases
    aliases_map = load_aliases(args.aliases)

    # janela de datas
    today = dt.date.today()
    lookahead = int(os.environ.get("LOOKAHEAD_DAYS", HARD_LOOKAHEAD_DAYS))
    date_from = today.isoformat()
    date_to = (today + dt.timedelta(days=lookahead)).isoformat()

    # lê whitelist
    rows = []
    with open(wl_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        need = {"match_id", "home", "away"}
        missing = need.difference({c.lower() for c in rdr.fieldnames})
        if missing:
            log(f"::error::missing columns in whitelist: {sorted(list(missing))}")
            sys.exit(5)
        for r in rdr:
            rows.append({
                "match_id": str(r.get("match_id") or r.get("MATCH_ID") or "").strip(),
                "home": (r.get("home") or r.get("HOME") or "").strip(),
                "away": (r.get("away") or r.get("AWAY") or "").strip(),
            })

    if debug:
        log(f"[apifootball][DEBUG] whitelist linhas={len(rows)} — janela {date_from} .. {date_to}")

    out_records = []
    missing_any = []

    for i, r in enumerate(rows, start=1):
        mid = r["match_id"]
        home_in = r["home"]
        away_in = r["away"]
        log(f"[apifootball] {i}: {home_in} x {away_in}")

        # resolve time por ID com aliases + fuzzy
        hid, hname, hnat, hcountry = resolve_team_id(home_in, aliases_map)
        if not hid:
            log(f"[apifootball][WARN] Mandante não encontrado: {home_in}")
            missing_any.append(mid)
            continue
        aid, aname, anat, acountry = resolve_team_id(away_in, aliases_map)
        if not aid:
            log(f"[apifootball][WARN] Visitante não encontrado: {away_in}")
            missing_any.append(mid)
            continue

        # Seleção? (heurística)
        is_national = bool(hnat or anat or hcountry in NATIONAL_COUNTRIES or acountry in NATIONAL_COUNTRIES)

        # Busca fixture
        fixtures = []
        try:
            if is_national:
                # H2H por janela (melhor p/ seleções)
                fixtures = fixtures_h2h_in_window(hid, aid, date_from=date_from, date_to=date_to)
            else:
                # clubes: pegar fixtures por season e filtrar H2H
                fx_h = fixtures_by_season(None, season, [hid])
                fx_a = fixtures_by_season(None, season, [aid])
                # filtra apenas confrontos diretos
                def _is_h2h(fx):
                    try:
                        th = fx["teams"]["home"]["id"]
                        ta = fx["teams"]["away"]["id"]
                        return {th, ta} == {hid, aid}
                    except Exception:
                        return False
                fixtures = [fx for fx in (fx_h + fx_a) if _is_h2h(fx)]
        except Exception as e:
            log(f"[apifootball][WARN] falha ao buscar fixtures para {hname} x {aname}: {e}")
            missing_any.append(mid)
            continue

        if not fixtures:
            log(f"[apifootball][WARN] Fixture não localizado para {hname or home_in} x {aname or away_in}")
            missing_any.append(mid)
            continue

        fx_best = pick_best_fixture(fixtures, hname or home_in, aname or away_in)
        if not fx_best:
            log(f"[apifootball][WARN] Fixture inadequado (sem matching) para {hname} x {aname}")
            missing_any.append(mid)
            continue

        fixture_id = fx_best["fixture"]["id"]
        try:
            odds = fetch_odds_for_fixture(fixture_id)
        except Exception as e:
            odds = None
            if debug:
                log(f"[apifootball][DEBUG] erro ao buscar odds do fixture {fixture_id}: {e}")

        if not odds:
            log(f"[apifootball][WARN] Odds 1x2 ausentes p/ fixture={fixture_id} ({hname} x {aname})")
            missing_any.append(mid)
            continue

        o_home, o_draw, o_away = odds
        out_records.append({
            "match_id": mid,
            "home": hname or home_in,
            "away": aname or away_in,
            "odds_home": f"{o_home:.3f}",
            "odds_draw": f"{o_draw:.3f}",
            "odds_away": f"{o_away:.3f}",
        })

        # pequeno delay anti rate-limit
        time.sleep(0.2)

    # saída
    out_csv = os.path.join(rodada_dir, "odds_apifootball.csv")
    if out_records:
        cols = ["match_id", "home", "away", "odds_home", "odds_draw", "odds_away"]
        with open(out_csv, "w", encoding="utf-8", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=cols)
            wr.writeheader()
            for rec in out_records:
                wr.writerow(rec)
        log(f"[apifootball] odds geradas: {len(out_records)} — arquivo: {out_csv}")
    else:
        log("::error::Nenhuma odd coletada da API-Football")
        sys.exit(5)

    # relatório final em modo estrito: aponta faltantes (para o step de consenso abortar, se quiser)
    if missing_any:
        log("::error::Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).")
        log(f"[apifootball][DEBUG] coletadas: {len(out_records)}  faltantes: {len(missing_any)} -> {missing_any}")
        # mantém exit 0 aqui porque quem decide FAIL-FAST é o step de consenso/strict.
        # Se quiser abortar aqui também, descomente:
        # sys.exit(5)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log(f"::error::Falha inesperada ingest_apifootball: {e}")
        sys.exit(5)