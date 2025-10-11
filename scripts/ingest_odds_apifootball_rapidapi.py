# scripts/ingest_odds_apifootball_rapidapi.py
# -*- coding: utf-8 -*-

"""
Ingestão de odds via API-Football (RapidAPI) com:
- normalização + fuzzy match;
- suporte a aliases em data/aliases.json (opcional);
- janela de datas para seleções (H2H) e clubes (season + H2H do ano);
- saída: {OUT_DIR}/odds_apifootball.csv (match_id,home,away,odds_home,odds_draw,odds_away).

Execução:
  python -m scripts.ingest_odds_apifootball_rapidapi \
      --rodada data/out/123456789 \
      --season 2025 \
      [--debug]

Requer:
  - env X_RAPIDAPI_KEY
  - arquivo {OUT_DIR}/matches_whitelist.csv com colunas: match_id,home,away
  - aliases opcional em data/aliases.json  ({"teams": {"Nome Canonico": ["alias1", ...]}})
"""

import os
import sys
import csv
import json
import time
import argparse
import datetime as dt
import unicodedata
from typing import Dict, List, Optional, Tuple

import requests
from rapidfuzz import process, fuzz  # pip install rapidfuzz

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"
REQ_TIMEOUT = 30
HARD_LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "7"))  # fallback maior p/ seleções

NATIONAL_COUNTRIES = {
    "Bulgaria","Turkey","Türkiye","Romania","Austria",
    "Denmark","Greece","Lithuania","Poland","Brazil","Portugal","Spain",
    "Italy","France","Germany","Netherlands","England","Ireland","Serbia",
    "Albania","Georgia","Estonia","Finland"
}

def log(msg: str):
    print(msg, flush=True)

def norm_ascii(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.replace("/", " ").replace("-", " ")
    return " ".join(s.split()).lower()

def fuzzy_pick(query: str, candidates: List[str], cutoff: int = 86) -> Optional[str]:
    if not query or not candidates:
        return None
    # RapidFuzz pode trabalhar com lista diretamente
    best = process.extractOne(norm_ascii(query), [norm_ascii(c) for c in candidates], scorer=fuzz.WRatio)
    if not best:
        return None
    # best -> (match_text_normalized, score, index)
    score = best[1]
    idx = best[2]
    if score < cutoff:
        return None
    return candidates[idx]

def load_aliases(path: str = "data/aliases.json") -> Dict[str, List[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        teams = data.get("teams", {}) or {}
        full = {}
        for canon, al in teams.items():
            al = (al or [])
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
    return {"x-rapidapi-key": key, "x-rapidapi-host": API_HOST}

def rq(path: str, params: Dict) -> Dict:
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.get(url, params=params, headers=auth_headers(), timeout=REQ_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {url} {params} => {r.text[:300]}")
    try:
        return r.json()
    except Exception as e:
        raise RuntimeError(f"JSON parse error: {e} - body[:200]={r.text[:200]}")

def api_search_teams(query: str) -> List[Dict]:
    try:
        js = rq("teams", {"search": query})
        return js.get("response", []) or []
    except Exception as e:
        log(f"[apifootball][WARN] falha api_search_teams('{query}'): {e}")
        return []

def team_name(obj: Dict) -> str:
    return (((obj or {}).get("team") or {}).get("name")) or ""

def team_code(obj: Dict) -> str:
    return (((obj or {}).get("team") or {}).get("code")) or ""

def team_country(obj: Dict) -> str:
    return (((obj or {}).get("team") or {}).get("country")) or ""

def team_is_national(obj: Dict) -> bool:
    return bool(((obj or {}).get("team") or {}).get("national"))

def team_id(obj: Dict) -> Optional[int]:
    tid = (((obj or {}).get("team") or {}).get("id"))
    try:
        return int(tid) if tid is not None else None
    except Exception:
        return None

def resolve_team_id(wanted: str, aliases: Dict[str, List[str]]) -> Tuple[Optional[int], Optional[str], Optional[bool], Optional[str]]:
    """
    Retorna: (id, nomeAPI, national?, country)
    Robusta a ausência de aliases e a múltiplos candidatos.
    """
    wanted = (wanted or "").strip()
    if not wanted:
        return None, None, None, None

    # 1) Constrói termos de busca a partir dos aliases (se houver)
    terms: List[str] = []
    if aliases:
        # se 'wanted' é alias de algum canônico, usa todos os aliases desse canônico
        for canon, al in aliases.items():
            allv = [canon] + (al or [])
            if wanted in allv:
                terms = allv
                break
        # senão, tenta achar canônico por fuzzy
        if not terms:
            for canon, al in aliases.items():
                allv = [canon] + (al or [])
                if fuzzy_pick(wanted, allv, cutoff=96):
                    terms = allv
                    break
    if not terms:
        terms = [wanted]

    # 2) Busca na API por todos os termos e agrega candidatos únicos
    found_by_name: Dict[str, Dict] = {}
    for term in terms:
        for obj in api_search_teams(term):
            nm = team_name(obj)
            if nm:
                found_by_name[nm] = obj
        # pequeno delay anti rate limit
        time.sleep(0.15)

    # fallback: busca uma vez pelo termo original caso nada tenha vindo
    if not found_by_name:
        for obj in api_search_teams(wanted):
            nm = team_name(obj)
            if nm:
                found_by_name[nm] = obj

    if not found_by_name:
        return None, None, None, None

    # 3) Escolha por fuzzy, mas também olhando country/code se ajudar
    names = list(found_by_name.keys())
    best_name = fuzzy_pick(wanted, names, cutoff=84)  # ligeiramente mais permissivo
    if not best_name:
        # sem cutoff, pega o primeiro
        best_name = names[0]

    obj = found_by_name.get(best_name) or next(iter(found_by_name.values()))
    return team_id(obj), team_name(obj), team_is_national(obj), team_country(obj)

def fixtures_h2h_in_window(home_id: int, away_id: int, date_from: str, date_to: str) -> List[Dict]:
    try:
        js = rq("fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "from": date_from, "to": date_to})
        return js.get("response", []) or []
    except Exception as e:
        log(f"[apifootball][WARN] fixtures_h2h_in_window({home_id},{away_id}) falhou: {e}")
        return []

def fixtures_by_season(team_id_: int, season: int) -> List[Dict]:
    try:
        js = rq("fixtures", {"team": team_id_, "season": season})
        return js.get("response", []) or []
    except Exception as e:
        log(f"[apifootball][WARN] fixtures_by_season(team={team_id_}, season={season}) falhou: {e}")
        return []

def pick_best_fixture(fixtures: List[Dict], home_name_expect: str, away_name_expect: str) -> Optional[Dict]:
    if not fixtures:
        return None
    scored = []
    now_ts = int(time.time())
    for fx in fixtures:
        try:
            th = (((fx or {}).get("teams") or {}).get("home") or {}).get("name") or ""
            ta = (((fx or {}).get("teams") or {}).get("away") or {}).get("name") or ""
            when = (((fx or {}).get("fixture") or {}).get("date")) or ""
            ts = 0
            if when:
                try:
                    ts = int(dt.datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp())
                except Exception:
                    ts = 0
            s_h = fuzz.WRatio(norm_ascii(home_name_expect), norm_ascii(th))
            s_a = fuzz.WRatio(norm_ascii(away_name_expect), norm_ascii(ta))
            scored.append((min(s_h, s_a), -abs(now_ts - ts), fx))
        except Exception:
            scored.append((0, 0, fx))
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return scored[0][2] if scored else None

def extract_1x2_odds(odds_payload: Dict) -> Optional[Tuple[float, float, float]]:
    resp = (odds_payload or {}).get("response") or []
    for item in resp:
        for bm in (item.get("bookmakers") or []):
            for bet in (bm.get("bets") or []):
                label = (bet.get("name") or bet.get("label") or "").strip().lower()
                if label in {"match winner", "1x2", "winner"}:
                    home = draw = away = None
                    for v in (bet.get("values") or []):
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
    try:
        js = rq("odds", {"fixture": fixture_id})
        got = extract_1x2_odds(js)
        if got:
            return got
    except Exception as e:
        log(f"[apifootball][DEBUG] odds fixture={fixture_id} falhou: {e}")
    # fallback leve
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

    aliases_map = load_aliases(args.aliases)

    # janela de datas (seleções)
    today = dt.date.today()
    lookahead = int(os.environ.get("LOOKAHEAD_DAYS", HARD_LOOKAHEAD_DAYS))
    date_from = today.isoformat()
    date_to = (today + dt.timedelta(days=lookahead)).isoformat()

    # lê whitelist
    rows = []
    with open(wl_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            log("::error::whitelist sem header")
            sys.exit(5)
        fields_low = [c.lower() for c in rdr.fieldnames if c]
        for need in ("match_id","home","away"):
            if need not in fields_low:
                log(f"::error::missing column '{need}' in matches_whitelist.csv")
                sys.exit(5)
        for r in rdr:
            mid = (r.get("match_id") or r.get("MATCH_ID") or "").strip()
            home = (r.get("home") or r.get("HOME") or "").strip()
            away = (r.get("away") or r.get("AWAY") or "").strip()
            if not mid or not home or not away:
                log(f"[apifootball][WARN] linha inválida (vazia) — ignorando")
                continue
            rows.append({"match_id": mid, "home": home, "away": away})

    if debug:
        log(f"[apifootball][DEBUG] whitelist linhas={len(rows)} — janela {date_from}..{date_to}")

    out_records: List[Dict] = []
    missing_any: List[str] = []

    for i, r in enumerate(rows, start=1):
        mid = r["match_id"]
        home_in = r["home"]
        away_in = r["away"]
        log(f"[apifootball] {i}: {home_in} x {away_in}")

        try:
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

            is_national = bool(hnat or anat or (hcountry in NATIONAL_COUNTRIES) or (acountry in NATIONAL_COUNTRIES))

            # Busca fixtures
            fixtures = []
            if is_national:
                fixtures = fixtures_h2h_in_window(hid, aid, date_from=date_from, date_to=date_to)
            else:
                fx_h = fixtures_by_season(hid, season)
                fx_a = fixtures_by_season(aid, season)
                def _is_h2h(fx):
                    try:
                        th = (((fx or {}).get("teams") or {}).get("home") or {}).get("id")
                        ta = (((fx or {}).get("teams") or {}).get("away") or {}).get("id")
                        return {th, ta} == {hid, aid}
                    except Exception:
                        return False
                fixtures = [fx for fx in (fx_h + fx_a) if _is_h2h(fx)]

            if not fixtures:
                log(f"[apifootball][WARN] Fixture não localizado para {hname or home_in} x {aname or away_in}")
                missing_any.append(mid)
                continue

            fx_best = pick_best_fixture(fixtures, hname or home_in, aname or away_in)
            if not fx_best:
                log(f"[apifootball][WARN] Fixture inadequado (sem matching) para {hname} x {aname}")
                missing_any.append(mid)
                continue

            fixture_id = ((((fx_best or {}).get("fixture") or {}).get("id")) or None)
            if not fixture_id:
                log(f"[apifootball][WARN] Fixture sem ID para {hname} x {aname}")
                missing_any.append(mid)
                continue

            odds = fetch_odds_for_fixture(int(fixture_id))
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

            time.sleep(0.15)  # anti rate-limit
        except SystemExit:
            raise
        except Exception as e:
            log(f"[apifootball][WARN] erro ao processar '{home_in} x {away_in}': {e}")
            missing_any.append(mid)

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
        # mantém exit 5 para forçar o step a falhar (APIs obrigatórias)
        sys.exit(5)

    # relatório final (mantém comportamento esperado pelo seu consenso estrito)
    if missing_any:
        log("::error::Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).")
        log(f"[apifootball][DEBUG] coletadas: {len(out_records)}  faltantes: {len(missing_any)} -> {missing_any}")
        # NÃO damos sys.exit aqui; o step do consenso decide o fail-fast.
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