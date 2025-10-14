#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
af_resolve_ids.py
- Resolve team_id (API-Football) a partir de nomes livres (pt/en)
- Opcionalmente encontra o próximo fixture entre os dois times (fixture_id + kickoff)
- Saída: matches_norm.csv com chaves canônicas para casar odds e enriquecer features

Uso:
  python -m scripts.af_resolve_ids \
    --source data/in/matches_source.csv \
    --out data/out/XXXX/matches_norm.csv \
    --horizon_days 14

Requer:
  env API_FOOTBALL_KEY
"""

import os, sys, csv, time, argparse, unicodedata, re
from typing import Optional, Dict, Any, List
import requests

BASE = "https://v3.football.api-sports.io"

def _env(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        print(f"[af] ERRO: variável {key} não configurada.", file=sys.stderr)
        sys.exit(3)
    return val

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def norm_token(s: str) -> str:
    s = s.replace("/", " ")
    s = re.sub(r"\((?:PA|PR|SP|RJ|MG|RS|SC|GO)\)", " ", s, flags=re.I)  # remove UF entre parênteses
    s = s.replace("-", " ").replace(".", " ").replace("’", "'")
    s = strip_accents(s).lower()
    s = re.sub(r"\bfc\b|\bafc\b|\bec\b|\besporte clube\b|\bsc\b|\bclube\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Apelidos/variações BR -> forma de busca preferida
BR_EQUIV = {
    "athletico pr": ["athletico paranaense","athletico-pr","athletico","cap"],
    "atletico go":  ["atletico-go","atletico goianiense","acg"],
    "botafogo sp":  ["botafogo-sp","botafogo ribeirao","botafogo ribeirao preto"],
    "ferroviaria":  ["ferroviaria araraquara","afe","ferrroviaria"],
    "paysandu":     ["paysandu sport club","paysandu pa"],
    "remo":         ["remo pa","clube do remo"],
    "avai":         ["avai sc"],
    "volta redonda":["vrfc","volta redonda rj"],
    "chapecoense":  ["chapecoense sc","chapecoense af"],
}

def alt_queries(name: str) -> List[str]:
    base = [name]
    n = norm_token(name)
    base.append(n)
    for k, alts in BR_EQUIV.items():
        if n == k or n in alts:
            base.extend(alts)
    # Heurísticas simples
    if "atletico" in n and "go" in n and "goianiense" not in " ".join(base):
        base.append("atletico goianiense")
    if "athletico" in n and "paranaense" not in " ".join(base):
        base.append("athletico paranaense")
    if "botafogo sp" in n and "ribeirao" not in " ".join(base):
        base.append("botafogo ribeirao")
        base.append("botafogo ribeirao preto")
    if "ferroviaria" in n and "araraquara" not in " ".join(base):
        base.append("ferroviaria araraquara")
    if "avai" == n:
        base.append("avai sc")
    if "remo" == n:
        base.append("clube do remo")
        base.append("remo pa")
    if "paysandu" == n:
        base.append("paysandu sport club")
        base.append("paysandu pa")
    if "volta redonda" == n:
        base.append("vrfc")
    if "chapecoense" == n:
        base.append("chapecoense sc")
    # Sem duplicatas
    out, seen = [], set()
    for q in base:
        qn = q.strip()
        if qn and qn not in seen:
            out.append(qn)
            seen.add(qn)
    return out[:6]  # limita requests

class APISports:
    def __init__(self, key: str, timeout: int = 25):
        self.s = requests.Session()
        self.s.headers.update({"x-apisports-key": key})
        self.timeout = timeout

    def _get(self, path: str, **params) -> Dict[str, Any]:
        url = f"{BASE}/{path.lstrip('/')}"
        for attempt in range(3):
            r = self.s.get(url, params=params, timeout=self.timeout)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "2"))
                time.sleep(wait)
                continue
            r.raise_for_status()
            js = r.json()
            if js.get("errors"):
                # API-Football gosta de responder 200 com 'errors'
                # Não aborta aqui — devolve pro chamador decidir
                return js
            return js
        r.raise_for_status()
        return r.json()

    def search_team(self, name: str, country_hint: Optional[str] = None) -> Optional[Dict[str, Any]]:
        # 1) tentar search direto (com variações)
        for q in alt_queries(name):
            js = self._get("teams", search=q)
            data = js.get("response", [])
            if not data:
                continue
            # preferir Brasil quando for BR
            ranked = []
            for item in data:
                team = item.get("team") or {}
                venue = item.get("venue") or {}
                cn = (item.get("country") or {}).get("name") if isinstance(item.get("country"), dict) else None
                # API-Football v3 retorna country dentro de team? (varia por recurso)
                tname = (team.get("name") or "").strip()
                nat = (team.get("national", False) or False)
                code_country = item.get("country") or None  # fallback
                # país (heurística)
                country = None
                if isinstance(item.get("team"), dict) and "country" in item["team"]:
                    country = item["team"]["country"]
                if not country and cn:
                    country = cn
                if not country and venue.get("city"):
                    if venue["city"].upper().endswith(" BR"):
                        country = "Brazil"
                score = 0
                if country_hint:
                    score += 5 if (country or "").lower() == country_hint.lower() else 0
                # aproximação textual
                nt = norm_token(tname)
                score += 4 if nt == norm_token(name) else 0
                if norm_token(name) in nt or nt in norm_token(name):
                    score += 2
                if nat:  # evite seleções
                    score -= 3
                ranked.append((score, item))
            if ranked:
                ranked.sort(key=lambda x: x[0], reverse=True)
                best = ranked[0][1]
                return best.get("team")
        return None

    def next_fixtures_for_team(self, team_id: int, horizon: int = 14) -> List[Dict[str, Any]]:
        js = self._get("fixtures", team=team_id, next=20)
        resp = js.get("response", []) or []
        # Não há filtro de data exata; vamos aceitar os próximos e filtrar por horizonte no caller
        return resp

def pick_fixture_between(fixtures: List[Dict[str, Any]], home_id: int, away_id: int) -> Optional[Dict[str, Any]]:
    for f in fixtures:
        teams = f.get("teams", {})
        th = (teams.get("home") or {}).get("id")
        ta = (teams.get("away") or {}).get("id")
        if th == home_id and ta == away_id:
            return f
    # Tenta invertido (só por segurança, mas esperamos home/away corretos)
    for f in fixtures:
        teams = f.get("teams", {})
        th = (teams.get("home") or {}).get("id")
        ta = (teams.get("away") or {}).get("id")
        if th == away_id and ta == home_id:
            return f
    return None

def run(source: str, out_csv: str, horizon_days: int):
    key = _env("API_FOOTBALL_KEY")
    api = APISports(key)

    if not os.path.isfile(source) or os.path.getsize(source) == 0:
        print(f"[af] ERRO: arquivo de origem não encontrado ou vazio: {source}", file=sys.stderr)
        sys.exit(4)

    rows = []
    with open(source, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for req in ("match_id","home","away"):
            if req not in rd.fieldnames:
                print(f"[af] ERRO: coluna ausente em {source}: {req}", file=sys.stderr)
                sys.exit(4)
        for r in rd:
            rows.append(r)

    out_rows = []
    for r in rows:
        mid = r["match_id"]
        home_name = r["home"].strip()
        away_name = r["away"].strip()

        # país sugerido para BR (ajuda muito)
        hint = "Brazil"

        th = api.search_team(home_name, country_hint=hint)
        ta = api.search_team(away_name, country_hint=hint)

        if not th or not ta:
            print(f"[af][WARN] team_id ausente: {home_name} vs {away_name}", file=sys.stderr)
            out_rows.append({
                "match_id": mid, "home": home_name, "away": away_name,
                "home_id": th.get("id") if th else "",
                "away_id": ta.get("id") if ta else "",
                "fixture_id": "", "league_id": "", "season": "", "kickoff_utc": "", "venue": ""
            })
            continue

        hid = th["id"]; aid = ta["id"]

        # pegar próximos do mandante e tentar achar o confronto
        fx = api.next_fixtures_for_team(hid, horizon=horizon_days)
        cand = pick_fixture_between(fx, hid, aid)

        if not cand:
            # fallback: varre próximos do visitante
            fx2 = api.next_fixtures_for_team(aid, horizon=horizon_days)
            cand = pick_fixture_between(fx2, hid, aid)

        if cand:
            fix = cand.get("fixture") or {}
            league = cand.get("league") or {}
            venue = (fix.get("venue") or {}).get("name") or ""
            kickoff = fix.get("date") or ""
            out_rows.append({
                "match_id": mid,
                "home": home_name, "away": away_name,
                "home_id": hid, "away_id": aid,
                "fixture_id": fix.get("id") or "",
                "league_id": league.get("id") or "",
                "season": league.get("season") or "",
                "kickoff_utc": kickoff,
                "venue": venue
            })
        else:
            print(f"[af][WARN] fixture_id não encontrado: {home_name} vs {away_name}", file=sys.stderr)
            out_rows.append({
                "match_id": mid,
                "home": home_name, "away": away_name,
                "home_id": hid, "away_id": aid,
                "fixture_id": "", "league_id": "", "season": "", "kickoff_utc": "", "venue": ""
            })

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["match_id","home","away","home_id","away_id","fixture_id","league_id","season","kickoff_utc","venue"])
        for r in out_rows:
            wr.writerow([r["match_id"], r["home"], r["away"], r["home_id"], r["away_id"], r["fixture_id"], r["league_id"], r["season"], r["kickoff_utc"], r["venue"]])

    print(f"[af] OK — gravado {len(out_rows)} em {out_csv}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, help="CSV com match_id,home,away")
    ap.add_argument("--out", required=True, help="CSV normalizado de saída")
    ap.add_argument("--horizon_days", type=int, default=14, help="janela para buscar próximos jogos (default=14)")
    args = ap.parse_args()
    run(args.source, args.out, args.horizon_days)

if __name__ == "__main__":
    main()