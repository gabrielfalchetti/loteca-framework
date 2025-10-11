# scripts/ingest_odds_apifootball_rapidapi.py
# -*- coding: utf-8 -*-

"""
Ingestão de odds via API-Football (RapidAPI) para os jogos presentes em
OUT_DIR/matches_whitelist.csv, com aliases e normalização de nomes.

Saída obrigatória:
  {OUT_DIR}/odds_apifootball.csv com colunas:
  match_id,home,away,odds_home,odds_draw,odds_away

Variáveis de ambiente esperadas:
  - OUT_DIR (definida no workflow)
  - SEASON  (ex.: 2025)
  - LOOKAHEAD_DAYS (ex.: 3..7)
  - X_RAPIDAPI_KEY  (secreto)
  - DEBUG_FLAG (opcional, ex.: "--debug")
"""

from __future__ import annotations
import os
import sys
import csv
import json
import time
import math
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import urllib.request
import urllib.parse

# --------------------------------------------------------------------------------------
# Utils
# --------------------------------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)

def is_debug() -> bool:
    return "--debug" in sys.argv or os.environ.get("DEBUG", "").lower() in {"1", "true", "yes"}

def deacc(s: str) -> str:
    if not isinstance(s, str):
        return s
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def normalize_team(s: str) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("’", "'").replace("‘", "'").replace("`", "'")
    # remove UF brasileira "/SP", "/PR" etc.
    s = re.sub(r"/[A-Za-z]{2}($|[^A-Za-z])", " ", s)
    s = deacc(s)
    s = s.lower().strip()
    return s

def apply_alias(name: str, aliases_map: Dict[str, List[str]]) -> str:
    """
    Recebe um nome (ex.: 'Ponte Preta/SP') e aplica o alias para o nome canônico
    (chave em aliases_map). A comparação é feita por normalização.
    """
    norm = normalize_team(name)
    for canon, alist in aliases_map.items():
        # o canônico também conta como match
        candidates = [canon] + list(alist or [])
        for cand in candidates:
            if normalize_team(cand) == norm:
                return canon
    # se não achou, devolve o original "limpo"
    return name.strip()

def read_json_file(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        log(f"[WARN] Falha ao ler JSON {path}: {e}")
        return {}

def ensure_csv_header(path: str, header: List[str]) -> None:
    must_write = not os.path.exists(path)
    if not must_write:
        with open(path, "r", encoding="utf-8") as f:
            first = f.readline().strip().replace("\r", "")
            if first.lower() != ",".join([h.lower() for h in header]):
                must_write = True
    if must_write:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)

# --------------------------------------------------------------------------------------
# HTTP (RapidAPI)
# --------------------------------------------------------------------------------------

API_BASE = "https://api-football-v1.p.rapidapi.com/v3"

def rq(endpoint: str, params: Dict[str, str]) -> dict:
    """
    Chamada simples à API-Football via RapidAPI. Retorna JSON (dict).
    Lança exceção em erro HTTP >= 400.
    """
    key = os.environ.get("X_RAPIDAPI_KEY", "")
    if not key:
        raise RuntimeError("X_RAPIDAPI_KEY vazio")

    url = f"{API_BASE}/{endpoint}?" + urllib.parse.urlencode(params or {})
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
        "Accept": "application/json",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    if is_debug():
        log(f"[apifootball][DEBUG] GET {endpoint} {params}")

    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        data = resp.read()
        js = json.loads(data.decode("utf-8"))
        return js

# --------------------------------------------------------------------------------------
# API helpers
# --------------------------------------------------------------------------------------

def search_team_by_name(name: str) -> Optional[Tuple[int, str]]:
    """
    /teams?search= - retorna (team_id, nome_oficial) do melhor match.
    """
    try:
        js = rq("teams", {"search": name})
    except Exception as e:
        log(f"[apifootball][DEBUG] teams search '{name}' falhou: {e}")
        return None

    resp = (js or {}).get("response") or []
    if not resp:
        return None

    # primeiro que bater normalização
    norm_target = normalize_team(name)
    best: Optional[Tuple[int, str]] = None
    for item in resp:
        tm = (item or {}).get("team") or {}
        tname = (tm.get("name") or "").strip()
        tid = tm.get("id")
        if not tid or not tname:
            continue
        if normalize_team(tname) == norm_target:
            return (tid, tname)
        # fallback: guarda o primeiro válido (caso não haja igualdade exata normalizada)
        if best is None:
            best = (tid, tname)
    return best

def list_upcoming_fixtures_for_team(team_id: int, season: int, from_dt: datetime, to_dt: datetime) -> List[dict]:
    """
    Busca fixtures futuros para um time e filtra por janela de datas.
    API tipicamente permite "next" ou "from/to". Aqui preferimos varrer "next" e filtrar.
    """
    out: List[dict] = []
    try:
        js = rq("fixtures", {"season": season, "team": team_id, "next": 50})
        resp = (js or {}).get("response") or []
        for fx in resp:
            dt_iso = (((fx or {}).get("fixture") or {}).get("date") or "").strip()
            try:
                when = datetime.fromisoformat(dt_iso.replace("Z", "+00:00"))
            except Exception:
                continue
            if from_dt <= when <= to_dt:
                out.append(fx)
    except Exception as e:
        log(f"[apifootball][DEBUG] fixtures(team={team_id}) falhou: {e}")
    return out

def find_fixture_by_teams(home_name: str, away_name: str, season: int, lookahead_days: int) -> Optional[dict]:
    """
    Tenta localizar o fixture do confronto (home x away) na janela de LOOKAHEAD_DAYS.
    Estratégia:
      1) busca o ID do mandante
      2) varre próximos jogos do mandante e procura adversário
    """
    home = search_team_by_name(home_name)
    if not home:
        log(f"[apifootball][WARN] Mandante não encontrado: {home_name}")
        return None

    away = search_team_by_name(away_name)
    if not away:
        log(f"[apifootball][WARN] Visitante não encontrado: {away_name}")
        # não retorna ainda; vamos tentar pelo nome no fixture

    home_id, home_official = home
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=lookahead_days)
    fixtures = list_upcoming_fixtures_for_team(home_id, season, now - timedelta(days=1), end)

    norm_away = normalize_team(away_name)
    for fx in fixtures:
        t_home = ((((fx or {}).get("teams") or {}).get("home") or {}).get("name") or "").strip()
        t_away = ((((fx or {}).get("teams") or {}).get("away") or {}).get("name") or "").strip()
        if normalize_team(t_home) == normalize_team(home_official) and normalize_team(t_away) == norm_away:
            return fx

    log(f"[apifootball][WARN] Fixture não localizado para {home_name} x {away_name}")
    return None

def extract_1x2_odds(js: dict) -> Optional[Tuple[float, float, float]]:
    """
    Tenta extrair odds 1X2 do payload de odds (ou estrutura similar).
    """
    resp = (js or {}).get("response") or []
    # API-Football pode vir uma lista por bookmaker -> bets -> values
    for node in resp:
        bookmaker = (node or {}).get("bookmaker") or {}
        bets = (node or {}).get("bets") or []
        # varre apostas até achar "Match Winner" ou equivalente com 3 outcomes
        for b in bets:
            label = (b or {}).get("name") or ""
            values = (b or {}).get("values") or []
            # Heurística: se houver três outcomes com nomes comuns
            if len(values) >= 3:
                # tenta mapear
                h = d = a = None
                for v in values:
                    nm = (v or {}).get("value") or ""
                    odd = (v or {}).get("odd") or None
                    if odd is None:
                        continue
                    try:
                        oddf = float(str(odd).replace(",", "."))
                    except Exception:
                        continue
                    nml = nm.lower()
                    if "home" in nml or nml in {"1","casa"}:
                        h = oddf
                    elif "draw" in nml or "empate" in nml or nml in {"x"}:
                        d = oddf
                    elif "away" in nml or nml in {"2","fora"}:
                        a = oddf
                if h and d and a:
                    return (h, d, a)
    return None

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# NOVO: busca de odds com 3 fallbacks (odds, odds/live e odds embutidas em fixtures)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

def fetch_odds_for_fixture(fixture_id: int) -> Optional[Tuple[float, float, float]]:
    """
    Busca odds principais (1X2). Fallbacks:
      1. /odds
      2. /odds/live
      3. /fixtures (odds embutidas — visto em seleções/amistosos)
    """
    try:
        js = rq("odds", {"fixture": fixture_id})
        got = extract_1x2_odds(js)
        if got:
            return got
    except Exception as e:
        log(f"[apifootball][DEBUG] odds fixture={fixture_id} falhou: {e}")

    # fallback live odds
    try:
        js2 = rq("odds/live", {"fixture": fixture_id})
        got2 = extract_1x2_odds(js2)
        if got2:
            return got2
    except Exception:
        pass

    # fallback 3: odds embutidas no fixture (alguns payloads contêm odds resumidas)
    try:
        fx = rq("fixtures", {"id": fixture_id})
        resp = (fx or {}).get("response") or []
        if resp:
            fx0 = resp[0] or {}
            odds_obj = fx0.get("odds") or {}
            if odds_obj:
                # Convencional: {'home': 1.95, 'draw': 3.20, 'away': 3.80}
                home = float(odds_obj.get("home") or 0)
                draw = float(odds_obj.get("draw") or 0)
                away = float(odds_obj.get("away") or 0)
                if home and draw and away:
                    return (home, draw, away)
    except Exception:
        pass

    return None

# --------------------------------------------------------------------------------------
# Carregamento de whitelist e aliases
# --------------------------------------------------------------------------------------

def load_whitelist(path: str) -> List[Dict[str, str]]:
    """
    Espera CSV com colunas: match_id,home,away
    """
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        need = ["match_id", "home", "away"]
        low = [c.lower() for c in r.fieldnames or []]
        for c in need:
            if c not in low:
                raise RuntimeError(f"Coluna obrigatória ausente em whitelist: {c}")
        # mapping
        idx = {c.lower(): c for c in r.fieldnames}
        for row in r:
            rows.append({
                "match_id": (row[idx["match_id"]] or "").strip(),
                "home": (row[idx["home"]] or "").strip(),
                "away": (row[idx["away"]] or "").strip(),
            })
    return rows

def load_aliases(path: str) -> Dict[str, List[str]]:
    """
    Lê data/aliases.json e devolve dict {canônico: [aliases...]}
    Estrutura esperada:
    { "teams": { "Ponte Preta": ["Ponte Preta/SP", ...], ... } }
    """
    data = read_json_file(path)
    teams = data.get("teams") or {}
    # normaliza para garantir lista
    fixed: Dict[str, List[str]] = {}
    for canon, arr in teams.items():
        if isinstance(arr, list):
            fixed[canon] = arr
        elif isinstance(arr, str):
            fixed[canon] = [arr]
        else:
            fixed[canon] = []
    return fixed

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------

def main() -> int:
    out_dir = os.environ.get("OUT_DIR", "").strip()
    season = int(os.environ.get("SEASON", "2025"))
    lookahead = int(os.environ.get("LOOKAHEAD_DAYS", "5") or "5")

    if not out_dir:
        log("::error::OUT_DIR vazio")
        return 5

    wl_path = os.path.join(out_dir, "matches_whitelist.csv")
    if not os.path.exists(wl_path):
        log(f"::error::Whitelist não encontrada: {wl_path}")
        return 5

    # Aliases em data/aliases.json  (ATENÇÃO: conforme combinado)
    aliases_path = "data/aliases.json"
    aliases = load_aliases(aliases_path)
    if not aliases:
        log("[apifootball][WARN] aliases.json não encontrado em data/aliases.json — prosseguindo sem aliases")

    wl = load_whitelist(wl_path)
    log(f"[apifootball] whitelist: {wl_path}  linhas={len(wl)}  mapeamento={{'match_id': 'match_id', 'home': 'home', 'away': 'away'}}")

    out_file = os.path.join(out_dir, "odds_apifootball.csv")
    ensure_csv_header(out_file, ["match_id", "home", "away", "odds_home", "odds_draw", "odds_away"])

    missing: List[str] = []
    produced = 0

    with open(out_file, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        for i, row in enumerate(wl, start=1):
            raw_home = row["home"]
            raw_away = row["away"]
            mid = row["match_id"]

            # Aplica aliases e normalização suave
            can_home = apply_alias(raw_home, aliases)
            can_away = apply_alias(raw_away, aliases)

            log(f"[apifootball] {i}: {can_home} x {can_away}")

            fx = find_fixture_by_teams(can_home, can_away, season, lookahead)
            if not fx:
                missing.append(str(i))
                continue

            fixture_id = (((fx or {}).get("fixture") or {}).get("id") or None)
            if not fixture_id:
                log(f"[apifootball][DEBUG] fixture sem ID para {can_home} x {can_away}")
                missing.append(str(i))
                continue

            odds = fetch_odds_for_fixture(int(fixture_id))
            if not odds:
                missing.append(str(i))
                continue

            oh, od, oa = odds
            # sanity (odds > 1.0)
            if min(oh, od, oa) <= 1.0:
                log(f"[apifootball][DEBUG] odds inválidas {odds} para fixture={fixture_id}")
                missing.append(str(i))
                continue

            w.writerow([mid, can_home, can_away, f"{oh:.3f}", f"{od:.3f}", f"{oa:.3f}"])
            produced += 1

    if produced == 0:
        log("::error::Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).")
        for i, row in enumerate(wl, start=1):
            log(f"[apifootball] {i}: {row['home']} x {row['away']}")
        log(f"[apifootball][DEBUG] coletadas: {produced}  faltantes: {len(wl)} -> { [str(i) for i in range(1, len(wl)+1)] }")
        return 5

    if missing:
        log("::error::Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).")
        log(f"[apifootball][DEBUG] coletadas: {produced}  faltantes: {len(missing)} -> {missing}")
        return 5

    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"::error::Falha inesperada ingest_apifootball: {e}")
        sys.exit(5)