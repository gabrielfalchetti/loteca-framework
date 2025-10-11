# scripts/ingest_odds_apifootball_rapidapi.py
import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from unidecode import unidecode

API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"

def log(msg, level="INFO"):
    print(f"[apifootball][{level}] {msg}")

def load_aliases(path):
    if not path or not os.path.isfile(path):
        log(f"aliases.json não encontrado em {path} — prosseguindo sem aliases", "WARN")
        return {"teams": {}, "leagues": {}, "normalize_rules": []}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def norm_name(s: str) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = unidecode(s)  # remove acentos
    s = s.replace(".", " ").replace("-", " ").replace("/", " ")
    s = " ".join(s.split())
    return s

# fallback PT->EN para seleções e clubes que causaram erro nos logs
FALLBACK_MAP = {
    "Servia/Ser": "Serbia",
    "Servia": "Serbia",
    "Serbia/Ser": "Serbia",
    "Paises Baixos": "Netherlands",
    "Holanda": "Netherlands",
    "Irlanda": "Ireland",
    "Italia": "Italy",
    "Romenia": "Romania",
    "Polonia": "Poland",
    "Lituania": "Lithuania",
    "Grecia": "Greece",
    "Dinamarca": "Denmark",
    "Espanha": "Spain",
    "Turquia": "Turkey",
    "Georgia": "Georgia",
    "Estonia": "Estonia",
    # clubes BR:
    "Novorizontino": "Gremio Novorizontino",
    "Gremio Novorizontino": "Gremio Novorizontino",
    "Criciuma": "Criciuma",
    "America": "America Mineiro",
    "America/MG": "America Mineiro",
    "America MG": "America Mineiro",
    "América Mineiro": "America Mineiro",
    "Operario PR": "Operario PR",
    "Operario/PR": "Operario PR",
}

def apply_alias(name: str, aliases: dict) -> str:
    if not name:
        return name
    n = norm_name(name)
    # primeiro, lookup direto em "teams"
    teams = aliases.get("teams", {})
    if n in teams:
        return teams[n]
    # tenta com casing original
    if name in teams:
        return teams[name]
    # fallback map
    if n in FALLBACK_MAP:
        return FALLBACK_MAP[n]
    if name in FALLBACK_MAP:
        return FALLBACK_MAP[name]
    return n

def req(session, path, params, key):
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": API_HOST,
    }
    url = f"{BASE_URL}{path}"
    r = session.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def search_team_id(session, key, team_name):
    """Busca team_id por nome. Tenta seleções e clubes."""
    if not team_name:
        return None
    # 1) teams?search=
    try:
        data = req(session, "/teams", {"search": team_name}, key)
        for it in data.get("response", []):
            name = norm_name(it.get("team", {}).get("name", ""))
            if name.lower() == norm_name(team_name).lower():
                return it.get("team", {}).get("id")
        # se não bateu exatamente, retorna o primeiro similar
        if data.get("response"):
            return data["response"][0].get("team", {}).get("id")
    except Exception as e:
        log(f"Falha search_team_id(search) {team_name}: {e}", "WARN")

    # 2) tenta /teams?name=
    try:
        data = req(session, "/teams", {"name": team_name}, key)
        if data.get("response"):
            return data["response"][0].get("team", {}).get("id")
    except Exception as e:
        log(f"Falha search_team_id(name) {team_name}: {e}", "WARN")
    return None

def find_fixture(session, key, team_home_id, team_away_id, date_from, date_to):
    """Tenta achar fixture pelo par de IDs dentro da janela."""
    params = {
        "from": date_from,
        "to": date_to,
        "team": team_home_id,
    }
    try:
        data = req(session, "/fixtures", params, key)
        candidates = []
        for fx in data.get("response", []):
            th = fx.get("teams", {}).get("home", {}).get("id")
            ta = fx.get("teams", {}).get("away", {}).get("id")
            if {th, ta} == {team_home_id, team_away_id}:
                candidates.append(fx)
        if candidates:
            # preferência para o que tem status "NS" (not started) e está mais próximo
            candidates.sort(key=lambda x: x.get("fixture", {}).get("date", ""))
            return candidates[0]
    except Exception as e:
        log(f"Falha find_fixture(team window): {e}", "WARN")

    # fallback: h2h
    try:
        h2h = f"{team_home_id}-{team_away_id}"
        data = req(session, "/fixtures/headtohead", {"h2h": h2h}, key)
        # filtra por janela
        for fx in data.get("response", []):
            dt = fx.get("fixture", {}).get("date")
            if dt:
                dt_utc = datetime.fromisoformat(dt.replace("Z", "+00:00"))
                if date_from <= dt_utc.strftime("%Y-%m-%d") <= date_to:
                    return fx
    except Exception as e:
        log(f"Falha find_fixture(h2h): {e}", "WARN")
    return None

def fetch_odds_for_fixture(session, key, fixture_id):
    """Busca odds para um fixture."""
    # endpoint de odds:
    # https://api-football-v1.p.rapidapi.com/v3/odds?fixture=XXXX
    try:
        data = req(session, "/odds", {"fixture": fixture_id}, key)
        # estrutura: response -> [ { bookmakers: [ { bets: [ { name: "Match Winner", values: [...] } ] } ] } ]
        for item in data.get("response", []):
            for bk in item.get("bookmakers", []):
                for bet in bk.get("bets", []):
                    if bet.get("name", "").lower() in ["match winner", "1x2", "winner"]:
                        # procurar 1 (home), X (draw), 2 (away)
                        oh = od = oa = None
                        for v in bet.get("values", []):
                            valname = v.get("value", "").strip().upper()
                            odd = v.get("odd")
                            if valname in ["HOME", "1", "1 (HOME)"]:
                                oh = odd
                            elif valname in ["DRAW", "X"]:
                                od = odd
                            elif valname in ["AWAY", "2", "2 (AWAY)"]:
                                oa = odd
                        if oh and od and oa:
                            return float(oh), float(od), float(oa)
        return None
    except Exception as e:
        log(f"Falha fetch_odds_for_fixture: {e}", "WARN")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório de saída desta rodada")
    parser.add_argument("--season", required=True, help="Temporada (ex.: 2025)")
    parser.add_argument("--aliases", required=False, default="", help="Caminho para data/aliases.json")
    parser.add_argument("--debug", dest="debug", action="store_true")
    args = parser.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    key = os.environ.get("X_RAPIDAPI_KEY", "")
    if not key:
        log("X_RAPIDAPI_KEY não definido", "ERROR")
        sys.exit(5)

    whitelist_path = os.path.join(out_dir, "matches_whitelist.csv")
    if not os.path.isfile(whitelist_path):
        log(f"Whitelist não encontrada: {whitelist_path}", "ERROR")
        sys.exit(5)

    aliases = load_aliases(args.aliases)
    # janela de datas (LOOKAHEAD_DAYS)
    lookahead = int(os.environ.get("LOOKAHEAD_DAYS", "3"))
    today = datetime.now(timezone.utc).date()
    date_from = today.strftime("%Y-%m-%d")
    date_to = (today + timedelta(days=lookahead)).strftime("%Y-%m-%d")

    # Lê whitelist
    rows = []
    with open(whitelist_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append({
                "match_id": str(r["match_id"]).strip(),
                "home": str(r["home"]).strip(),
                "away": str(r["away"]).strip(),
            })

    log(f"whitelist: {whitelist_path}  linhas={len(rows)}  mapeamento={{'match_id': 'match_id', 'home': 'home', 'away': 'away'}}")

    # Resolve odds
    out_rows = []
    missing = []
    with requests.Session() as session:
        for r in rows:
            mid = r["match_id"]
            raw_home, raw_away = r["home"], r["away"]

            # aplica aliases + normalização
            home = apply_alias(raw_home, aliases)
            away = apply_alias(raw_away, aliases)

            # log do par que estamos tentando
            log(f"{mid}: {home} x {away}")

            # pega team ids
            th_id = search_team_id(session, key, home)
            ta_id = search_team_id(session, key, away)

            if not th_id or not ta_id:
                if not th_id:
                    log(f"Mandante não encontrado: {home}", "WARN")
                if not ta_id:
                    log(f"Visitante não encontrado: {away}", "WARN")
                missing.append(mid)
                continue

            fx = find_fixture(session, key, th_id, ta_id, date_from, date_to)
            if not fx:
                log(f"Fixture não localizado para {home} x {away}", "WARN")
                missing.append(mid)
                continue

            fixture_id = fx.get("fixture", {}).get("id")
            odds = fetch_odds_for_fixture(session, key, fixture_id)
            if not odds:
                log(f"Odds não encontradas para fixture {fixture_id} ({home} x {away})", "WARN")
                missing.append(mid)
                continue

            oh, od, oa = odds
            out_rows.append({
                "match_id": mid,
                "home": home,
                "away": away,
                "odds_home": oh,
                "odds_draw": od,
                "odds_away": oa,
            })

    # escreve saída
    out_csv = os.path.join(out_dir, "odds_apifootball.csv")
    if out_rows:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=["match_id", "home", "away", "odds_home", "odds_draw", "odds_away"])
            wr.writeheader()
            wr.writerows(out_rows)
    else:
        # nada coletado
        pass

    if missing:
        log("Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).", "ERROR")
        for r in rows:
            mid = r["match_id"]
            if mid in missing:
                log(f"{mid}: {apply_alias(r['home'], aliases)} x {apply_alias(r['away'], aliases)}")
        log(f"[DEBUG] coletadas: {len(out_rows)}  faltantes: {len(missing)} -> {missing}", "DEBUG")
        # força falha para o modo estrito parar o pipeline
        sys.exit(5)

    if not os.path.isfile(out_csv) or os.path.getsize(out_csv) == 0:
        log("odds_apifootball.csv não gerado", "ERROR")
        sys.exit(5)

    log(f"Gerado: {out_csv}")

if __name__ == "__main__":
    main()