#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestor de odds da API-FOOTBALL com suporte a:
 - Modo DIRETO (API-SPORTS) via x-apisports-key (recomendado)
 - Modo RapidAPI (retrocompatibilidade) via X-RapidAPI-Key

Saída: <rodada>/odds_apifootball.csv com colunas:
  match_id,home,away,odds_home,odds_draw,odds_away

Mapeia nomes usando:
 - data/aliases.json (se existir)  -> {"Ponte Preta": ["AA Ponte Preta", ...], ...}
 - fallback: busca /teams?search=

Uso:
  python -m scripts.ingest_odds_apifootball_rapidapi --rodada data/out/XXXX --season 2025 --aliases data/aliases.json

Requisitos:
  pip install requests pandas unidecode python-dateutil
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

# -------------------------
# CLI
# -------------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída desta rodada (ex: data/out/123456)")
    ap.add_argument("--season", required=True, help="Temporada (ex: 2025)")
    ap.add_argument("--aliases", default="data/aliases.json", help="Arquivo JSON de aliases (opcional)")
    ap.add_argument("--debug", action="store_true", help="Modo debug (mais logs)")
    return ap.parse_args()

# -------------------------
# Logging simples
# -------------------------
def log(level, msg):
    print(f"[apifootball][{level}] {msg}", flush=True)

# -------------------------
# HTTP client bimodal
# -------------------------
def build_session():
    api_key = (os.getenv("API_FOOTBALL_KEY") or "").strip()
    rapid = (os.getenv("RAPIDAPI_KEY") or os.getenv("X_RAPIDAPI_KEY") or "").strip()

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    if api_key:
        # API-FOOTBALL direto
        base = "https://v3.football.api-sports.io"
        sess.headers["x-apisports-key"] = api_key
        mode = "direct"
    elif rapid:
        # RapidAPI (retro)
        base = "https://api-football-v1.p.rapidapi.com/v3"
        sess.headers["X-RapidAPI-Key"] = rapid
        sess.headers["X-RapidAPI-Host"] = "api-football-v1.p.rapidapi.com"
        mode = "rapidapi"
    else:
        log("ERROR", "Nenhuma chave encontrada: defina API_FOOTBALL_KEY (direta) ou RAPIDAPI_KEY/X_RAPIDAPI_KEY.")
        sys.exit(5)

    return sess, base, mode

def api_get(sess, base, path, params=None, retries=3, backoff=2):
    params = params or {}
    url = f"{base}{path}"
    for i in range(retries):
        r = sess.get(url, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(backoff * (i + 1))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()  # se chegou aqui, vai exceção
    return None

# -------------------------
# Utils
# -------------------------
def normalize(s):
    return unidecode(str(s or "")).strip()

def load_aliases(path):
    if not path or not os.path.isfile(path):
        log("INFO", f"aliases.json não encontrado em {path} — seguindo sem aliases.")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # normaliza chaves e valores
        norm = {}
        for k, v in data.items():
            kk = normalize(k)
            vv = {normalize(k)} | {normalize(x) for x in (v if isinstance(v, list) else [])}
            norm[kk] = sorted(vv)
        log("INFO", f"aliases carregados: {len(norm)} times")
        return norm
    except Exception as e:
        log("WARN", f"Falha lendo aliases.json: {e} — seguindo sem aliases.")
        return {}

def alias_candidates(name, aliases_dict):
    base = normalize(name)
    cands = [base]
    # se houver grupo que contenha este nome, adiciona todos do grupo como candidatos
    for k, group in aliases_dict.items():
        if base == k or base in group:
            cands.extend(list(group))
    # dedup preservando ordem
    seen, out = set(), []
    for c in cands:
        if c not in seen:
            out.append(c); seen.add(c)
    return out

# -------------------------
# Busca de teams e fixture
# -------------------------
def find_team_id(sess, base, name, season, aliases):
    """
    Tenta localizar o team_id usando:
     1) aliases (variações) -> /teams?search=
     2) nome direto -> /teams?search=
    Retorna (team_id, team_name_oficial) ou (None, None)
    """
    for cand in alias_candidates(name, aliases):
        try:
            data = api_get(sess, base, "/teams", params={"search": cand})
        except Exception as e:
            log("WARN", f"Falha /teams?search={cand}: {e}")
            continue

        resp = (data or {}).get("response") or []
        if not resp:
            continue

        # heurística simples: prioriza matches exatos após normalizar
        cand_norm = normalize(cand).lower()
        scored = []
        for it in resp:
            t = it.get("team") or {}
            tname = normalize(t.get("name"))
            # score: 2 se match exato, 1 se inclui
            score = 2 if tname.lower() == cand_norm else (1 if cand_norm in tname.lower() else 0)
            scored.append((score, t.get("id"), tname))
        scored.sort(reverse=True)

        if scored and scored[0][0] > 0:
            tid, tname = scored[0][1], scored[0][2]
            return tid, tname

    return None, None

def pick_fixture_for_pair(sess, base, home_tid, away_tid, lookahead_days):
    """
    Tenta escolher um fixture futuro entre home_tid x away_tid.
    Estratégia:
      1) /fixtures/headtohead?h2h=home-away&from=YYYY-MM-DD&to=YYYY-MM-DD
      2) fallback: /fixtures?team=home&next=10 e filtra por away
    Retorna fixture_id ou None
    """
    today = datetime.now(timezone.utc).date()
    to_date = today + timedelta(days=int(float(os.getenv("LOOKAHEAD_DAYS", "3"))))
    from_str = today.isoformat()
    to_str = to_date.isoformat()

    # 1) head-to-head com janela
    try:
        h2h = f"{home_tid}-{away_tid}"
        data = api_get(sess, base, "/fixtures", params={"h2h": h2h, "from": from_str, "to": to_str})
        resp = (data or {}).get("response") or []
        if resp:
            # pega o mais próximo (menor data)
            resp.sort(key=lambda x: x.get("fixture", {}).get("timestamp", 0))
            return resp[0].get("fixture", {}).get("id")
    except Exception as e:
        log("WARN", f"Falha /fixtures?h2h={home_tid}-{away_tid}: {e}")

    # 2) fallback por time mandante
    try:
        data = api_get(sess, base, "/fixtures", params={"team": home_tid, "from": from_str, "to": to_str})
        resp = (data or {}).get("response") or []
        for fx in resp:
            t_home = fx.get("teams", {}).get("home", {}).get("id")
            t_away = fx.get("teams", {}).get("away", {}).get("id")
            if t_home == home_tid and t_away == away_tid:
                return fx.get("fixture", {}).get("id")
    except Exception as e:
        log("WARN", f"Falha /fixtures?team={home_tid}: {e}")

    return None

# -------------------------
# Odds parser
# -------------------------
def extract_1x2_from_odds(odds_json):
    """
    Tenta extrair mercado 1X2 do JSON de odds do v3:
      /odds?fixture=<id>
    Estrutura pode variar por bookmaker; procurar market de Winner/Match Winner/1X2.
    Retorna (home, draw, away) como floats (ou None).
    """
    resp = (odds_json or {}).get("response") or []
    best = None

    # percorre bookmakers/markets/outcomes e tenta identificar 1X2
    for item in resp:
        for bm in (item.get("bookmakers") or []):
            for mk in (bm.get("bets") or bm.get("markets") or []):  # alguns retornos usam bets, outros markets
                mname = (mk.get("name") or mk.get("market") or "").lower()
                if any(k in mname for k in ["match winner", "winner", "1x2", "full time result"]):
                    outcomes = mk.get("values") or mk.get("outcomes") or []
                    home = draw = away = None
                    for oc in outcomes:
                        on = (oc.get("value") or oc.get("name") or "").strip().lower()
                        odd = oc.get("odd") or oc.get("price") or oc.get("value_us")
                        try:
                            odd = float(str(odd).replace(",", "."))
                        except Exception:
                            odd = None
                        if on in ("home", "1", "home team"):
                            home = odd
                        elif on in ("draw", "x"):
                            draw = odd
                        elif on in ("away", "2", "away team"):
                            away = odd
                    if home and draw and away:
                        # escolhe a primeira ocorrência completa
                        return home, draw, away
                    # guarda melhor parcial caso não ache completo
                    if not best:
                        best = (home, draw, away)

    return best if best else (None, None, None)

# -------------------------
# Main
# -------------------------
def main():
    args = parse_args()
    rodada_dir = args.rodada
    season = str(args.season)
    aliases_path = args.aliases
    debug = args.debug

    os.makedirs(rodada_dir, exist_ok=True)

    # Carrega whitelist
    wl_path = os.path.join(rodada_dir, "matches_whitelist.csv")
    if not os.path.isfile(wl_path):
        log("ERROR", f"Whitelist não encontrada: {wl_path}")
        sys.exit(5)

    df_wl = pd.read_csv(wl_path, dtype=str).fillna("")
    df_wl.columns = [c.strip().lower() for c in df_wl.columns]
    for need in ["match_id", "home", "away"]:
        if need not in df_wl.columns:
            log("ERROR", f"missing column '{need}' in matches_whitelist.csv")
            sys.exit(5)

    aliases = load_aliases(aliases_path)

    sess, base, mode = build_session()
    if debug:
        log("INFO", f"MODO={mode} BASE={base}")

    rows = []
    missing = []

    for _, row in df_wl.iterrows():
        mid = str(row["match_id"]).strip()
        home_name = str(row["home"]).strip()
        away_name = str(row["away"]).strip()

        log("INFO", f"{mid}: {home_name} x {away_name}")

        # 1) localizar IDs de time
        h_id, h_off = find_team_id(sess, base, home_name, season, aliases)
        a_id, a_off = find_team_id(sess, base, away_name, season, aliases)

        if not h_id:
            log("WARN", f"Mandante não encontrado: {home_name}")
            missing.append(mid)
            continue
        if not a_id:
            log("WARN", f"Visitante não encontrado: {away_name}")
            missing.append(mid)
            continue

        # 2) localizar fixture
        fix_id = pick_fixture_for_pair(sess, base, h_id, a_id, os.getenv("LOOKAHEAD_DAYS", "3"))
        if not fix_id:
            log("WARN", f"Fixture não localizado para {home_name} x {away_name}")
            missing.append(mid)
            continue

        # 3) buscar odds
        try:
            odds_json = api_get(sess, base, "/odds", params={"fixture": fix_id})
        except Exception as e:
            log("WARN", f"Erro buscando odds do fixture {fix_id}: {e}")
            missing.append(mid)
            continue

        oh, od, oa = extract_1x2_from_odds(odds_json)
        if not (oh and od and oa):
            log("WARN", f"Odds não encontradas para fixture {fix_id} ({home_name} x {away_name})")
            missing.append(mid)
            continue

        rows.append({
            "match_id": mid,
            "home": home_name,
            "away": away_name,
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
        })

    # escrever CSV
    out_path = os.path.join(rodada_dir, "odds_apifootball.csv")
    if not rows:
        log("ERROR", "Nenhuma odd coletada da API-Football.")
        # ainda assim escreve cabeçalho vazio para facilitar debug
        pd.DataFrame(columns=["match_id","home","away","odds_home","odds_draw","odds_away"]).to_csv(out_path, index=False)
        # erro se houve faltantes
        sys.exit(5)

    df_out = pd.DataFrame(rows, columns=["match_id","home","away","odds_home","odds_draw","odds_away"])
    df_out.to_csv(out_path, index=False)

    if missing:
        log("ERROR", "Alguns jogos da whitelist ficaram sem odds da API-Football (APIs obrigatórias).")
        for mid in missing:
            r = df_wl[df_wl["match_id"] == mid].iloc[0]
            log("INFO", f"{mid}: {r['home']} x {r['away']}")
        log("DEBUG", f"[DEBUG] coletadas: {len(rows)}  faltantes: {len(missing)} -> {missing}")
        sys.exit(5)

    log("INFO", f"Odds coletadas: {len(rows)} -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())