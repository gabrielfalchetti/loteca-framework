#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor resiliente de odds via API-Football (RapidAPI).

- Aceita --leagues "71,72,73" (opcional). Se não vier, tenta ler de APIFOOT_LEAGUE_IDS (env).
- Lê X_RAPIDAPI_KEY do ambiente.
- Consulta fixtures +/- janela de dias (--window, default 2) e extrai mercado 1X2.
- Faz matching com data/in/matches_source.csv (layout simples) e escreve:
    <OUT_DIR>/odds_apifootball.csv
    <OUT_DIR>/unmatched_apifootball.csv
- Em caso de 403/401, sai com código 2 (para o workflow tratar com aviso/catch).
"""

import os
import sys
import csv
import json
import time
import argparse
from typing import List, Dict, Any, Tuple
import requests
import pandas as pd

API_BASE = "https://api-football-v1.p.rapidapi.com/v3"

def log(msg: str):
    print(f"[apifootball] {msg}")

def load_aliases(path: str | None) -> Dict[str, str]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # formato esperado: {"alias":"canonico", ...}
        return {k.lower(): v for k, v in data.items()}
    except Exception:
        return {}

def norm_team(s: str, aliases: Dict[str,str]) -> str:
    x = (s or "").strip().lower()
    return aliases.get(x, x)

def make_key(home: str, away: str) -> str:
    return f"{home.lower()}__vs__{away.lower()}"

def read_matches_source() -> pd.DataFrame:
    # layout simplificado: data/in/matches_source.csv
    p = os.path.join("data", "in", "matches_source.csv")
    if not os.path.isfile(p):
        # fallback: primeiro diretório em data/in/*/matches_source.csv
        din = os.path.join("data", "in")
        if os.path.isdir(din):
            for name in sorted(os.listdir(din)):
                cand = os.path.join(din, name, "matches_source.csv")
                if os.path.isfile(cand):
                    p = cand
                    break
    if not os.path.isfile(p):
        raise FileNotFoundError(f"matches_source.csv não encontrado em data/in/. Caminho tentado: {p}")

    df = pd.read_csv(p)
    need = {"home","away"}
    missing = need - set(c.lower() for c in df.columns)
    if missing:
        raise ValueError(f"colunas ausentes em matches_source.csv: {sorted(list(missing))}")
    # normaliza nomes das colunas
    df.columns = [c.lower() for c in df.columns]
    return df

def rapid_get(endpoint: str, params: Dict[str, Any], api_key: str, debug: bool=False) -> Dict[str, Any]:
    url = f"{API_BASE}/{endpoint}"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com",
    }
    if debug:
        log(f"GET {url} {params}")
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if debug:
        log(f"HTTP {r.status_code}")
    if r.status_code in (401,403):
        # erro de permissão/assinatura — deixa o workflow tratar
        print(f"[apifootball] HTTP {r.status_code} em {url} params={params} body={r.text}")
        r.raise_for_status()
    r.raise_for_status()
    return r.json()

def collect_fixtures_by_league(date_str: str, season: int, league_id: int, api_key: str, debug: bool=False) -> List[Dict[str,Any]]:
    # Fixtures por data/temporada/league
    params = {"date": date_str, "season": season, "league": league_id}
    data = rapid_get("fixtures", params=params, api_key=api_key, debug=debug)
    return data.get("response", []) or []

def collect_bookmakers(api_key: str, debug: bool=False) -> Dict[int, str]:
    try:
        data = rapid_get("odds/bookmakers", params={}, api_key=api_key, debug=debug)
        out = {}
        for item in (data.get("response") or []):
            out[int(item.get("id"))] = item.get("name","")
        return out
    except Exception:
        return {}

def collect_odds_fixture(fixture_id: int, api_key: str, debug: bool=False) -> List[Dict[str, Any]]:
    # Odds por fixture (mercado 1x2)
    params = {"fixture": fixture_id, "type": "prematch"}
    data = rapid_get("odds", params=params, api_key=api_key, debug=debug)
    return data.get("response", []) or []

def extract_h2h(odds_resp: List[Dict[str,Any]]) -> Tuple[float|None,float|None,float|None]:
    # percorre odds -> bookmakers -> bets -> values
    # Queremos mercado "Match Winner" (1X2) ou equivalente (bet id 1 costuma ser 1x2 em alguns planos)
    for item in odds_resp:
        for bk in (item.get("bookmakers") or []):
            for bet in (bk.get("bets") or []):
                name = (bet.get("name") or "").lower()
                if "match winner" in name or "1x2" in name or "winner" in name:
                    vals = bet.get("values") or []
                    home = draw = away = None
                    for v in vals:
                        label = (v.get("value") or "").strip().lower()
                        odd = v.get("odd")
                        try:
                            odd_f = float(odd)
                        except Exception:
                            odd_f = None
                        if label in ("home","1","1 (home)","local"):
                            home = odd_f
                        elif label in ("draw","x","empate"):
                            draw = odd_f
                        elif label in ("away","2","2 (away)","visitante","visitors"):
                            away = odd_f
                    if home and draw and away:
                        return home, draw, away
    return None, None, None

def within_window(date_str: str, days: int, offset: int) -> List[str]:
    # monta vetor de datas no formato YYYY-MM-DD, [hoje-offset ... hoje+offset] limitado por --window
    # aqui usamos a data de execução (UTC) — porém como via matches_source já temos os jogos,
    # vamos usar apenas a data corrente para fixtures do dia como fallback
    from datetime import datetime, timedelta, timezone
    base = datetime.now(timezone.utc).date()
    out = []
    for d in range(-days, days+1):
        out.append(str(base + timedelta(days=d+offset)))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="OUT_DIR (ex.: data/out/<RUN_ID>)")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--leagues", type=str, default=os.environ.get("APIFOOT_LEAGUE_IDS","").strip(),
                    help="IDs de ligas separados por vírgula (ex: '71,72'). Se vazio, usa env APIFOOT_LEAGUE_IDS.")
    ap.add_argument("--window", type=int, default=2, help="Janela de dias para fixtures (fallback)")
    ap.add_argument("--fuzzy", type=float, default=0.90, help="(reservado) threshold para matching mais flexível")
    ap.add_argument("--aliases", type=str, default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    api_key = os.environ.get("X_RAPIDAPI_KEY","").strip()
    if not api_key:
        print("[apifootball] ERRO: X_RAPIDAPI_KEY não definido no ambiente.")
        sys.exit(2)

    # leagues
    leagues_raw = (args.leagues or "").strip()
    league_ids: List[int] = []
    if leagues_raw:
        try:
            league_ids = [int(x) for x in leagues_raw.split(",") if x.strip()]
        except Exception:
            print(f"[apifootball] AVISO: não foi possível interpretar --leagues '{leagues_raw}'.")
            league_ids = []
    if not league_ids:
        print("[apifootball] AVISO: nenhuma liga informada (APIFOOT_LEAGUE_IDS). Coleta pode retornar vazia.")

    # matches reais (para match exato quando possível)
    try:
        matches_df = read_matches_source()
    except Exception as e:
        print(f"[apifootball] AVISO: não foi possível ler matches_source.csv ({e}). Matching ficará mais fraco.")
        matches_df = pd.DataFrame(columns=["home","away"])

    aliases = load_aliases(args.aliases)

    # Coleta fixtures (por hoje +/- window), por liga
    from datetime import datetime, timezone
    dates = within_window(datetime.now(timezone.utc).strftime("%Y-%m-%d"), args.window, 0)
    fixtures_all: List[Dict[str,Any]] = []
    for lid in league_ids:
        for d in dates:
            try:
                fx = collect_fixtures_by_league(d, args.season, lid, api_key, debug=args.debug)
                fixtures_all.extend(fx)
                time.sleep(0.2)  # educado com a API
            except requests.HTTPError:
                raise
            except Exception as e:
                print(f"[apifootball] AVISO: falha coletando fixtures league={lid} date={d}: {e}")

    # Indexa fixtures -> odds
    rows: List[Dict[str,Any]] = []
    for fx in fixtures_all:
        try:
            fixture = fx.get("fixture",{})
            teams = fx.get("teams",{})
            fid = fixture.get("id")
            th = (teams.get("home",{}) or {}).get("name","")
            ta = (teams.get("away",{}) or {}).get("name","")
            th_n = norm_team(th, aliases)
            ta_n = norm_team(ta, aliases)

            odds_resp = collect_odds_fixture(fid, api_key, debug=args.debug)
            h,d,a = extract_h2h(odds_resp)
            if not (h and d and a):
                continue

            rows.append({
                "team_home": th,
                "team_away": ta,
                "match_key": make_key(th_n, ta_n),
                "odds_home": h,
                "odds_draw": d,
                "odds_away": a
            })
            time.sleep(0.2)
        except requests.HTTPError:
            raise
        except Exception as e:
            print(f"[apifootball] AVISO: falha processando fixture: {e}")

    odds_df = pd.DataFrame(rows, columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"])

    # Matching com matches_source (se disponível)
    unmatched = pd.DataFrame()
    if not matches_df.empty and not odds_df.empty:
        matches_df["home_n"] = matches_df["home"].apply(lambda s: norm_team(str(s), aliases))
        matches_df["away_n"] = matches_df["away"].apply(lambda s: norm_team(str(s), aliases))
        matches_df["match_key"] = matches_df.apply(lambda r: make_key(r["home_n"], r["away_n"]), axis=1)

        merged = matches_df.merge(odds_df, on="match_key", how="left", suffixes=("_src",""))
        unmatched = merged[merged["odds_home"].isna()][["home","away","match_key"]].copy()
        # Mantém só odds que deram match ou, se não houver matches_source, mantém todas
        if not merged.empty:
            odds_df = merged[~merged["odds_home"].isna()][["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]].copy()

    # Salva arquivos
    odds_p = os.path.join(out_dir, "odds_apifootball.csv")
    unmatched_p = os.path.join(out_dir, "unmatched_apifootball.csv")
    odds_df.to_csv(odds_p, index=False)
    if not unmatched.empty:
        unmatched.to_csv(unmatched_p, index=False)
    else:
        # cria vazio amigável
        pd.DataFrame(columns=["home","away","match_key"]).to_csv(unmatched_p, index=False)

    log(f"linhas -> {{\"odds_apifootball.csv\": {len(odds_df)}, \"unmatched_apifootball.csv\": {len(unmatched)}}}")
    print("9:Marcador requerido pelo workflow: \"apifootball-safe\"")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        # Propaga como exit 2 para o step com '|| { echo warning; }'
        sys.stderr.write(str(e) + "\n")
        sys.exit(2)