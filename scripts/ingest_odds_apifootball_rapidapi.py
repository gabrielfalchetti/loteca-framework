#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball_rapidapi.py — Framework Loteca v4.3.RC2

Coleta odds na API-Football (via RapidAPI) para jogos de uma rodada.
Inclui:
- Skip explícito para jogos já finalizados (ontem/antes) com log claro
- Filtro de status de fixtures elegíveis para odds
- Normalização com aliases + fuzzy match
- Janela temporal parametrizável
- Saída consolidada no formato 1x2 (Match Winner)

Uso:
  python scripts/ingest_odds_apifootball_rapidapi.py \
    --rodada 2025-09-27_1213 --season 2025 --window 14 --fuzzy 0.9 \
    --aliases data/aliases_br.json --debug
"""

from __future__ import annotations
import os
import sys
import json
import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import requests
from rapidfuzz import fuzz, process

# ------------- Config padrão ------------- #

API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"

# Status de fixtures que ainda podem ter odds úteis
ELIGIBLE_STATUSES = {
    "NS",  # Not Started
    "TBD", "PST", "SUSP",  # pendentes
    "1H", "HT", "2H", "ET", "BT"  # em jogo
}
# Status que consideramos finalizados
FINISHED_STATUSES = {"FT", "AET", "PEN"}

# Ligas padrão (BR + principais)
DEFAULT_LEAGUES = [
    # Brasil
    71, 72, 73, 128, 776,
    # Europa principais (útil p/ jogos cruzados na planilha)
    39,   # Premier League
    140,  # LaLiga
    135,  # Serie A ITA
    78,   # Bundesliga
    61,   # Ligue 1
    88,   # Primeira Liga (POR)
    94,   # Eredivisie
    179,  # Argentina Liga Profesional
    144,  # MLS
    203,  # Championship
]

# ------------- Utilitários ------------- #

def now_sao_paulo() -> datetime:
    # Runner usa UTC; convert para São Paulo se quiser logs locais (só estética)
    # Aqui manteremos UTC para comparações consistentes com timestamps ISO da API.
    return datetime.now(timezone.utc)

def parse_iso(dt_str: str) -> datetime:
    # API-Football retorna ISO com offset; pandas pode parsear, mas fazemos manual
    return pd.to_datetime(dt_str, utc=True).to_pydatetime()

def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def load_matches_csv(rodada: str) -> pd.DataFrame:
    src = os.path.join("data", "in", rodada, "matches_source.csv")
    if not os.path.isfile(src):
        raise RuntimeError(f"[apifootball] arquivo de entrada ausente: {src}")
    df = pd.read_csv(src)
    req_cols = {"home", "away"}
    if not req_cols.issubset(set(c.lower() for c in df.columns)):
        # tentar normalizar colunas
        cols = {c.lower(): c for c in df.columns}
        missing = req_cols - set(cols.keys())
        if missing:
            raise RuntimeError(f"[apifootball] CSV sem colunas mínimas {missing} -> {src}")
    # padroniza nomes de colunas (lower)
    df.columns = [c.lower() for c in df.columns]
    return df

def norm_name(s: str) -> str:
    # normalize: minúsculas, sem acentos, remove sufixos comuns e pontuação básica
    import unicodedata, re
    if not isinstance(s, str):
        return ""
    t = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    t = t.lower()
    # sufixos comuns
    t = re.sub(r"\b(ec|fc|sc|afc|cf|ac|esporte clube|clube de regatas)\b", "", t)
    t = t.replace("&", " and ")
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def load_aliases(path: Optional[str]) -> Dict[str, List[str]]:
    if not path:
        return {}
    if not os.path.isfile(path):
        print(f"[apifootball] Aviso: aliases não encontrado: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # normaliza chaves e valores
    ali: Dict[str, List[str]] = {}
    for k, vals in data.items():
        base = norm_name(k)
        lst = list({norm_name(v) for v in ([k] + list(vals))})
        ali[base] = sorted({v for v in lst if v})
    return ali

def expand_aliases(name_norm: str, aliases: Dict[str, List[str]]) -> List[str]:
    # retorna o conjunto de formas aceitáveis p/ matching
    forms = {name_norm}
    if name_norm in aliases:
        forms.update(aliases[name_norm])
    return sorted(forms)

def is_past_match(row: pd.Series) -> bool:
    # Se CSV contém 'date' (ISO ou yyyy-mm-dd hh:mm), decide se já passou
    # Caso não tenha, retorna False (não consegue inferir)
    if "date" not in row or pd.isna(row["date"]):
        return False
    try:
        dt = pd.to_datetime(row["date"], utc=True)
        return dt.to_pydatetime() < now_sao_paulo()
    except Exception:
        return False

# ------------- API helpers ------------- #

def api_headers(rapid_key: str) -> Dict[str, str]:
    return {
        "x-rapidapi-key": rapid_key,
        "x-rapidapi-host": API_HOST,
    }

def api_get(url: str, headers: Dict[str, str], params: Dict[str, Any], debug: bool=False) -> Dict[str, Any]:
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if debug:
        print(f"[apifootball][GET] {r.url}")
    r.raise_for_status()
    out = r.json()
    return out

def fetch_fixtures(leagues: List[int], season: int, date_from: str, date_to: str,
                   headers: Dict[str, str], debug: bool=False) -> List[Dict[str, Any]]:
    fixtures: List[Dict[str, Any]] = []
    for lg in leagues:
        try:
            data = api_get(
                f"{API_BASE}/fixtures",
                headers,
                params={"league": lg, "season": season, "from": date_from, "to": date_to},
                debug=debug
            )
            res = data.get("response", []) or []
            print(f"[apifootball] liga={lg}: fixtures={len(res)}")
            fixtures.extend(res)
        except requests.HTTPError as e:
            print(f"[apifootball] ERRO liga={lg}: {e}")
        except Exception as e:
            print(f"[apifootball] ERRO liga={lg}: {e}")
    return fixtures

def build_fixture_index(fixtures: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Index por chave (home_n || away_n || data) -> fixture info
    """
    idx: Dict[str, Dict[str, Any]] = {}
    for fx in fixtures:
        try:
            home = fx["teams"]["home"]["name"]
            away = fx["teams"]["away"]["name"]
            dt_iso = fx["fixture"]["date"]  # ISO
            status = fx["fixture"]["status"]["short"]
            fid = fx["fixture"]["id"]
        except Exception:
            continue
        hn, an = norm_name(home), norm_name(away)
        key = f"{hn}||{an}||{dt_iso[:10]}"  # usa data (yyyy-mm-dd) como desambiguador
        idx[key] = {
            "fid": fid,
            "home": home,
            "away": away,
            "home_n": hn,
            "away_n": an,
            "date": dt_iso,
            "status": status,
        }
    return idx

def fetch_odds_for_fixture(fid: int, headers: Dict[str, str], debug: bool=False) -> Optional[Dict[str, Any]]:
    """
    Busca odds para um fixture específico e retorna o melhor 'Match Winner' (1x2).
    Estrutura esperada:
    - response: [ { bookmaker:{name}, bets:[ {name or id, values:[{value, odd}]} ] } ]
    """
    try:
        data = api_get(f"{API_BASE}/odds", headers, params={"fixture": fid}, debug=debug)
        res = data.get("response", []) or []
        if not res:
            return None
        # procura primeiro bookmaker com aposta 'Match Winner' / '1X2'
        for book in res:
            bname = book.get("bookmaker", {}).get("name") or "api-football"
            for bet in book.get("bets", []) or []:
                name = (bet.get("name") or "").strip().lower()
                if name in {"match winner", "1x2", "1x2 - full time", "winner"}:
                    # values com 'Home', 'Draw', 'Away'
                    vals = bet.get("values", []) or []
                    out_map = {}
                    for v in vals:
                        lbl = (v.get("value") or "").strip().lower()
                        odd = v.get("odd")
                        if not odd:
                            continue
                        if lbl in {"home", "1", "home team"}:
                            out_map["home"] = float(str(odd).replace(",", "."))
                        elif lbl in {"draw", "x"}:
                            out_map["draw"] = float(str(odd).replace(",", "."))
                        elif lbl in {"away", "2", "away team"}:
                            out_map["away"] = float(str(odd).replace(",", "."))
                    if out_map:
                        return {"book": bname,
                                "odd_home": out_map.get("home"),
                                "odd_draw": out_map.get("draw"),
                                "odd_away": out_map.get("away")}
        return None
    except requests.HTTPError as e:
        if debug:
            print(f"[apifootball] ERRO odds fixture={fid}: {e}")
        return None
    except Exception as e:
        if debug:
            print(f"[apifootball] ERRO odds fixture={fid}: {e}")
        return None

# ------------- Core ------------- #

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Identificador da rodada, ex.: 2025-09-27_1213")
    parser.add_argument("--season", type=int, default=None, help="Temporada (ex.: 2025). Se ausente, tenta inferir do ano.")
    parser.add_argument("--window", type=int, default=14, help="Janela em dias para busca de fixtures [default=14]")
    parser.add_argument("--fuzzy", type=float, default=0.90, help="limiar de similaridade [0-1] p/ fuzzy match [default=0.90]")
    parser.add_argument("--aliases", type=str, default=None, help="Caminho para JSON de aliases (ex.: data/aliases_br.json)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    rodada = args.rodada
    out_dir = os.path.join("data", "out", rodada)
    safe_mkdir(out_dir)

    # Secrets / Headers
    rapid_key = os.environ.get("RAPIDAPI_KEY", "").strip()
    if not rapid_key:
        raise RuntimeError("ERRO: defina RAPIDAPI_KEY (Actions > Secrets).")
    headers = api_headers(rapid_key)

    # Carrega CSV de jogos da rodada
    df_matches = load_matches_csv(rodada)
    n_matches = len(df_matches)
    print(f"[apifootball] Jogos no CSV: {n_matches}")

    # Season
    if args.season is not None:
        season = args.season
    else:
        # heurística: pega ano da primeira data ou do 'rodada'
        season = now_sao_paulo().year
        if "date" in df_matches.columns and df_matches["date"].notna().any():
            try:
                season = int(pd.to_datetime(df_matches["date"].dropna().iloc[0]).year)
            except Exception:
                pass

    # Janela temporal
    today_utc = now_sao_paulo()
    dfrom = (today_utc - timedelta(days=max(1, args.window))).date().isoformat()
    dto   = (today_utc + timedelta(days=max(1, args.window))).date().isoformat()

    # Se CSV contém datas, ajusta janela para cobrir de fato as datas do CSV, com margem
    if "date" in df_matches.columns and df_matches["date"].notna().any():
        try:
            dmin = pd.to_datetime(df_matches["date"]).min().to_pydatetime().date()
            dmax = pd.to_datetime(df_matches["date"]).max().to_pydatetime().date()
            dfrom = min(dfrom, (dmin - timedelta(days=7)).isoformat())
            dto   = max(dto,   (dmax + timedelta(days=7)).isoformat())
        except Exception:
            pass

    print(f"[apifootball] Janela: {dfrom} -> {dto}; season={season}")

    # Ligas buscadas
    leagues = DEFAULT_LEAGUES[:]
    if "league" in df_matches.columns and df_matches["league"].notna().any():
        # se seu CSV já fornece ligas específicas, você pode optar por filtrá-las aqui
        pass

    # Coleta fixtures
    fixtures = fetch_fixtures(leagues, season, dfrom, dto, headers, debug=args.debug)
    print(f"[apifootball] Fixtures coletados: {len(fixtures)}")

    # Índice rápido (home_n, away_n, date) -> fixture
    fx_index = build_fixture_index(fixtures)

    # Aliases
    aliases = load_aliases(args.aliases)

    rows: List[Dict[str, Any]] = []
    miss = 0
    skipped_past = 0
    matched = 0

    for _, row in df_matches.iterrows():
        home = str(row.get("home", "")).strip()
        away = str(row.get("away", "")).strip()
        date_csv = row.get("date", None)

        # Normalizados
        home_n = norm_name(home)
        away_n = norm_name(away)

        # Se está finalizado (pela data do CSV): skip com log claro
        if is_past_match(row):
            print(f"[apifootball] SKIP (jogo já finalizado): '{home}' vs '{away}'"
                  f"{f' (data={date_csv})' if date_csv else ''}")
            skipped_past += 1
            continue

        # Expande aliases
        home_forms = expand_aliases(home_n, aliases)
        away_forms = expand_aliases(away_n, aliases)

        # tenta chave direta por data (dia)
        key_candidates = []
        date_key = None
        if date_csv and pd.notna(date_csv):
            try:
                dkey = pd.to_datetime(date_csv, utc=True).date().isoformat()
                date_key = dkey
            except Exception:
                date_key = None

        if date_key:
            for hf in home_forms:
                for af in away_forms:
                    key_candidates.append(f"{hf}||{af}||{date_key}")

        # fallback: procura por qualquer data (pior – pode colidir)
        if not key_candidates:
            # varre todas as chaves com mesmo home/away (ignorando data)
            for k, v in fx_index.items():
                hn, an, _ = k.split("||")
                if hn in home_forms and an in away_forms:
                    key_candidates.append(k)

        # Fallback final: fuzzy match usando rapidfuzz
        got = None
        for kc in key_candidates:
            if kc in fx_index:
                got = fx_index[kc]
                break

        if not got:
            # fuzzy: busca melhor par (home, away) no índice
            pairs = []
            for v in fx_index.values():
                score = min(
                    fuzz.ratio(home_n, v["home_n"]) / 100.0,
                    fuzz.ratio(away_n, v["away_n"]) / 100.0
                )
                pairs.append((score, v))
            pairs.sort(key=lambda x: x[0], reverse=True)
            if pairs and pairs[0][0] >= float(args.fuzzy):
                got = pairs[0][1]

        if not got:
            print(f"[apifootball] sem match p/ '{home}' vs '{away}' (norm: {home_n} x {away_n})")
            miss += 1
            continue

        # Verifica status do fixture para odds
        status = (got.get("status") or "").upper()
        if status in FINISHED_STATUSES:
            print(f"[apifootball] SKIP (finalizado na API): '{home}' vs '{away}' status={status}")
            skipped_past += 1
            continue
        if status and (status not in ELIGIBLE_STATUSES):
            # Se a API usa outros códigos, aqui apenas informamos
            print(f"[apifootball] Aviso: status não elegível para odds: '{home}' vs '{away}' status={status}")

        # Busca odds 1x2 do fixture
        fid = got["fid"]
        odds = fetch_odds_for_fixture(fid, headers, debug=args.debug)
        if not odds:
            # Sem odds — ainda assim loga como miss (de odds), não de matching
            print(f"[apifootball] sem odds p/ fixture={fid} '{home}' vs '{away}'")
            miss += 1
            continue

        rows.append({
            "home": home,
            "away": away,
            "home_n": home_n,
            "away_n": away_n,
            "book": odds["book"],
            "odd_home": odds.get("odd_home"),
            "odd_draw": odds.get("odd_draw"),
            "odd_away": odds.get("odd_away"),
            "fixture_id": fid,
            "provider": "apifootball_rapidapi"
        })
        matched += 1

    # Salva CSV
    out_path = os.path.join(out_dir, "odds_apifootball.csv")
    if rows:
        pd.DataFrame(rows).to_csv(out_path, index=False)
    else:
        # cria CSV vazio com header consistente
        pd.DataFrame(columns=[
            "home","away","home_n","away_n","book","odd_home","odd_draw","odd_away","fixture_id","provider"
        ]).to_csv(out_path, index=False)

    print(f"[apifootball] OK -> {out_path} ({len(rows)} linhas)")
    if miss:
        print(f"[apifootball] Aviso: {miss} jogo(s) sem match/odds — ver nomes/ligas/janela/aliases.")
    if skipped_past:
        print(f"[apifootball] Skips (finalizados): {skipped_past}")

if __name__ == "__main__":
    main()
