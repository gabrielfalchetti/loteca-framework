#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestão de odds via API-Football (oficial ou RapidAPI), com pareamento
automático de nomes usando aliases e normalização leve.

Saída: {OUT_DIR}/odds_apifootball.csv

Requer:
- Env API_FOOTBALL_KEY (oficial) OU X_RAPIDAPI_KEY (RapidAPI).
- SOURCE_CSV_NORM com colunas minimamente: team_home,team_away,date
  (date em ISO UTC ou algo parseável por python-dateutil).
- (Opcional) AUTO_ALIASES_JSON com mapeamentos de nome->canônico.

Uso:
  python -m scripts.ingest_odds_apifootball \
    --rodada data/out/12345 \
    --source_csv data/out/12345/matches_norm.csv \
    [--hours 72]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import requests

# --- Utilidades de nomes -----------------------------------------------------


def _token_key(name: str) -> str:
    """Normaliza nome para comparação frouxa (minúsculas, alfanumérico e ordenação)."""
    if not name:
        return ""
    import re
    toks = re.findall(r"[a-z0-9]+", name.lower())
    # remove sufixos comuns
    stop = {"fc", "cf", "ac", "afc", "sc", "bc", "u23", "women", "ladies"}
    toks = [t for t in toks if t not in stop]
    return " ".join(toks)


def _best_match(
    target: str,
    candidates: List[str],
    score_cutoff: int = 80,
) -> Tuple[Optional[str], int]:
    """
    Fuzzy-match leve usando SequenceMatcher.
    Retorna (melhor_candidato, score 0..100). score_cutoff mínima para aceitar.
    """
    from difflib import SequenceMatcher

    tkey = _token_key(target)
    best = (None, 0)
    for c in candidates:
        ratio = int(round(100 * SequenceMatcher(None, tkey, _token_key(c)).ratio()))
        if ratio > best[1]:
            best = (c, ratio)
    if best[1] >= score_cutoff:
        return best
    return (None, best[1])


def _load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _dump_csv(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _parse_dt(value: str) -> Optional[datetime]:
    # tenta vários formatos comuns
    from dateutil import parser

    try:
        dt = parser.parse(value)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# --- Cliente API-Football ----------------------------------------------------


@dataclass
class APIFootballClient:
    use_rapidapi: bool
    key: str

    @classmethod
    def from_env(cls) -> Optional["APIFootballClient"]:
        key_official = os.getenv("API_FOOTBALL_KEY", "").strip()
        key_rapid = os.getenv("X_RAPIDAPI_KEY", "").strip()
        if key_official:
            return cls(use_rapidapi=False, key=key_official)
        if key_rapid:
            return cls(use_rapidapi=True, key=key_rapid)
        return None

    @property
    def base_url(self) -> str:
        if self.use_rapidapi:
            return "https://api-football-v1.p.rapidapi.com/v3"
        return "https://v3.football.api-sports.io"

    @property
    def headers(self) -> Dict[str, str]:
        if self.use_rapidapi:
            return {
                "X-RapidAPI-Key": self.key,
                "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
            }
        return {"x-apisports-key": self.key}

    def get(self, path: str, params: dict) -> dict:
        url = f"{self.base_url}{path}"
        r = requests.get(url, headers=self.headers, params=params, timeout=20)
        if r.status_code == 429:
            # rate-limit: espera um pouco e tenta 1x
            time.sleep(2.0)
            r = requests.get(url, headers=self.headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        # Estrutura comum: { "response": [...], "results": N }
        return data or {}


# --- Leitura do CSV de partidas ---------------------------------------------


@dataclass
class MatchRow:
    home: str
    away: str
    when_utc: Optional[datetime]


def _read_source_matches(path: str) -> List[MatchRow]:
    rows: List[MatchRow] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # tenta deduzir nomes de colunas aceitas
        home_keys = ["home", "team_home", "home_team", "mandante"]
        away_keys = ["away", "team_away", "away_team", "visitante"]
        date_keys = ["date", "datetime", "kickoff", "utc", "match_date"]

        for r in reader:
            home = next((r.get(k) for k in home_keys if k in r and r.get(k)), "")
            away = next((r.get(k) for k in away_keys if k in r and r.get(k)), "")
            dval = next((r.get(k) for k in date_keys if k in r and r.get(k)), "")
            rows.append(MatchRow(home=home, away=away, when_utc=_parse_dt(dval)))
    return rows


# --- Aliases -----------------------------------------------------------------


def _load_aliases() -> Dict[str, str]:
    """Lê JSON de aliases do env AUTO_ALIASES_JSON quando existir."""
    path = os.getenv("AUTO_ALIASES_JSON", "").strip()
    if not path:
        return {}
    data = _load_json(path)
    # formata em dict simples: "alias_normalizado" -> "canonico"
    out = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, str):
                out[_token_key(k)] = v
            elif isinstance(v, dict) and "canonical" in v:
                out[_token_key(k)] = str(v["canonical"])
            elif isinstance(v, list) and v:
                # se vier lista de aliases cujo primeiro é canônico
                out[_token_key(k)] = str(v[0])
    return out


def _apply_alias(name: str, aliases: Dict[str, str]) -> str:
    canon = aliases.get(_token_key(name))
    return canon or name


# --- Pipeline principal ------------------------------------------------------


def _fixtures_in_window(client: APIFootballClient, hours: int) -> List[dict]:
    """
    Busca fixtures na janela [agora, agora+hours] em UTC.
    Observação: sem filtro de liga, isso pode retornar muitos jogos dependendo do plano/limite.
    """
    now = datetime.now(timezone.utc)
    to = now + timedelta(hours=hours)
    params = {"from": now.strftime("%Y-%m-%d"), "to": to.strftime("%Y-%m-%d")}
    data = client.get("/fixtures", params=params)
    resp = data.get("response", []) or []
    # Filtra de fato por janela de horas, pois o endpoint por data retorna por dia
    out = []
    for fx in resp:
        # estrutura típica: fx["fixture"]["id"], fx["teams"]["home"]["name"], ...
        try:
            ts = fx["fixture"]["date"]
            dt = _parse_dt(ts)
            if not dt:
                continue
            if now <= dt <= to:
                out.append(fx)
        except Exception:
            continue
    return out


def _match_fixture_to_source(
    fix: dict,
    src_matches: List[MatchRow],
    aliases: Dict[str, str],
    score_cutoff: int = 86,
) -> Optional[Tuple[MatchRow, int, int]]:
    """Tenta parear fixture com uma linha do SOURCE_CSV_NORM por nome (com aliases) e horário próximo."""
    try:
        fh = fix["teams"]["home"]["name"]
        fa = fix["teams"]["away"]["name"]
        fdt = _parse_dt(fix["fixture"]["date"])
    except Exception:
        return None

    fh_canon = _apply_alias(fh, aliases)
    fa_canon = _apply_alias(fa, aliases)

    # candidatos por horário ±12h para reduzir falsos positivos
    cand: List[Tuple[MatchRow, int, int]] = []
    for m in src_matches:
        if not m.home or not m.away:
            continue
        mh = _apply_alias(m.home, aliases)
        ma = _apply_alias(m.away, aliases)

        # match por nomes (cada lado independentemente)
        h_cand, h_sc = _best_match(fh_canon, [mh], score_cutoff=score_cutoff)
        a_cand, a_sc = _best_match(fa_canon, [ma], score_cutoff=score_cutoff)
        if not h_cand or not a_cand:
            # tenta invertido (às vezes fonte troca mandante)
            h2, s2 = _best_match(fh_canon, [ma], score_cutoff=score_cutoff)
            a2, s3 = _best_match(fa_canon, [mh], score_cutoff=score_cutoff)
            if h2 and a2:
                h_sc, a_sc = s2, s3
            else:
                continue

        # checa horário (se existir)
        ok_time = True
        if m.when_utc and fdt:
            delta = abs((m.when_utc - fdt).total_seconds()) / 3600.0
            ok_time = delta <= 12.0  # tolerância

        if ok_time:
            cand.append((m, h_sc, a_sc))

    if not cand:
        return None
    # escolhe maior soma de scores
    cand.sort(key=lambda x: (x[1] + x[2]), reverse=True)
    return cand[0]


def _odds_for_fixture(client: APIFootballClient, fixture_id: int) -> Optional[dict]:
    data = client.get("/odds", params={"fixture": fixture_id})
    resp = data.get("response", [])
    # A estrutura pode trazer múltiplos bookmakers/markets; vamos caçar 1x2 (Match Winner)
    # markets tipicamente têm key "1x2" ou "Winner", etc.
    for item in resp:
        try:
            bookmakers = item.get("bookmakers", []) or []
            for bk in bookmakers:
                bk_name = bk.get("name", "bookmaker")
                for market in bk.get("bets", []) or bk.get("markets", []) or []:
                    # policy: aceitar mercado 1x2 com três valores
                    mname = (market.get("name") or market.get("key") or "").lower()
                    if mname in ("match winner", "1x2", "winner", "full time result"):
                        values = market.get("values") or market.get("outcomes") or []
                        # precisamos H/D/A
                        price_map = {}
                        for v in values:
                            label = (v.get("value") or v.get("name") or v.get("label") or "").strip().upper()
                            odd = v.get("odd") or v.get("price") or v.get("decimal") or v.get("value")
                            try:
                                odd = float(odd)
                            except Exception:
                                continue
                            # normaliza label
                            if label in ("HOME", "1", "H"):
                                price_map["home"] = odd
                            elif label in ("DRAW", "X", "D"):
                                price_map["draw"] = odd
                            elif label in ("AWAY", "2", "A"):
                                price_map["away"] = odd
                        if {"home", "draw", "away"}.issubset(price_map):
                            return {
                                "bookie": bk_name,
                                "odds_home": price_map["home"],
                                "odds_draw": price_map["draw"],
                                "odds_away": price_map["away"],
                            }
        except Exception:
            continue
    return None


def main():
    ap = argparse.ArgumentParser(description="Ingest odds from API-Football into CSV.")
    ap.add_argument("--rodada", required=True, help="Diretório de saída da rodada (OUT_DIR)")
    ap.add_argument("--source_csv", required=True, help="CSV normalizado de partidas (matches_norm.csv)")
    ap.add_argument("--hours", type=int, default=72, help="Janela futura em horas para buscar fixtures (default: 72)")
    args = ap.parse_args()

    out_dir = args.rodada
    src_csv = args.source_csv
    hours = max(1, int(args.hours))

    client = APIFootballClient.from_env()
    if not client:
        # Sem credenciais — gera vazio, sem erro
        out_path = os.path.join(out_dir, "odds_apifootball.csv")
        _dump_csv(out_path, [], ["team_home", "team_away", "odds_home", "odds_draw", "odds_away", "bookie", "source", "fixture_id"])
        print("[apifootball]Sem credenciais; arquivo vazio gerado.")
        return

    # Carrega fonte e aliases
    try:
        src_matches = _read_source_matches(src_csv)
    except Exception as e:
        print(f"::error::falha ao ler {src_csv}: {e}")
        sys.exit(5)

    aliases = _load_aliases()

    # Busca fixtures
    try:
        fixtures = _fixtures_in_window(client, hours=hours)
    except Exception as e:
        print(f"::notice::falha ao consultar fixtures: {e}")
        fixtures = []

    rows_out: List[dict] = []
    seen_pairs = set()

    for fx in fixtures:
        try:
            fixture_id = fx["fixture"]["id"]
        except Exception:
            continue

        match_found = _match_fixture_to_source(fx, src_matches, aliases, score_cutoff=86)
        if not match_found:
            continue
        m, sc_h, sc_a = match_found

        # evita duplicados (mesmo par)
        key = (_token_key(m.home), _token_key(m.away))
        if key in seen_pairs:
            continue
        seen_pairs.add(key)

        # pega odds
        try:
            odds = _odds_for_fixture(client, fixture_id)
        except Exception:
            odds = None

        if not odds:
            continue

        rows_out.append(
            {
                "team_home": aliases.get(_token_key(m.home), m.home),
                "team_away": aliases.get(_token_key(m.away), m.away),
                "odds_home": odds["odds_home"],
                "odds_draw": odds["odds_draw"],
                "odds_away": odds["odds_away"],
                "bookie": odds["bookie"],
                "source": "apifootball",
                "fixture_id": fixture_id,
            }
        )

    out_path = os.path.join(out_dir, "odds_apifootball.csv")
    _dump_csv(
        out_path,
        rows_out,
        ["team_home", "team_away", "odds_home", "odds_draw", "odds_away", "bookie", "source", "fixture_id"],
    )

    print(f"[apifootball]Arquivo odds_apifootball.csv gerado com {len(rows_out)} jogos encontrados.")


if __name__ == "__main__":
    main()