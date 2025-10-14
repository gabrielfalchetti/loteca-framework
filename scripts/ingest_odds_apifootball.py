#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball.py

Lê uma lista de jogos (match_id, home, away), casa os times na API-Football
com normalização BR robusta + fuzzy (difflib), tenta localizar o fixture via
head-to-head (próximos jogos) e, se disponível no plano, coleta odds do fixture.

Saída: <rodada>/odds_apifootball.csv com colunas:
match_id,team_home,team_away,fixture_id,odds_home,odds_draw,odds_away

- Sempre grava o arquivo com cabeçalho (mesmo se 0 linhas).
- Se não houver odds no seu plano, grava as linhas com fixture_id e odds vazias.
- Usa apenas bibliotecas da stdlib (requests é esperado no runner GitHub).
"""

from __future__ import annotations
import os
import sys
import csv
import json
import time
import argparse
import unicodedata
import re
from typing import Optional, Dict, Any, List, Tuple
from difflib import SequenceMatcher

try:
    import requests
except Exception as e:
    print("[apifootball][CRITICAL] 'requests' não disponível. Instale requests para rodar este script.")
    sys.exit(2)

API_BASE = "https://v3.football.api-sports.io"


# ---------------------- Logging helpers ----------------------
def log(msg: str) -> None:
    print(f"[apifootball]{msg}")

def info(msg: str) -> None:
    print(f"[apifootball]{msg}")

def warn(msg: str) -> None:
    print(f"[apifootball][WARN] {msg}")

def critical(msg: str) -> None:
    print(f"[apifootball][CRITICAL] {msg}")


# ---------------------- String normalization ----------------------
UF_TOKENS = {"rj", "sp", "pr", "pa", "go", "sc", "rs", "mg", "ba", "pe", "ce", "es", "mt", "ms", "pb", "rn", "al", "pi", "ma", "ro", "rr", "ap", "am", "ac", "se", "df"}


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return s.encode("ascii", "ignore").decode("ascii")


def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


# Mapeamentos específicos BR (chaves e valores já "normalizados").
# A ideia é levar variantes comuns para a forma mais próxima do catálogo da API.
MAP_FIX: Dict[str, str] = {
    # Athletico Paranaense
    "athletico pr": "athletico paranaense",
    "atletico pr": "athletico paranaense",
    # Atletico Goianiense
    "atletico go": "atletico goianiense",
    "atletico goiania": "atletico goianiense",
    # Botafogo
    "botafogo rj": "botafogo",
    # Botafogo SP
    "botafogo sp": "botafogo sp",
    "botafogo sao paulo": "botafogo sp",
    "botafogo-sp": "botafogo sp",
    # Ferroviária
    "ferroviaria": "ferroviaria",
    # Avai
    "avai": "avai",
    # Chapecoense
    "chapecoense": "chapecoense",
    # Paysandu / Remo (PA)
    "paysandu": "paysandu pa",
    "remo": "remo pa",
    # Ponte Preta (algumas bases usam "aa ponte preta")
    "aa ponte preta": "ponte preta",
    "ponte preta sp": "ponte preta",
    # Coritiba / Curitiba confusões
    "coritiba pr": "coritiba",
    # America Mineiro
    "america mg": "america mg",
    "america mineiro": "america mg",
    # Ceara / Ceará
    "ceara": "ceara",
    # Vitoria
    "vitoria ba": "vitoria",
    # Goias / Goiás
    "goias": "goias",
}


def clean_tokens(s: str) -> List[str]:
    # remove tokens vazios e UF isoladas
    toks = [t for t in re.split(r"[^\w]+", s) if t]
    toks = [t for t in toks if t not in UF_TOKENS]
    return toks


def norm_br(s: str) -> str:
    s0 = s or ""
    s1 = strip_accents(s0.lower())
    s1 = s1.replace(" - ", " ").replace("-", " ")
    s1 = norm_spaces(s1)

    # remove sufixos tipo "/SP", "(SP)", "-SP"
    s1 = re.sub(r"[\(\[\{]\s*(rj|sp|pr|pa|go|sc|rs|mg)\s*[\)\]\}]", "", s1)
    s1 = re.sub(r"\b(rj|sp|pr|pa|go|sc|rs|mg)\b", "", s1)
    s1 = norm_spaces(s1)

    # normaliza alguns padrões com hífen nas fontes
    s1 = s1.replace(" atletico ", " atletico ")

    # aplica mapa fixo se existir
    if s1 in MAP_FIX:
        return MAP_FIX[s1]

    # heurística: juntar tokens e remover duplicados simples
    toks = clean_tokens(s1)
    out = norm_spaces(" ".join(toks))
    return MAP_FIX.get(out, out)


def ratio(a: str, b: str) -> float:
    # Similaridade simples via SequenceMatcher (0..1)
    return SequenceMatcher(None, a, b).ratio()


# ---------------------- API helpers ----------------------
class ApiSports:
    def __init__(self, api_key: str, timeout: int = 30):
        self.api_key = api_key
        self.sess = requests.Session()
        self.sess.headers.update({"x-apisports-key": self.api_key})
        self.timeout = timeout

    def get(self, path: str, params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        url = f"{API_BASE}{path}"
        try:
            r = self.sess.get(url, params=params, timeout=self.timeout)
            code = r.status_code
            js = r.json() if r.content else {}
            if code != 200:
                warn(f"HTTP {code} em {path} params={params} resp={js}")
            return js, code
        except Exception as e:
            warn(f"Falha request {path} params={params}: {e}")
            return None, None

    def search_team(self, name: str, country_hint: Optional[str] = "Brazil") -> Optional[int]:
        q = norm_br(name)
        params = {"search": q}
        if country_hint:
            params["country"] = country_hint
        js, code = self.get("/teams", params)
        resp = (js or {}).get("response", []) if js else []
        if not resp:
            # tenta sem country
            js, code = self.get("/teams", {"search": q})
            resp = (js or {}).get("response", []) if js else []
        if not resp:
            return None

        # Ranking por similaridade do nome normalizado
        best_id, best_sc = None, 0.0
        for it in resp:
            nm = it.get("team", {}).get("name", "")
            nm_n = norm_br(nm)
            sc = ratio(q, nm_n)
            if sc > best_sc:
                best_sc = sc
                best_id = it.get("team", {}).get("id")
        if best_sc >= 0.75:
            return best_id
        return None

    def find_upcoming_fixture_h2h(self, home_id: int, away_id: int) -> Optional[Dict[str, Any]]:
        # tenta h2h (ordem home-away)
        for pair in (f"{home_id}-{away_id}", f"{away_id}-{home_id}"):
            js, code = self.get("/fixtures/headtohead", {"h2h": pair, "next": 10})
            resp = (js or {}).get("response", []) if js else []
            if resp:
                # preferir o que estiver marcado como upcoming (status 'NS', 'TBD', etc.)
                # senão devolve o primeiro
                for fx in resp:
                    st = fx.get("fixture", {}).get("status", {}).get("short", "")
                    if st in {"NS", "TBD", "PST", "SUSP"}:
                        return fx
                return resp[0]
        return None

    def odds_by_fixture(self, fixture_id: int) -> Optional[Tuple[float, float, float]]:
        # Endpoint depende do plano. Tentamos, mas aceitamos falhar.
        js, code = self.get("/odds", {"fixture": fixture_id})
        if not js or code != 200:
            return None
        resp = js.get("response", [])
        if not resp:
            return None

        # Procurar mercado 1X2 (home/draw/away)
        # Estrutura típica: response -> [ { "bookmakers":[{ "bets":[{ "name":"Match Winner","values":[{"value":"Home","odd":"2.10"}, ...]} ] } ] } ]
        for itm in resp:
            for bm in itm.get("bookmakers", []):
                for bet in bm.get("bets", []):
                    name = (bet.get("name") or "").lower()
                    if "match winner" in name or "1x2" in name or "to win" in name:
                        oh = od = oa = None
                        for v in bet.get("values", []):
                            val = (v.get("value") or "").strip().lower()
                            odd_str = v.get("odd")
                            try:
                                odd = float(str(odd_str).replace(",", "."))
                            except Exception:
                                odd = None
                            if odd is None:
                                continue
                            if val in {"home", "1"}:
                                oh = odd if oh is None else min(oh, odd)
                            elif val in {"draw", "x"}:
                                od = odd if od is None else min(od, odd)
                            elif val in {"away", "2"}:
                                oa = odd if oa is None else min(oa, odd)
                        if oh or od or oa:
                            return oh, od, oa
        return None


# ---------------------- Core ----------------------
def read_matches(source_csv: str) -> List[Dict[str, str]]:
    if not os.path.exists(source_csv) or os.path.getsize(source_csv) == 0:
        critical(f"Arquivo de origem inexistente ou vazio: {source_csv}")
        return []

    with open(source_csv, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        cols = [c.strip().lower() for c in rd.fieldnames or []]
        # aceita home/away ou team_home/team_away
        def pick(row, *keys):
            for k in keys:
                if k in row and row[k]:
                    return row[k]
            return ""
        rows = []
        for r in rd:
            rnorm = {k.strip().lower(): (v or "").strip() for k, v in r.items()}
            match_id = rnorm.get("match_id") or rnorm.get("id") or ""
            home = pick(rnorm, "home", "team_home")
            away = pick(rnorm, "away", "team_away")
            if not home or not away:
                warn(f"Linha ignorada: faltando home/away (match_id={match_id})")
                continue
            rows.append({"match_id": match_id, "home": home, "away": away})
    return rows


def write_output(out_path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fields = ["match_id", "team_home", "team_away", "fixture_id", "odds_home", "odds_draw", "odds_away"]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader()
        for r in rows:
            wr.writerow({
                "match_id": r.get("match_id", ""),
                "team_home": r.get("team_home", ""),
                "team_away": r.get("team_away", ""),
                "fixture_id": r.get("fixture_id", ""),
                "odds_home": r.get("odds_home", ""),
                "odds_draw": r.get("odds_draw", ""),
                "odds_away": r.get("odds_away", ""),
            })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório da rodada para salvar odds_apifootball.csv")
    parser.add_argument("--source_csv", required=True, help="CSV com match_id,home,away (ou team_home/team_away)")
    args = parser.parse_args()

    api_key = os.environ.get("API_FOOTBALL_KEY", "").strip()
    if not api_key:
        info("API_FOOTBALL_KEY ausente; nada a fazer (saindo com código 0).")
        # Saída silenciosa (workflow já trata o caso)
        sys.exit(0)

    jogos = read_matches(args.source_csv)
    if not jogos:
        warn("Nenhum jogo válido no arquivo de origem.")
        out = os.path.join(args.rodada, "odds_apifootball.csv")
        write_output(out, [])
        info(f"Arquivo odds_apifootball.csv gerado com 0 jogos encontrados.")
        sys.exit(0)

    info(f"Iniciando busca direcionada para {len(jogos)} jogos do arquivo de origem.")

    api = ApiSports(api_key)
    out_rows: List[Dict[str, Any]] = []

    for j in jogos:
        h_raw, a_raw = j["home"], j["away"]
        h_n, a_n = norm_br(h_raw), norm_br(a_raw)

        # Busca team_id com hint Brazil (depois sem hint)
        hid = api.search_team(h_raw, country_hint="Brazil") or api.search_team(h_raw, country_hint=None)
        aid = api.search_team(a_raw, country_hint="Brazil") or api.search_team(a_raw, country_hint=None)

        if not hid or not aid:
            warn(f"Sem team_id para: {h_raw} vs {a_raw}")
            # ainda assim gravamos a linha (sem fixture/odds) para debug de matching
            out_rows.append({
                "match_id": j["match_id"],
                "team_home": h_raw,
                "team_away": a_raw,
                "fixture_id": "",
                "odds_home": "",
                "odds_draw": "",
                "odds_away": "",
            })
            continue

        fx = api.find_upcoming_fixture_h2h(hid, aid)
        if not fx:
            warn(f"Sem fixture_id para: {h_raw} vs {a_raw}")
            out_rows.append({
                "match_id": j["match_id"],
                "team_home": h_raw,
                "team_away": a_raw,
                "fixture_id": "",
                "odds_home": "",
                "odds_draw": "",
                "odds_away": "",
            })
            continue

        fixture_id = fx.get("fixture", {}).get("id", "")
        oh = od = oa = ""

        # Tenta odds (se o plano/endpoint permitir)
        odds = api.odds_by_fixture(fixture_id)
        if odds:
            oh_v, od_v, oa_v = odds
            oh = f"{oh_v:.4f}" if oh_v else ""
            od = f"{od_v:.4f}" if od_v else ""
            oa = f"{oa_v:.4f}" if oa_v else ""
        else:
            warn(f"Sem odds para fixture {fixture_id} (plano pode não incluir endpoint de odds).")

        out_rows.append({
            "match_id": j["match_id"],
            "team_home": h_raw,
            "team_away": a_raw,
            "fixture_id": fixture_id,
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
        })

    out_file = os.path.join(args.rodada, "odds_apifootball.csv")
    write_output(out_file, out_rows)
    info(f"Arquivo odds_apifootball.csv gerado com {sum(1 for r in out_rows if r.get('fixture_id'))} jogos encontrados.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        critical("Interrompido pelo usuário.")
        sys.exit(130)
    except Exception as e:
        critical(f"Exceção não tratada: {e}")
        sys.exit(1)