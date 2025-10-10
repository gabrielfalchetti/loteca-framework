#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coleta notícias (NewsAPI) para cada partida da whitelist e salva em:
  {OUT_DIR}/news.csv

Regras:
- API obrigatória: se NEWSAPI_KEY não estiver definido -> exit(18)
- Sempre gera uma linha por match_id (mesmo com 0 artigos)
- Saída mínima: match_id,team_home,team_away,articles_json,updated_at
"""

import os
import sys
import csv
import json
import time
import argparse
import datetime as dt
from urllib.parse import quote_plus

import requests

EXIT_CODE = 18

def eprint(*a, **k):
    print(*a, file=sys.stderr, **k)

def read_whitelist(path: str):
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        eprint(f"[news] ERRO whitelist ausente/vazia: {path}")
        sys.exit(EXIT_CODE)
    with open(path, "r", encoding="utf-8") as f:
        header = next(csv.reader(f))
    # normaliza nomes
    cols = [c.strip().lower() for c in header]
    need = {"match_id", "home", "away"}
    if not need.issubset(set(cols)):
        eprint(f"[news] ERRO whitelist sem colunas {sorted(list(need))}: {path}")
        sys.exit(EXIT_CODE)

    rows = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({
                "match_id": row.get("match_id"),
                "team_home": row.get("home") or row.get("team_home"),
                "team_away": row.get("away") or row.get("team_away"),
            })
    return rows

def newsapi_query(api_key: str, q: str, language="en", page_size=10):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": q,
        "pageSize": page_size,
        "language": language,
        "sortBy": "publishedAt",
    }
    headers = {"X-Api-Key": api_key}
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    return data.get("articles", []) or []

def build_queries(home: str, away: str):
    base = [
        f'"{home}" OR "{away}"',
        f'"{home}" AND injury',
        f'"{away}" AND injury',
        f'"{home}" AND odds',
        f'"{away}" AND odds',
    ]
    return base

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório da rodada (OUT_DIR)")
    parser.add_argument("--whitelist", required=True, help="CSV de matches_whitelist.csv")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out_dir = args.rodada
    wl_path = args.whitelist
    os.makedirs(out_dir, exist_ok=True)

    api_key = os.environ.get("NEWSAPI_KEY", "").strip()
    if not api_key:
        eprint("::error::NEWSAPI_KEY ausente no ambiente.")
        sys.exit(EXIT_CODE)

    matches = read_whitelist(wl_path)

    out_csv = os.path.join(out_dir, "news.csv")
    now_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "team_home", "team_away", "articles_json", "updated_at"])

        for m in matches:
            mid = m["match_id"]
            home = (m["team_home"] or "").strip()
            away = (m["team_away"] or "").strip()

            queries = build_queries(home, away)
            collected = []
            for q in queries:
                try:
                    arts = newsapi_query(api_key, q, language="en", page_size=5)
                    # normaliza campos essenciais
                    for a in arts:
                        collected.append({
                            "title": a.get("title"),
                            "source": (a.get("source") or {}).get("name"),
                            "publishedAt": a.get("publishedAt"),
                            "url": a.get("url"),
                        })
                    # evita flood da API
                    time.sleep(0.4)
                except Exception as ex:
                    eprint(f"[news] aviso ao consultar '{q}': {ex}")

            # remove duplicatas simples por url
            seen = set()
            uniq = []
            for a in collected:
                u = a.get("url")
                if u and u not in seen:
                    uniq.append(a)
                    seen.add(u)

            w.writerow([mid, home, away, json.dumps(uniq, ensure_ascii=False), now_iso])

    if os.path.getsize(out_csv) == 0:
        eprint("::error::news.csv ficou vazio.")
        sys.exit(EXIT_CODE)

    if args.debug:
        eprint(f"[news] OK gerado: {out_csv}")

if __name__ == "__main__":
    main()