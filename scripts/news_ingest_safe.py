#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
news_ingest_safe.py
Coleta notícias relacionadas aos jogos (se possível usando os times de data/in/matches_source.csv)
e grava em data/out/<RODADA_ID>/news.csv

Uso:
  python scripts/news_ingest_safe.py --out-dir data/out/<RODADA_ID>

Requisitos:
  - Variável de ambiente NEWSAPI_KEY (https://newsapi.org)
Comportamento:
  - Se a API falhar ou retornar vazio, ainda assim gera o CSV com cabeçalho (não quebra o workflow)
  - Tolerante a campos None e a artigos malformados
"""

import os
import csv
import sys
import json
import time
import argparse
from typing import List, Dict, Any

try:
    import requests
except Exception:
    requests = None  # GitHub runner deve ter, mas se não tiver, ainda geramos CSV vazio

def parse_args():
    p = argparse.ArgumentParser(description="Ingest de notícias com tolerância a falhas")
    p.add_argument("--out-dir", required=True, help="Diretório de saída: ex. data/out/<RODADA_ID>")
    p.add_argument("--max", type=int, default=40, help="Qtd máx. de artigos (default=40)")
    p.add_argument("--lang", default="pt", help="Idioma preferido (pt|en|es...)")
    p.add_argument("--timeout", type=int, default=12, help="Timeout por request (s)")
    return p.parse_args()

def _safe_str(x: Any, maxlen: int = None) -> str:
    s = "" if x is None else str(x)
    if maxlen is not None and len(s) > maxlen:
        return s[:maxlen]
    return s

def _load_teams_from_matches() -> List[str]:
    """
    Tenta ler data/in/matches_source.csv para extrair nomes de times.
    Cabeçalho esperado: match_id,home,away,source  (case-insensitive)
    Retorna lista de termos únicos (lower) sem vazios.
    """
    path = os.path.join("data", "in", "matches_source.csv")
    if not os.path.exists(path):
        return []
    out = []
    try:
        import pandas as pd
        df = pd.read_csv(path)
        cols = {c.lower(): c for c in df.columns}
        for need in ["home", "away"]:
            if need not in cols:
                return []
        col_home = cols["home"]
        col_away = cols["away"]
        vals = list(df[col_home].dropna().astype(str)) + list(df[col_away].dropna().astype(str))
        out = sorted({v.strip() for v in vals if v and v.strip()})
    except Exception:
        # se der ruim, segue sem equipe
        return []
    return out

def _build_queries(teams: List[str]) -> List[str]:
    """
    Monta queries para a NewsAPI. Se houver times, cria queries por time.
    Caso contrário, usa termos genéricos de futebol.
    """
    if teams:
        # limita para não estourar rate
        teams = teams[:12]
        return [f'"{t}" AND (futebol OR soccer)' for t in teams]
    # fallback
    return [
        'futebol OR soccer AND (lesão OR injury OR transfer OR técnico OR coach)',
        'Brasileirão OR Serie B OR Copa do Brasil',
        'Libertadores OR Sul-Americana',
    ]

def _fetch_news(api_key: str, q: str, lang: str, timeout: int, page_size: int = 20) -> List[Dict[str, Any]]:
    """
    Chama NewsAPI Everything endpoint. Tolera falhas e retorna lista de artigos (pode ser vazia).
    """
    if requests is None:
        return []
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": q,
        "language": lang,
        "sortBy": "publishedAt",
        "pageSize": page_size,
    }
    headers = {"X-Api-Key": api_key}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return []
        data = r.json() if r.content else {}
        articles = data.get("articles") or []
        if not isinstance(articles, list):
            return []
        return articles
    except Exception:
        return []

def _dedup_by_url(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for it in items:
        url = _safe_str((it or {}).get("url"))
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(it)
    return out

def main():
    args = parse_args()
    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    api_key = os.environ.get("NEWSAPI_KEY", "").strip()
    if not api_key:
        print("::error::NEWSAPI_KEY ausente no ambiente", file=sys.stderr)
        # Ainda assim gerar arquivo (cabeçalho) para não quebrar o job imediatamente
        out_csv = os.path.join(out_dir, "news.csv")
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["source","author","title","description","url","publishedAt"])
        sys.exit(2)

    teams = _load_teams_from_matches()
    queries = _build_queries(teams)

    collected: List[Dict[str, Any]] = []
    for q in queries:
        arts = _fetch_news(api_key, q, args.lang, args.timeout, page_size=min(20, max(5, args.max)))
        # Normaliza cada artigo para dicionário seguro
        for a in arts:
            a = a or {}
            src = a.get("source") or {}
            row = {
                "source": _safe_str(src.get("name") or src.get("id"), 120),
                "author": _safe_str(a.get("author"), 120),
                "title": _safe_str(a.get("title"), 300),
                "description": _safe_str(a.get("description"), 500),
                "url": _safe_str(a.get("url"), 500),
                "publishedAt": _safe_str(a.get("publishedAt"), 40),
            }
            # filtra lixo/sem título/sem url
            if row["title"] and row["url"]:
                collected.append(row)
        # rate-limit básico
        time.sleep(0.5)

    # deduplica por URL e corta no máximo
    collected = _dedup_by_url(collected)
    if args.max and args.max > 0:
        collected = collected[:args.max]

    out_csv = os.path.join(out_dir, "news.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source","author","title","description","url","publishedAt"])
        for r in collected:
            w.writerow([r["source"], r["author"], r["title"], r["description"], r["url"], r["publishedAt"]])

    print(f"[news] OK -> {out_csv} | artigos: {len(collected)}")
    if len(collected) == 0:
        print("[news] aviso: nenhuma notícia coletada (API vazia ou sem correspondência). CSV gerado com cabeçalho.")

if __name__ == "__main__":
    main()